//! A fixed size buffer

use std::ops::{Add, AddAssign, Index, Range, Sub, SubAssign};

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy, Default)]
pub struct Pointer(usize);

impl Pointer {
    /// The pointer returns the _absolute_ position in the byte stream that was
    /// consumed using the shift buffer.
    pub fn abs(&self) -> usize {
        self.0
    }
}

impl Add<usize> for Pointer {
    type Output = Pointer;

    fn add(self, rhs: usize) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl AddAssign<usize> for Pointer {
    fn add_assign(&mut self, rhs: usize) {
        self.0 += rhs
    }
}

impl Sub<usize> for Pointer {
    type Output = Pointer;

    fn sub(self, rhs: usize) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl SubAssign<usize> for Pointer {
    fn sub_assign(&mut self, rhs: usize) {
        self.0 -= rhs
    }
}

impl Sub<Pointer> for Pointer {
    type Output = usize;

    fn sub(self, rhs: Pointer) -> Self::Output {
        self.0 - rhs.0
    }
}

pub struct ShiftBuffer<T> {
    buf: Vec<T>,
    // The absolute position of the lower end of the window in the overall byte
    // stream. To put it differently, this is the total sum of all advances.
    offset: Pointer,
    lower: Pointer,
    upper: Pointer,
}

impl<T: Default + Copy> ShiftBuffer<T> {
    pub fn new(init_size: usize) -> Self {
        let buf = (0..init_size).map(|_| T::default()).collect();
        Self {
            buf,
            offset: Pointer::default(),
            lower: Pointer::default(),
            upper: Pointer::default(),
        }
    }

    /// Moves the lower end of the window by `n`.
    pub fn shrink(&mut self, n: usize) -> Pointer {
        debug_assert!(self.lower + n < self.upper);
        self.lower += n;
        self.lower
    }

    pub fn extend(&mut self, n: usize) -> Pointer {
        debug_assert!(self.relative_pos(self.upper) + n <= self.buf.len());
        self.upper += n;
        self.upper
    }

    /// Make room in the buffer for more data.
    ///
    /// If the upper end of the window is not at the stop position of the
    /// internal buffer, this method has no effect on the state of the buffer.
    ///
    /// Otherwise, it performs either of two operations: if the lower end is at
    /// the beginning of the buffer (the window covers the entire buffer), the
    /// buffer is extended. Otherwise, the buffer is shifted; i.e., all entries
    /// prior to the lower end are discarded and the content is moved to the
    /// beginning of the buffer.
    ///
    /// In all cases, the return value of this method is the same as for
    /// [ShiftBuffer::free].
    pub fn make_room(&mut self) -> &mut [T] {
        if self.relative_pos(self.upper) == self.buf.len() {
            if self.lower == self.offset {
                self.buf.extend((0..self.buf.len()).map(|_| T::default()))
            } else {
                self.shift();
            }
        }
        self.free()
    }

    pub fn shift(&mut self) {
        let d = self.upper.abs() - self.lower.abs();
        for p in 0..d {
            self.buf[p] = self.buf[p + d]
        }
        self.offset = self.lower;
    }

    pub fn free(&mut self) -> &mut [T] {
        let r = self.relative_pos(self.upper);
        &mut self.buf[r..]
    }

    pub fn lower(&self) -> Pointer {
        self.lower
    }

    pub fn upper(&self) -> Pointer {
        self.upper
    }

    fn relative_pos(&self, p: Pointer) -> usize {
        p.abs() - self.offset.abs()
    }
}

impl<T: Default + Copy> Index<Pointer> for ShiftBuffer<T> {
    type Output = T;

    fn index(&self, index: Pointer) -> &Self::Output {
        debug_assert!(self.lower <= index && index <= self.upper);
        &self.buf[self.relative_pos(index)]
    }
}

impl<T: Default + Copy> Index<Range<Pointer>> for ShiftBuffer<T> {
    type Output = [T];

    fn index(&self, r: Range<Pointer>) -> &Self::Output {
        debug_assert!(r.start <= r.end);
        debug_assert!(self.lower <= r.start && r.start <= self.upper);
        debug_assert!(self.lower <= r.end && r.end <= self.upper);
        &self.buf[self.relative_pos(r.start)..self.relative_pos(r.end)]
    }
}
