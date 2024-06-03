//! Parse a journal entries in the Journal Export Format.
//!
//! [self::parser::JournalExportParser] contains the parser logic and manages
//! the buffer. The [JournalExportAsyncRead] and [sync::JournalExportRead]
//! provide async and sync versions of a parser.
//!
//! ## Parser and buffer management
//!
//! The parser is optimized for situations where the journal entry is
//! immediately 'reduced' for further processing; for example, for most
//! applications, most of the fields are ignored or their representation changes
//! based on the type (for a simple example, integers are parsed). It'd thus be
//! wasteful to create heap-allocated objects for each entry or even field
//! upfront containing the raw representation of the field.
//!
//! The journal entries are read into a buffer whose size is only increased (by
//! the initial buffer size) if a single entry is larger than the current buffer
//! size. Currently, there is no mechanism to decrease the buffer size
//! again; such an extensions might be of interest in networking applications
//! that consume data from potentially untrustworthy sources.
//!
//! ## Implementation notes
//!
//! Both, [sync::JournalExportRead] and [JournalExportAsyncRead] do not
//! implement [Iterator] or [futures::Stream] respectively. The reason is that
//! the lifetime of the object returned from `next(&mut self)` (which is a
//! reference in our case) must be valid for at least the lifetime of the
//! iterator itself — not the mutable reference `&mut self`.
//!
//! One can make use of the iterator pattern by turning the `Entry<'_>` into an
//! OwnedEntry.

use thiserror::Error;

use self::parser::{BufferState, JournalExportParser};
pub use self::{parser::EntryRef, sync::JournalExportRead};
use futures::{AsyncRead, AsyncReadExt};

// We assume that 16KiB (half L1 cache on modern CPUs) is enough to hold at
// least one Journal Entry.
const DEFAULT_BUF_SIZE: usize = 4096 * 4;

pub mod parser {
    use crate::shiftbuffer::{Pointer, ShiftBuffer};

    use super::JournalExportReadError;

    pub struct JournalExportParser {
        buf: ShiftBuffer<u8>,
        entry_start: Pointer,
        field_start: Pointer,
        cursor: Pointer,
        namelen: usize,
        remaining: u64,
        parse_state: ParserState,
        field_offsets: Vec<FieldOffset>,
    }

    impl JournalExportParser {
        pub fn new(buf_size: usize) -> Self {
            let buf = ShiftBuffer::new(buf_size);
            let entry_start = buf.lower();
            let field_start = entry_start;
            let cursor = entry_start;
            Self {
                buf,
                entry_start,
                field_start,
                cursor,
                namelen: 0,
                remaining: 0,
                parse_state: ParserState::FieldStart,
                field_offsets: vec![],
            }
        }

        pub fn extend(&mut self, n: usize) {
            self.buf.extend(n);
        }

        #[inline]
        pub fn parse(&mut self) -> BufferState<()> {
            loop {
                // If the cursor reached the upper end of the window, ask for
                // more byte from the user.
                if self.cursor == self.buf.upper() {
                    match self.parse_state {
                        ParserState::EntryStart => {
                            return BufferState::UnderfilledEntryStart(self.buf.make_room())
                        }
                        _ => return BufferState::Underfilled(self.buf.make_room()),
                    }
                }

                let mut c = self.buf[self.cursor];

                use ParserState::*;
                self.parse_state = match self.parse_state {
                    EntryStart => {
                        if c.is_ascii_alphanumeric() || c == b'_' {
                            self.entry_start = self.cursor;
                            self.field_start = self.entry_start;
                            self.cursor += 1;
                            ParserState::Fieldname
                        } else {
                            return self
                                .close_on_err(Err(JournalExportReadError::UnexpectedCharacter(c)));
                        }
                    }
                    FieldStart => match c {
                        b'\n' => {
                            if !self.field_offsets.is_empty() {
                                self.cursor += 1;
                                self.parse_state = ParserState::EntryStart;
                                return BufferState::Result(Ok(()));
                            } else {
                                return self.close_on_err(Err(
                                    JournalExportReadError::UnexpectedCharacter(c),
                                ));
                            }
                        }
                        c if (c.is_ascii_alphanumeric() || c == b'_') => {
                            self.field_start = self.cursor;
                            self.cursor += 1;
                            ParserState::Fieldname
                        }
                        c => {
                            return self
                                .close_on_err(JournalExportReadError::invalid_fieldname_char(c));
                        }
                    },
                    Fieldname => {
                        self.namelen = self.cursor - self.field_start;
                        self.cursor += 1;
                        match c {
                            b'=' => ParserState::StringField,
                            b'\n' => ParserState::BinaryValueLen,
                            c_ if c_.is_ascii_alphanumeric() || c_ == b'_' => {
                                ParserState::Fieldname
                            }
                            _ => {
                                return self.close_on_err(Err(
                                    JournalExportReadError::UnexpectedCharacter(c),
                                ))
                            }
                        }
                    }
                    BinaryValueLen => {
                        // [fieldname]  '\n'  [64bit le integer]
                        // <-namelen-> +  1 + <----8 bytes----->
                        let len_stop = self.field_start + self.namelen + 9;
                        self.cursor = self.buf.upper().min(len_stop);
                        if self.cursor < len_stop || self.cursor == self.buf.upper() {
                            ParserState::BinaryValueLen
                        } else {
                            let mut le_bytes = [0u8; 8];
                            let len_start = len_stop - 8;
                            le_bytes.copy_from_slice(&self.buf[len_start..len_stop]);
                            self.remaining = u64::from_le_bytes(le_bytes);
                            ParserState::BinaryValue
                        }
                    }
                    BinaryValue => {
                        let stop_pos =
                            self.field_start + self.namelen + 9 + self.remaining as usize;
                        self.cursor = self.buf.upper().min(stop_pos);
                        if self.cursor < stop_pos || self.cursor == self.buf.upper() {
                            ParserState::BinaryValue
                        } else {
                            c = self.buf[self.cursor];
                            if c != b'\n' {
                                return self.close_on_err(Err(
                                    JournalExportReadError::UnexpectedCharacter(c),
                                ));
                            }
                            self.cursor += 1;
                            self.field_offsets.push(FieldOffset {
                                start: self.field_start,
                                namelen: self.namelen,
                                typ: FieldType::Binary,
                            });
                            ParserState::FieldStart
                        }
                    }
                    StringField => {
                        self.cursor += 1;
                        if c == b'\n' {
                            self.field_offsets.push(FieldOffset {
                                start: self.field_start,
                                namelen: self.namelen,
                                typ: FieldType::String,
                            });
                            ParserState::FieldStart
                        } else {
                            ParserState::StringField
                        }
                    }
                    Eof => return BufferState::Result(Err(JournalExportReadError::Eof)),
                }
            }
        }

        #[inline]
        pub fn get_entry(&self) -> EntryRef<'_> {
            EntryRef {
                reader: self,
            }
        }

        #[inline]
        pub fn clear_entry(&mut self) {
            self.field_offsets.clear();
        }

        #[inline]
        fn close_on_err<T>(&mut self, r: Result<T, JournalExportReadError>) -> BufferState<T> {
            match r {
                Err(e) => {
                    self.parse_state = ParserState::Eof;
                    BufferState::Result(Err(e))
                }
                ok @ Ok(_) => BufferState::Result(ok),
            }
        }
    }

    pub enum BufferState<'a, T> {
        Result(Result<T, JournalExportReadError>),
        Underfilled(&'a mut [u8]),
        UnderfilledEntryStart(&'a mut [u8]),
    }

    enum ParserState {
        EntryStart,
        FieldStart,
        Fieldname,
        BinaryValueLen,
        BinaryValue,
        StringField,
        Eof,
    }

    pub struct EntryRef<'a> {
        reader: &'a JournalExportParser,
    }

    pub trait Entry<'a> {
        fn as_bytes(&self) -> &'a [u8];
        fn iter(&'a self) -> FieldIter<'a>;
    }

    impl<'a> EntryRef<'a> {
        pub fn as_bytes(&self) -> &'a [u8] {
            let start = self.reader.field_offsets[0].start;
            &self.reader.buf[start..self.reader.cursor]
        }

        pub fn iter(&'a self) -> FieldIter<'a> {
            FieldIter {
                index: 0,
                entry: self
            }
        }
    }

    pub struct FieldIter<'a> {
        index: usize,
        entry: &'a EntryRef<'a>,
    }

    impl<'a> Iterator for FieldIter<'a> {
        type Item = (&'a [u8], &'a [u8], FieldType);

        fn next(&mut self) -> Option<Self::Item> {
            let r = self.entry.reader;
            let res = next(&r.buf, r.cursor, &r.field_offsets, self.index);
            self.index += 1;
            res
        }
    }

    #[inline]
    fn next<'a>(buf: &'a ShiftBuffer<u8>, stop: Pointer, offsets: &'a [FieldOffset], index: usize) -> Option<(&'a[u8], &'a[u8], FieldType)>{
        let field_stop = if index == offsets.len() {
            // The cursor points to the first byte of the next entry. Thus,
            // cursor-2 points to the first NL after the last field of this
            // entry.
            stop - 2
        } else {
            // The fields are separated by one NL character, therefore
            // .start-1 of the next field points to the NL character that
            // terminates this field.
            offsets[index + 1].start - 1
        };
        let res = offsets.get(index).map(|f| {
            let bin_offset = match &f.typ {
                FieldType::Binary => 9,
                FieldType::String => 1,
            };
            (
                &buf[f.start..(f.start + f.namelen)],
                &buf[(f.start + f.namelen + bin_offset)..field_stop],
                f.typ.clone(),
            )
        });
        res
    }

    pub struct OwnedEntry {
        // tbd
    }

    #[derive(Clone, Debug)]
    pub enum FieldType {
        Binary,
        String,
    }

    struct FieldOffset {
        start: Pointer,
        namelen: usize,
        typ: FieldType,
    }
}

pub mod sync {
    use super::{
        parser::{BufferState, EntryRef, JournalExportParser},
        JournalExportReadError, DEFAULT_BUF_SIZE,
    };
    use std::io::Read;

    pub struct JournalExportRead<R> {
        buf_read: R,
        parse_state: JournalExportParser,
    }

    /// Read journal entries into a memory buffer which has at most
    impl<R: Read> JournalExportRead<R> {
        pub fn new(buf_read: R) -> Self {
            Self {
                buf_read,
                parse_state: JournalExportParser::new(DEFAULT_BUF_SIZE),
            }
        }

        pub fn parse_next(&mut self) -> Result<(), JournalExportReadError> {
            self.parse_state.clear_entry();
            loop {
                match self.parse_state.parse() {
                    BufferState::Result(Ok(())) => return Ok(()),
                    BufferState::Result(Err(e)) => {
                        return Err::<_, JournalExportReadError>(e);
                    }
                    BufferState::Underfilled(b) => {
                        let n = self.buf_read.read(b)?;
                        if n == 0 {
                            return Err(JournalExportReadError::UnexpectedEof);
                        }
                        self.parse_state.extend(n);
                    }
                    BufferState::UnderfilledEntryStart(b) => {
                        let n = self.buf_read.read(b)?;
                        if n == 0 {
                            return Err(JournalExportReadError::Eof);
                        }
                        self.parse_state.extend(n);
                    }
                }
            }
        }

        pub fn get_entry(&self) -> EntryRef<'_> {
            self.parse_state.get_entry()
        }
    }
}

pub struct JournalExportAsyncRead<R> {
    buf_read: R,
    parse_state: JournalExportParser,
}

/// Read journal entries into a memory buffer which has at most
impl<R: AsyncRead + Unpin> JournalExportAsyncRead<R> {
    pub fn new(buf_read: R) -> Self {
        Self {
            buf_read,
            parse_state: JournalExportParser::new(DEFAULT_BUF_SIZE),
        }
    }

    pub async fn parse_next(&mut self) -> Result<(), JournalExportReadError> {
        self.parse_state.clear_entry();
        loop {
            match self.parse_state.parse() {
                BufferState::Result(Ok(())) => return Ok(()),
                BufferState::Result(Err(e)) => return Err::<_, JournalExportReadError>(e),
                BufferState::Underfilled(b) => {
                    let n = self.buf_read.read(b).await?;
                    if n == 0 {
                        return Err(JournalExportReadError::UnexpectedEof);
                    }
                    self.parse_state.extend(n);
                }
                BufferState::UnderfilledEntryStart(b) => {
                    let n = self.buf_read.read(b).await?;
                    if n == 0 {
                        return Err(JournalExportReadError::Eof);
                    }
                    self.parse_state.extend(n);
                }
            }
        }
    }

    pub fn get_entry(&self) -> EntryRef<'_> {
        self.parse_state.get_entry()
    }
}

#[derive(Error, Debug)]
pub enum JournalExportReadError {
    #[error("IO error occured.")]
    IoError(#[from] std::io::Error),
    #[error("Unexpected character")]
    UnexpectedCharacter(u8),
    #[error("No more entries available.")]
    Eof,
    #[error("Unexpected Eof while parsing.")]
    UnexpectedEof,
}

impl JournalExportReadError {
    fn invalid_fieldname_char<T>(c: u8) -> Result<T, Self> {
        Err(Self::UnexpectedCharacter(c))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use super::{JournalExportRead, JournalExportReadError};

    #[test]
    fn can_parse_host_files() -> Result<(), Box<dyn std::error::Error + 'static>> {
        let test_files = match std::env::var("JOURNALD_TESTFILES") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let test_files: Vec<_> = test_files.split(',').collect();

        for fpath in test_files {
            let f = OpenOptions::new().read(true).open(fpath)?;

            let mut export_read = JournalExportRead::new(f);

            let mut count = 0usize;
            loop {
                match export_read.parse_next() {
                    Ok(_) => {
                        let e = export_read.get_entry();
                        let mut found_cursor = false;
                        let i = e.iter();
                        for (name, _content, _typ) in i {
                            let name = String::from_utf8_lossy(name);
                            let content = String::from_utf8_lossy(_content);
                            println!("{}={}", name, content);
                            if name == "__CURSOR" {
                                found_cursor = true;
                            }
                        }
                        assert!(found_cursor);
                        count += 1;
                    }
                    Err(JournalExportReadError::Eof) => break,
                    Err(e) => {
                        println!("{:?}", e);
                        panic!("{:?}", e);
                    }
                }
            }
            println!("count: {}", count);
        }

        Ok(())
    }
}
