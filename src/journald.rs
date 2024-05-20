//! Parse a journal entries given in the Journal Export Format.
//!
//! The structure is optimized for situations where the journal entry is
//! immediately 'reduced' for further processed; for example, for most
//! applications, most of the fields are ignored or their representation changes
//! based on the type. It'd thus be wasteful to create heap-allocated objects
//! for each entry or even field upfront.
//!
//! The journal entries are read into a buffer whose size is only increased (by
//! the initial buffer size) if a single entry is larger than the current buffer
//! size. Currently, there is no mechanism to decrease the buffer size
//! again; such an extensions might be of interest in networking applications.
//!
//! [parser::JournalExportParseBuffer] manages the buffer and corresponding
//! parser state. It is separated out from I/O-related code:
//! [JournalExportAsyncRead] and [sync::JournalExportRead] provide async and
//! sync versions of a parser.

use thiserror::Error;

use self::parser::{BufferState, JournalExportParseBuffer};
pub use self::{parser::JournalEntry, sync::JournalExportRead};
use futures::{AsyncRead, AsyncReadExt};

// We assume that 16KiB (half L1 cache on modern CPUs) is enough to hold at
// least one Journal Entry.
const DEFAULT_BUF_SIZE: usize = 4096 * 4;

pub mod parser {
    use super::JournalExportReadError;

    pub struct JournalExportParseBuffer {
        buf: Vec<u8>,
        buf_page_size: usize,

        // buffer state
        entry_start: usize,
        cur_pos: usize,
        buf_stop: usize,

        // parser state
        field_start: usize,
        namelen: usize,
        remaining: u64,
        parse_state: ParserState,

        field_offsets: Vec<FieldOffset>,
        completed_field_offsets: Vec<FieldOffset>,
    }

    impl JournalExportParseBuffer {
        pub fn new(buf_size: usize) -> Self {
            Self {
                buf: vec![0; buf_size],
                buf_page_size: buf_size,

                entry_start: 0,
                cur_pos: 0,
                field_start: 0,
                namelen: 0,
                remaining: 0,
                buf_stop: 0,

                parse_state: ParserState::FieldStart,
                field_offsets: vec![],
                completed_field_offsets: vec![],
            }
        }

        pub fn free(&mut self) -> &mut [u8] {
            &mut self.buf[self.buf_stop..]
        }

        pub fn advance(&mut self, n: usize) {
            assert!(n <= self.buf.len() - self.buf_stop);
            self.buf_stop += n;
        }

        // cycle_buffer performs one of two actions:
        //
        // If the stop position is at the end of the buffer and the entry starts at
        // position 0 (the buffer is too small to hold the current entry), the
        // buffer is extended.
        //
        // Otherwise, the buffer is 'shifted'; i.e. the content from the current
        // starting position is moved to the beginning of the buffer. Additionally,
        // all existing parser states are shifted as well as the offset relative to
        // the start of the buffer changed.
        //
        // In either case, a reference to a slice is returned that covers the unused
        // remainder of the buffer. The slice is guaranteed to have non-zero size.
        //
        // # Postcondition
        // `prev(buf_stop) - prev(entry_start) <= buf_stop - entry_start`
        #[inline]
        pub fn cycle_buffer(&mut self) -> &mut [u8] {
            if self.buf_stop == self.buf.len() {
                if self.field_start == 0 {
                    // increase buffer size
                    self.buf.extend((0..self.buf_page_size).map(|_| 0u8))
                } else {
                    // shift all existing entries
                    for s in self.field_offsets.iter_mut() {
                        s.shift(self.entry_start);
                    }
                    // shift buffer
                    for p in 0..(self.buf_stop - self.entry_start) {
                        self.buf[p] = self.buf[p + self.entry_start]
                    }
                    self.cur_pos -= self.entry_start;
                    self.buf_stop -= self.entry_start;
                    self.field_start -= self.entry_start;
                    self.entry_start = 0;
                }
            }
            self.free()
        }

        #[inline]
        pub fn parse(&mut self) -> BufferState<()> {
            loop {
                if self.cur_pos == self.buf_stop {
                    if !matches!(self.parse_state, ParserState::EntryStart) {
                        return BufferState::Underfilled(self.cycle_buffer());
                    } else {
                        return self.close_on_err(Err(JournalExportReadError::Eof));
                    }
                }

                let mut c = self.buf[self.cur_pos];

                use ParserState::*;
                self.parse_state = match self.parse_state {
                    EntryStart => {
                        if c.is_ascii_alphanumeric() || c == b'_' {
                            self.entry_start = self.cur_pos;
                            self.field_start = self.cur_pos;
                            self.cur_pos += 1;
                            ParserState::Fieldname
                        } else {
                            return self
                                .close_on_err(Err(JournalExportReadError::UnexpectedCharacter(c)));
                        }
                    }
                    FieldStart => match c {
                        b'\n' => {
                            if !self.field_offsets.is_empty() {
                                self.cur_pos += 1;
                                self.parse_state = ParserState::EntryStart;
                                return BufferState::Result(Ok(()));
                            } else {
                                return self.close_on_err(Err(
                                    JournalExportReadError::UnexpectedCharacter(c),
                                ));
                            }
                        }
                        c if (c.is_ascii_alphanumeric() || c == b'_') => {
                            self.field_start = self.cur_pos;
                            let res = ParserState::Fieldname;
                            self.cur_pos += 1;
                            res
                        }
                        c => {
                            return self
                                .close_on_err(JournalExportReadError::invalid_fieldname_char(c));
                        }
                    },
                    Fieldname => {
                        let start = self.field_start;
                        self.namelen = self.cur_pos - start;
                        let res = match c {
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
                        };
                        self.cur_pos += 1;
                        res
                    }
                    BinaryValueLen => {
                        // [fieldname]  '\n'  [64bit le integer]
                        // <-namelen-> +  1 + <----8 bytes----->
                        let len_stop = self.field_start + self.namelen + 9;
                        self.cur_pos = self.buf_stop.min(len_stop);
                        if self.cur_pos < len_stop {
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
                        self.cur_pos = self.buf_stop.min(stop_pos);
                        if self.cur_pos < stop_pos {
                            ParserState::BinaryValue
                        } else {
                            c = self.buf[self.cur_pos];
                            if c != b'\n' {
                                return self.close_on_err(Err(
                                    JournalExportReadError::UnexpectedCharacter(c),
                                ));
                            }
                            self.cur_pos += 1;
                            self.field_offsets.push(FieldOffset {
                                start: self.field_start,
                                namelen: self.namelen,
                                typ: FieldType::Binary,
                            });
                            ParserState::FieldStart
                        }
                    }
                    StringField => {
                        self.cur_pos += 1;
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

        pub fn get_entry(&mut self) -> JournalEntry<'_> {
            std::mem::swap(&mut self.field_offsets, &mut self.completed_field_offsets);
            self.field_offsets.clear();
            JournalEntry {
                index: 0,
                reader: self,
            }
        }

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

    pub struct JournalEntry<'a> {
        index: usize,
        reader: &'a JournalExportParseBuffer,
    }

    impl<'a> JournalEntry<'a> {
        pub fn as_bytes(&self) -> &'a [u8] {
            let start = self.reader.completed_field_offsets[0].start;
            &self.reader.buf[start..self.reader.cur_pos]
        }
    }

    impl<'a> Iterator for JournalEntry<'a> {
        type Item = (&'a [u8], &'a [u8], FieldType);

        fn next(&mut self) -> Option<Self::Item> {
            let field_stop = if self.index + 1 < self.reader.completed_field_offsets.len() {
                self.reader.completed_field_offsets[self.index + 1].start - 1
            } else {
                self.reader.cur_pos - 2
            };
            let res = self
                .reader
                .completed_field_offsets
                .get(self.index)
                .map(|f| {
                    let bin_offset = match &f.typ {
                        FieldType::Binary => 9,
                        FieldType::String => 1,
                    };
                    (
                        &self.reader.buf[f.start..(f.start + f.namelen)],
                        &self.reader.buf[(f.start + f.namelen + bin_offset)..field_stop],
                        f.typ.clone(),
                    )
                });
            self.index += 1;
            res
        }
    }

    #[derive(Clone, Debug)]
    pub enum FieldType {
        Binary,
        String,
    }

    struct FieldOffset {
        start: usize,
        namelen: usize,
        typ: FieldType,
    }

    impl FieldOffset {
        fn shift(&mut self, amount: usize) {
            self.start -= amount;
        }
    }
}

pub mod sync {
    use super::{
        parser::{BufferState, JournalEntry, JournalExportParseBuffer},
        JournalExportReadError, DEFAULT_BUF_SIZE,
    };
    use std::io::Read;

    pub struct JournalExportRead<R> {
        buf_read: R,
        parse_state: JournalExportParseBuffer,
    }

    /// Read journal entries into a memory buffer which has at most
    impl<R: Read> JournalExportRead<R> {
        pub fn new(buf_read: R) -> Self {
            Self {
                buf_read,
                parse_state: JournalExportParseBuffer::new(DEFAULT_BUF_SIZE),
            }
        }

        pub fn parse_next(&mut self) -> Result<JournalEntry<'_>, JournalExportReadError> {
            loop {
                match self.parse_state.parse() {
                    BufferState::Result(Ok(())) => break,
                    BufferState::Result(Err(e)) => return Err::<_, JournalExportReadError>(e),
                    BufferState::Underfilled(b) => {
                        let n = self.buf_read.read(b)?;
                        if n == 0 {
                            return Err(JournalExportReadError::UnexpectedEof);
                        }
                        self.parse_state.advance(n);
                    }
                }
            }

            Ok(self.parse_state.get_entry())
        }
    }
}

pub struct JournalExportAsyncRead<R> {
    buf_read: R,
    parse_state: JournalExportParseBuffer,
}

/// Read journal entries into a memory buffer which has at most
impl<R: AsyncRead + Unpin> JournalExportAsyncRead<R> {
    pub fn new(buf_read: R) -> Self {
        Self {
            buf_read,
            parse_state: JournalExportParseBuffer::new(DEFAULT_BUF_SIZE),
        }
    }

    pub async fn parse_next(&mut self) -> Result<JournalEntry<'_>, JournalExportReadError> {
        loop {
            match self.parse_state.parse() {
                BufferState::Result(Ok(())) => return Ok(self.parse_state.get_entry()),
                BufferState::Result(Err(e)) => return Err::<_, JournalExportReadError>(e),
                BufferState::Underfilled(b) => {
                    let n = self.buf_read.read(b).await?;
                    if n == 0 {
                        return Err(JournalExportReadError::UnexpectedEof);
                    }
                    self.parse_state.advance(n);
                }
            }
        }
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
        let test_files = std::env::var("JOURNALD_TESTFILES").unwrap_or_default();
        let test_files: Vec<_> = test_files.split(',').collect();

        for fpath in test_files {
            let f = OpenOptions::new().read(true).open(fpath)?;

            let mut export_read = JournalExportRead::new(f);

            loop {
                match export_read.parse_next() {
                    Ok(i) => {
                        let mut found_cursor = false;
                        for (name, _content, _typ) in i {
                            let name = String::from_utf8_lossy(name);
                            let content = String::from_utf8_lossy(_content);
                            println!("{}={}", name, content);
                            if name == "__CURSOR" {
                                found_cursor = true;
                            }
                        }
                        assert!(found_cursor);
                    }
                    Err(JournalExportReadError::Eof | JournalExportReadError::UnexpectedEof) => {
                        break;
                    }
                    Err(e) => {
                        println!("{:?}", e);
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}
