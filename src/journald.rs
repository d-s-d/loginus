//! Parser for the Journal Export Format.
//!
//! [self::parser::JournalExportParser] contains the parser logic and manages
//! the buffer. The [JournalExportAsyncRead] and [sync::JournalExportRead]
//! provide async and sync versions of a parser.
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
//! size. Currently, there is no mechanism to decrease the buffer size again;
//! such an extensions might be of interest in networking applications that
//! consume data from potentially untrustworthy sources.
//!
//! ## Implementation notes
//!
//! Both, [sync::JournalExportRead] and [JournalExportAsyncRead] are stateful
//! objects that buffer the last parsed journal entry. The latter can be
//! accessed using the `get_entry()`-method which returns a [parser::RefEntry]
//! object.

use thiserror::Error;

use crate::config::JournalExportLimits;

use self::parser::{JournalExportParser, ParseResult};
pub use self::{parser::RefEntry, sync::JournalExportRead};
use futures::{AsyncRead, AsyncReadExt};

// We assume that 16KiB (half the L1 cache on modern CPUs) is enough to hold at
// least one Journal Entry.
const DEFAULT_BUF_SIZE: usize = 1 << 14;

pub trait Entry {
    fn as_bytes(&self) -> &[u8];
    fn iter(&self) -> parser::FieldIter<'_>;
}

pub mod parser {
    use crate::{
        config::JournalExportLimits,
        shiftbuffer::{Pointer, ShiftBuffer},
    };

    use super::{Entry, JournalExportReadError};

    pub struct JournalExportParser {
        buf: ShiftBuffer<u8>,
        entry_start: Pointer,
        field_start: Pointer,
        cursor: Pointer,
        namelen: usize,
        remaining: u64,
        parse_state: ParserState,
        buffer_state: BufferState,
        field_offsets: Vec<FieldOffset>,
        limits: JournalExportLimits,
    }

    impl JournalExportParser {
        pub fn new(limits: JournalExportLimits, buf_size: usize) -> Self {
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
                buffer_state: BufferState::Underfilled,
                field_offsets: vec![],
                limits,
            }
        }

        pub fn extend(&mut self, n: usize) {
            self.buf.extend(n);
        }

        #[inline]
        pub fn parse(&mut self) -> ParseResult<()> {
            loop {
                // If the cursor reached the upper end of the window, ask for
                // more byte from the user.
                if self.cursor == self.buf.upper() {
                    if self.buffer_state == BufferState::Filled {
                        if self.parse_state == ParserState::EntryStart {
                            return ParseResult::Eof;
                        }
                        return ParseResult::Err(JournalExportReadError::UnexpectedEof);
                    }
                    self.buffer_state = BufferState::Filled;
                    return ParseResult::Underfilled(self.buf.make_room());
                }
                debug_assert!(self.cursor < self.buf.upper());
                self.buffer_state = BufferState::Underfilled;

                let c = self.buf[self.cursor];
                use ParserState::*;
                self.parse_state = match self.parse_state {
                    EntryStart => {
                        if c.is_ascii_alphabetic() || c == b'_' {
                            self.entry_start = self.cursor;
                            self.field_start = self.entry_start;
                            self.cursor += 1;
                            ParserState::Fieldname
                        } else {
                            return self
                                .eof_and_return(JournalExportReadError::UnexpectedCharacter(c));
                        }
                    }
                    FieldStart => match c {
                        b'\n' => {
                            if !self.field_offsets.is_empty() {
                                self.cursor += 1;
                                self.parse_state = ParserState::EntryStart;
                                return ParseResult::Ok(());
                            } else {
                                return self.eof_and_return(
                                    JournalExportReadError::UnexpectedCharacter(c),
                                );
                            }
                        }
                        c if (c.is_ascii_alphanumeric() || c == b'_') => {
                            self.field_start = self.cursor;
                            self.cursor += 1;
                            ParserState::Fieldname
                        }
                        c => {
                            return self
                                .eof_and_return(JournalExportReadError::UnexpectedCharacter(c));
                        }
                    },
                    Fieldname => {
                        self.namelen = self.cursor - self.field_start;
                        if self.namelen > self.limits.max_field_name_len {
                            return self.eof_and_return(JournalExportReadError::FieldNameTooLong);
                        }
                        self.cursor += 1;
                        match c {
                            c_ if c_.is_ascii_alphanumeric() || c_ == b'_' => {
                                ParserState::Fieldname
                            }
                            b'=' => ParserState::StringField,
                            b'\n' => ParserState::BinaryValueLen,
                            _ => {
                                self.cursor -= 1;
                                return self.eof_and_return(
                                    JournalExportReadError::UnexpectedCharacter(c),
                                );
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
                            if self.remaining > self.limits.max_field_value_size as u64 {
                                return self
                                    .eof_and_return(JournalExportReadError::FieldValueTooLong);
                            }
                            ParserState::BinaryValue
                        }
                    }
                    BinaryValue => {
                        let stop_pos =
                            self.field_start + self.namelen + 9 + self.remaining as usize;
                        if self.cursor < stop_pos {
                            self.cursor = self.buf.upper().min(stop_pos);
                            ParserState::BinaryValue
                        } else {
                            if c != b'\n' {
                                return self.eof_and_return(
                                    JournalExportReadError::UnexpectedCharacter(c),
                                );
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
                            if self.cursor - self.field_start - self.namelen - 1
                                > self.limits.max_field_value_size
                            {
                                self.cursor -= 1;
                                return self
                                    .eof_and_return(JournalExportReadError::FieldValueTooLong);
                            }
                            ParserState::StringField
                        }
                    }
                    Eof => return ParseResult::Eof,
                }
            }
        }

        #[inline]
        pub fn get_entry(&self) -> RefEntry<'_> {
            RefEntry { reader: self }
        }

        #[inline]
        pub fn clear_entry(&mut self) {
            self.field_offsets.clear();
        }

        #[inline]
        fn eof_and_return<T>(&mut self, r: JournalExportReadError) -> ParseResult<T> {
            self.parse_state = ParserState::Eof;
            ParseResult::Err(r)
        }
    }

    pub enum ParseResult<'a, T> {
        Ok(T),
        Err(JournalExportReadError),
        Underfilled(&'a mut [u8]),
        Eof,
    }

    #[derive(PartialEq, Eq)]
    enum ParserState {
        EntryStart,
        FieldStart,
        Fieldname,
        BinaryValueLen,
        BinaryValue,
        StringField,
        Eof,
    }

    #[derive(PartialEq, Eq)]
    enum BufferState {
        Underfilled,
        Filled,
    }

    pub struct RefEntry<'a> {
        reader: &'a JournalExportParser,
    }

    impl<'a> RefEntry<'a> {
        pub fn to_owned(&self) -> OwnedEntry {
            OwnedEntry {
                cursor: self.reader.cursor,
                buf: self.reader.buf.clone_window(),
                offsets: self.reader.field_offsets.to_vec(),
            }
        }
    }

    impl<'a> Entry for RefEntry<'a> {
        fn as_bytes(&self) -> &[u8] {
            let start = self.reader.field_offsets[0].start;
            &self.reader.buf[start..self.reader.cursor]
        }

        fn iter(&self) -> FieldIter<'_> {
            FieldIter {
                index: 0,
                buf: &self.reader.buf,
                cursor: self.reader.cursor,
                offsets: &self.reader.field_offsets,
            }
        }
    }

    pub struct OwnedEntry {
        cursor: Pointer,
        buf: ShiftBuffer<u8>,
        offsets: Vec<FieldOffset>,
    }

    impl Entry for OwnedEntry {
        fn as_bytes(&self) -> &[u8] {
            let start = self.offsets[0].start;
            &self.buf[start..self.cursor]
        }

        fn iter(&self) -> FieldIter<'_> {
            FieldIter {
                index: 0,
                buf: &self.buf,
                cursor: self.cursor,
                offsets: &self.offsets,
            }
        }
    }

    pub struct FieldIter<'a> {
        index: usize,
        cursor: Pointer,
        buf: &'a ShiftBuffer<u8>,
        offsets: &'a [FieldOffset],
    }

    impl<'a> Iterator for FieldIter<'a> {
        type Item = (&'a [u8], &'a [u8], FieldType);

        fn next(&mut self) -> Option<Self::Item> {
            let res = next(self.buf, self.cursor, self.offsets, self.index);
            self.index += 1;
            res
        }
    }

    fn next<'a>(
        buf: &'a ShiftBuffer<u8>,
        stop: Pointer,
        offsets: &'a [FieldOffset],
        index: usize,
    ) -> Option<(&'a [u8], &'a [u8], FieldType)> {
        if index == offsets.len() {
            return None;
        }
        let field_stop = if index == offsets.len() - 1 {
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

    #[derive(Clone, Debug)]
    pub enum FieldType {
        Binary,
        String,
    }

    #[derive(Clone)]
    struct FieldOffset {
        start: Pointer,
        namelen: usize,
        typ: FieldType,
    }
}

pub mod sync {
    use crate::config::JournalExportLimits;

    use super::{
        parser::{JournalExportParser, OwnedEntry, ParseResult, RefEntry},
        JournalExportReadError, DEFAULT_BUF_SIZE,
    };
    use std::io::Read;

    pub struct JournalExportRead<R> {
        buf_read: R,
        parse_state: JournalExportParser,
    }

    impl<R: Read> JournalExportRead<R> {
        pub fn new(buf_read: R) -> Self {
            Self::new_with_limits(JournalExportLimits::default(), buf_read)
        }

        pub fn new_with_limits(limits: JournalExportLimits, buf_read: R) -> Self {
            Self {
                buf_read,
                parse_state: JournalExportParser::new(limits, DEFAULT_BUF_SIZE),
            }
        }

        pub fn parse_next(&mut self) -> Result<Option<()>, JournalExportReadError> {
            self.parse_state.clear_entry();
            loop {
                match self.parse_state.parse() {
                    ParseResult::Ok(()) => return Ok(Some(())),
                    ParseResult::Eof => return Ok(None),
                    ParseResult::Err(e) => {
                        return Err::<_, JournalExportReadError>(e);
                    }
                    ParseResult::Underfilled(b) => {
                        let n = self.buf_read.read(b)?;
                        self.parse_state.extend(n);
                    }
                }
            }
        }

        pub fn get_entry(&self) -> RefEntry<'_> {
            self.parse_state.get_entry()
        }
    }

    impl<R: Read> Iterator for JournalExportRead<R> {
        type Item = OwnedEntry;

        fn next(&mut self) -> Option<Self::Item> {
            self.parse_next().ok()??;
            Some(self.get_entry().to_owned())
        }
    }
}

pub struct JournalExportAsyncRead<R> {
    buf_read: R,
    parse_state: JournalExportParser,
}

/// Read journal entries into a memory buffer which has at most
impl<R: AsyncRead + Unpin> JournalExportAsyncRead<R> {
    pub fn new(limits: JournalExportLimits, buf_read: R) -> Self {
        Self {
            buf_read,
            parse_state: JournalExportParser::new(limits, DEFAULT_BUF_SIZE),
        }
    }

    pub async fn parse_next(&mut self) -> Result<Option<()>, JournalExportReadError> {
        self.parse_state.clear_entry();
        loop {
            match self.parse_state.parse() {
                ParseResult::Ok(()) => return Ok(Some(())),
                ParseResult::Eof => return Ok(None),
                ParseResult::Err(e) => return Err::<_, JournalExportReadError>(e),
                ParseResult::Underfilled(b) => {
                    let n = self.buf_read.read(b).await?;
                    self.parse_state.extend(n);
                }
            }
        }
    }

    pub fn get_entry(&self) -> RefEntry<'_> {
        self.parse_state.get_entry()
    }
}

#[derive(Error, Debug)]
pub enum JournalExportReadError {
    #[error("IO error occured.")]
    IoError(#[from] std::io::Error),
    #[error("Unexpected character")]
    UnexpectedCharacter(u8),
    #[error("Unexpected Eof while parsing.")]
    UnexpectedEof,
    #[error("Field name exceeds maximum allowed length.")]
    FieldNameTooLong,
    #[error("Field value maximum allowed length.")]
    FieldValueTooLong,
    #[error("Total size of journal entry exceeds maximum allowed size.")]
    EntryTooLarge,
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use super::{Entry, JournalExportRead};

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
                    Ok(Some(_)) => {
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
                    Ok(None) => break,
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
