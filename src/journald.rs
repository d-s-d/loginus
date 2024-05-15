//

use std::io::Read;

use thiserror::Error;

// We assume that 16KiB (half L1 cache on modern CPUs) is enough to hold at
// least one Journal Entry.
const BUF_PAGE_SIZE: usize = 4096 * 4;

pub struct BufJournalExportRead<R> {
    buf_read: R,
    buf: Vec<u8>,

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
}

/// Read journal entries into a memory buffer which has at most
impl<R: Read> BufJournalExportRead<R> {
    pub fn new(buf_read: R) -> Self {
        Self {
            buf_read,
            buf: vec![0; BUF_PAGE_SIZE],

            entry_start: 0,
            cur_pos: 0,
            field_start: 0,
            namelen: 0,
            remaining: 0,
            buf_stop: 0,

            parse_state: ParserState::FieldStart,
            field_offsets: vec![],
        }
    }

    //
    pub fn parse_next(
        &mut self,
    ) -> Result<impl Iterator<Item = (&[u8], &[u8], FieldType)>, JournaldReadError> {
        self.field_offsets.clear();

        loop {
            let r = self.cycle_buffer();
            self.close_on_err(r)?;

            if self.cur_pos == self.buf_stop {
                if !matches!(self.parse_state, ParserState::EntryStart) {
                    return self.close_on_err(Err(JournaldReadError::UnexpectedEof));
                } else {
                    return self.close_on_err(Err(JournaldReadError::Eof));
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
                        return self.close_on_err(Err(JournaldReadError::UnexpectedCharacter(c)));
                    }
                }
                FieldStart => match c {
                    b'\n' => {
                        if !self.field_offsets.is_empty() {
                            self.cur_pos += 1;
                            self.parse_state = ParserState::EntryStart;
                            return Ok(FieldIterator {
                                index: 0,
                                reader: self,
                            });
                        } else {
                            return self.close_on_err(Err(JournaldReadError::UnexpectedCharacter(c)));
                        }
                    }
                    c if (c.is_ascii_alphanumeric() || c == b'_') => {
                        self.field_start = self.cur_pos;
                        let res = ParserState::Fieldname;
                        self.cur_pos += 1;
                        res
                    }
                    c => {
                        return self.close_on_err(JournaldReadError::invalid_fieldname_char(c));
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
                        _ => self.close_on_err(Err(JournaldReadError::UnexpectedCharacter(c)))?,
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
                    let stop_pos = self.field_start + self.namelen + 9 + self.remaining as usize;
                    self.cur_pos = self.buf_stop.min(stop_pos);
                    if self.cur_pos < stop_pos {
                        ParserState::BinaryValue
                    } else {
                        c = self.buf[self.cur_pos];
                        if c != b'\n' {
                            return self
                                .close_on_err(Err(JournaldReadError::UnexpectedCharacter(c)));
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
                Eof => {
                    return Err(JournaldReadError::Eof)
                }
            }
        }
    }

    // cycle_buffer performs one of two actions, if the cursor position is at
    // the end of the buffer:
    //
    // If the stop position is at the end of the buffer and the entry starts at
    // position 0 (the buffer is too small to hold the current entry), the
    // buffer is extended.
    //
    // Otherwise, if the current entry does not start at the beginning of the
    // buffer, the buffer is 'shifted'; i.e. the content from the current
    // starting position is moved to the beginning of the buffer. Additionally,
    // all existing parser states are shifted as well as the offset relative to
    // the start of the buffer changed.
    //
    // In either case, cycle_buffer will attempt to fill the buffer by calling
    // read() on the underlying Read. While it is not guaranteed that the buffer
    // will be filled entirely, the stop position is adjusted acccordingly.
    //
    // # Postcondition
    // `prev(buf_stop) - prev(entry_start) <= buf_stop - entry_start`
    #[inline]
    fn cycle_buffer(&mut self) -> Result<(), JournaldReadError> {
        if self.cur_pos == self.buf_stop {
            if self.buf_stop == self.buf.len() {
                if self.field_start == 0 {
                    // increase buffer size
                    println!("increase buffer size");
                    self.buf.extend((0..BUF_PAGE_SIZE).map(|_| 0u8))
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
            let l = self.buf.len();
            self.buf_stop += self.buf_read.read(&mut self.buf[self.buf_stop..l])?;
        }
        Ok(())
    }

    fn close_on_err<T>(&mut self, r: Result<T, JournaldReadError>) -> Result<T, JournaldReadError> {
        match r {
            Err(e) => {
                self.parse_state = ParserState::Eof;
                Err(e)
            }
            ok @ Ok(_) => ok,
        }
    }
}

#[derive(Error, Debug)]
pub enum JournaldReadError {
    #[error("IO error occured.")]
    IoError(#[from] std::io::Error),
    #[error("Unexpected character")]
    UnexpectedCharacter(u8),
    #[error("No more entries available.")]
    Eof,
    #[error("Unexpected Eof while parsing.")]
    UnexpectedEof,
}

impl JournaldReadError {
    fn invalid_fieldname_char<T>(c: u8) -> Result<T, Self> {
        Err(Self::UnexpectedCharacter(c))
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

enum ParserState {
    EntryStart,
    FieldStart,
    Fieldname,
    BinaryValueLen, 
    BinaryValue,
    StringField,
    Eof,
}

struct FieldIterator<'a, R> {
    index: usize,
    reader: &'a BufJournalExportRead<R>,
}

impl<'a, R> Iterator for FieldIterator<'a, R> {
    type Item = (&'a [u8], &'a [u8], FieldType);

    fn next(&mut self) -> Option<Self::Item> {
        let field_stop = if self.index + 1 < self.reader.field_offsets.len() {
            self.reader.field_offsets[self.index + 1].start - 1
        } else {
            self.reader.cur_pos - 2
        };
        let res = self.reader.field_offsets.get(self.index).map(|f| {
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

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use super::{BufJournalExportRead, JournaldReadError};

    #[test]
    fn can_parse_host_files() -> Result<(), Box<dyn std::error::Error + 'static>> {
        let test_files = std::env::var("JOURNALD_TESTFILES").unwrap_or_default();
        let test_files: Vec<_> = test_files.split(',').collect();

        for fpath in test_files {
            let f = OpenOptions::new()
                .read(true)
                .open(fpath)?;

            let mut export_read = BufJournalExportRead::new(f);

            loop {
                match export_read.parse_next() {
                    Ok(i) => {
                        let mut found_cursor = false;
                        for (name, _content, _typ) in i {
                            if name == b"__CURSOR" {
                                found_cursor = true;
                            }
                        }
                        assert!(found_cursor);
                    }
                    Err(JournaldReadError::Eof) => {
                        break;
                    }
                    Err(e) => {
                        println!("{:?}", e);
                    }
                }
            }
        }

        Ok(())
    }
}
