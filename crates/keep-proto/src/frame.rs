/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::fmt;
use std::iter::FusedIterator;

use crate::header::Header;
use crate::limits::{EXT_WORD_LEN, HEADER_LEN, MAX_VALUE_LEN};

/// A framed message borrowed from a receive buffer; its fields are not yet
/// interpreted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Frame<'a> {
    /// The request id.
    pub id: u32,
    /// The op, status or push code.
    pub code: u8,
    /// The flag bits.
    pub flags: u8,
    /// The 32-bit argument.
    pub arg: u32,
    /// The extension words.
    pub ext: &'a [u8],
    /// The key bytes.
    pub key: &'a [u8],
    /// The value bytes.
    pub value: &'a [u8],
}

/// Outcome of decoding from the front of a buffer.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Decoded<T> {
    /// A whole frame of `len` bytes is at the front of the buffer.
    Complete {
        /// The decoded frame.
        frame: T,
        /// Its length in bytes.
        len: usize,
    },
    /// The buffer holds part of a frame; `needed` is the total frame length
    /// once the header is complete, otherwise the header length.
    Incomplete {
        /// Bytes needed before decoding can go on.
        needed: usize,
    },
}

/// A header that cannot be framed. Fatal for the connection.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FrameError {
    /// The header announces a value of this length, over the limit.
    ValueTooLarge(u32),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::ValueTooLarge(len) => {
                write!(f, "value length {len} exceeds {MAX_VALUE_LEN}")
            }
        }
    }
}

impl std::error::Error for FrameError {}

enum Split<'a> {
    Frame(Frame<'a>, &'a [u8]),
    Incomplete(usize),
}

impl<'a> Frame<'a> {
    /// Decodes the frame at the front of `buf`.
    #[inline]
    pub fn decode(buf: &'a [u8]) -> Result<Decoded<Frame<'a>>, FrameError> {
        Ok(match split(buf)? {
            Split::Frame(frame, rest) => Decoded::Complete {
                frame,
                len: buf.len() - rest.len(),
            },
            Split::Incomplete(needed) => Decoded::Incomplete { needed },
        })
    }

    /// Splits `buf` into its complete frames, stopping at the first partial
    /// one.
    pub fn split_all(buf: &'a [u8]) -> Frames<'a> {
        Frames { rest: buf }
    }
}

#[inline]
fn split(buf: &[u8]) -> Result<Split<'_>, FrameError> {
    let Some((head, body)) = buf.split_first_chunk::<HEADER_LEN>() else {
        return Ok(Split::Incomplete(HEADER_LEN));
    };
    let header = Header::decode(head);
    if header.value_len > MAX_VALUE_LEN {
        return Err(FrameError::ValueTooLarge(header.value_len));
    }
    let ext_len = usize::from(header.ext_words) * EXT_WORD_LEN;
    let key_len = usize::from(header.key_len);
    let value_len = header.value_len as usize;
    let parts = body.split_at_checked(ext_len).and_then(|(ext, rest)| {
        let (key, rest) = rest.split_at_checked(key_len)?;
        let (value, rest) = rest.split_at_checked(value_len)?;
        Some((ext, key, value, rest))
    });
    let Some((ext, key, value, rest)) = parts else {
        return Ok(Split::Incomplete(
            HEADER_LEN + ext_len + key_len + value_len,
        ));
    };
    Ok(Split::Frame(
        Frame {
            id: header.id,
            code: header.code,
            flags: header.flags,
            arg: header.arg,
            ext,
            key,
            value,
        },
        rest,
    ))
}

/// Iterator over the complete frames at the front of a buffer. A framing
/// error is yielded once and ends the iteration.
pub struct Frames<'a> {
    rest: &'a [u8],
}

impl<'a> Frames<'a> {
    /// The bytes after the frames yielded so far: a partial frame, or
    /// nothing once a framing error was yielded.
    pub fn remainder(&self) -> &'a [u8] {
        self.rest
    }
}

impl FusedIterator for Frames<'_> {}

impl<'a> Iterator for Frames<'a> {
    type Item = Result<Frame<'a>, FrameError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match split(self.rest) {
            Ok(Split::Frame(frame, rest)) => {
                self.rest = rest;
                Some(Ok(frame))
            }
            Ok(Split::Incomplete(_)) => None,
            Err(err) => {
                self.rest = &[];
                Some(Err(err))
            }
        }
    }
}
