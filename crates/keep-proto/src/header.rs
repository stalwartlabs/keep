/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::limits::{HEADER_LEN, MAX_KEY_LEN, MAX_VALUE_LEN};

/// The fixed 16-byte frame header, decoded.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Header {
    /// The request id.
    pub id: u32,
    /// The op, status or push code.
    pub code: u8,
    /// The flag bits.
    pub flags: u8,
    /// The key length in bytes.
    pub key_len: u8,
    /// The number of 4-byte extension words.
    pub ext_words: u8,
    /// The value length in bytes.
    pub value_len: u32,
    /// The 32-bit argument.
    pub arg: u32,
}

impl Header {
    /// Decodes a header from its 16 bytes.
    #[inline]
    pub fn decode(bytes: &[u8; HEADER_LEN]) -> Header {
        let [
            i0,
            i1,
            i2,
            i3,
            code,
            flags,
            key_len,
            ext_words,
            v0,
            v1,
            v2,
            v3,
            a0,
            a1,
            a2,
            a3,
        ] = *bytes;
        Header {
            id: u32::from_le_bytes([i0, i1, i2, i3]),
            code,
            flags,
            key_len,
            ext_words,
            value_len: u32::from_le_bytes([v0, v1, v2, v3]),
            arg: u32::from_le_bytes([a0, a1, a2, a3]),
        }
    }

    /// Encodes the header into its 16 bytes.
    #[inline]
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let word = u128::from(self.id)
            | u128::from(self.code) << 32
            | u128::from(self.flags) << 40
            | u128::from(self.key_len) << 48
            | u128::from(self.ext_words) << 56
            | u128::from(self.value_len) << 64
            | u128::from(self.arg) << 96;
        word.to_le_bytes()
    }
}

#[inline]
pub(crate) fn put_frame(
    out: &mut Vec<u8>,
    id: u32,
    code: u8,
    flags: u8,
    arg: u32,
    key: &[u8],
    value: &[u8],
) {
    debug_assert!(key.len() <= MAX_KEY_LEN && value.len() <= MAX_VALUE_LEN as usize);
    let header = Header {
        id,
        code,
        flags,
        key_len: key.len() as u8,
        ext_words: 0,
        value_len: value.len() as u32,
        arg,
    };
    out.reserve(HEADER_LEN + key.len() + value.len());
    out.extend_from_slice(&header.encode());
    out.extend_from_slice(key);
    out.extend_from_slice(value);
}
