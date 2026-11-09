/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::ops::Deref;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Key<'a>(&'a [u8]);

impl<'a> Key<'a> {
    pub const MAX_LEN: usize = u8::MAX as usize;

    #[inline]
    pub fn new(bytes: &'a [u8]) -> Option<Key<'a>> {
        (1..=Self::MAX_LEN)
            .contains(&bytes.len())
            .then_some(Key(bytes))
    }

    #[inline]
    pub fn as_bytes(self) -> &'a [u8] {
        self.0
    }

    #[inline]
    pub fn len_u8(self) -> u8 {
        self.0.len() as u8
    }

    #[inline]
    pub fn class(self) -> KeyClass {
        KeyClass::of(self.0.len())
    }
}

impl Deref for Key<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum KeyClass {
    K8,
    K16,
    K24,
    K32,
    K40,
    K48,
    Long,
}

impl KeyClass {
    pub const ALL: [KeyClass; 7] = [
        KeyClass::K8,
        KeyClass::K16,
        KeyClass::K24,
        KeyClass::K32,
        KeyClass::K40,
        KeyClass::K48,
        KeyClass::Long,
    ];

    #[inline]
    pub fn of(len: usize) -> KeyClass {
        match len {
            0..=8 => KeyClass::K8,
            9..=16 => KeyClass::K16,
            17..=24 => KeyClass::K24,
            25..=32 => KeyClass::K32,
            33..=40 => KeyClass::K40,
            41..=48 => KeyClass::K48,
            _ => KeyClass::Long,
        }
    }
}

#[inline]
pub fn same_key(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    match a.len() {
        8..=16 => {
            a.first_chunk::<8>() == b.first_chunk::<8>()
                && a.last_chunk::<8>() == b.last_chunk::<8>()
        }
        17..=32 => {
            a.first_chunk::<16>() == b.first_chunk::<16>()
                && a.last_chunk::<16>() == b.last_chunk::<16>()
        }
        _ => a == b,
    }
}
