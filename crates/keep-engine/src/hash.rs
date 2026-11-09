/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::hash::{Hash as _, Hasher};

use foldhash::SharedSeed;
use foldhash::fast::FoldHasher;

#[derive(Clone, Debug)]
pub struct KeyHasher {
    per_hasher: u64,
    shared: SharedSeed,
}

const PER_HASHER: u64 = 0x2D35_8DCC_AA6C_78A5;

impl KeyHasher {
    pub fn with_seed(seed: u64) -> KeyHasher {
        KeyHasher {
            per_hasher: seed.rotate_left(32) ^ PER_HASHER,
            shared: SharedSeed::from_u64(seed),
        }
    }

    #[inline]
    pub fn hash(&self, key: &[u8]) -> Hash {
        let mut hasher = FoldHasher::with_seed(self.per_hasher, &self.shared);
        key.hash(&mut hasher);
        Hash(hasher.finish())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Hash(u64);

impl Hash {
    #[inline]
    pub fn low(self) -> u32 {
        self.0 as u32
    }

    #[inline]
    pub fn middle(self) -> u32 {
        (self.0 >> 32) as u32 & 0x00FF_FFFF
    }

    #[inline]
    pub fn tag(self) -> u8 {
        ((self.0 >> 56) as u8).max(1)
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-engine/hash.rs"]
mod tests;
