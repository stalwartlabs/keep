/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::engine::Engine;
use crate::key::Key;
use crate::table::Room;
use crate::time::Time;
use crate::value::{INLINE_VALUE_LEN, LARGE_MIN, slot_size};

const WRITE_VICTIMS: usize = 256;

const UPKEEP_VICTIMS: usize = 64;

const LARGE_OVERSHOOT: usize = 16 << 20;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Eviction {
    Reject,
    Count,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Pinned([u64; 4]);

impl Pinned {
    pub const STALWART: Pinned = Pinned::new(&[0, 1, 14, 15, 16, 17]);

    pub const fn new(prefixes: &[u8]) -> Pinned {
        let mut set = [0u64; 4];
        let mut index = 0;
        while index < prefixes.len() {
            let prefix = prefixes[index];
            set[(prefix >> 6) as usize] |= 1 << (prefix & 63);
            index += 1;
        }
        Pinned(set)
    }

    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        key.first().is_some_and(|&first| {
            self.0
                .get(usize::from(first >> 6))
                .is_some_and(|word| word & (1 << (first & 63)) != 0)
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Cap {
    pub bytes: usize,
    pub eviction: Eviction,
    pub pinned: Pinned,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct MemoryUse {
    pub footprint: usize,
    pub large_bytes: usize,
    pub large_evicted: u64,
    pub large_orphaned: u64,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct Measured {
    footprint: usize,
    fixed: usize,
    stuck: usize,
}

impl Engine {
    pub(crate) fn measure(&mut self, cap: Cap) {
        let footprint = self.footprint();
        let values = &self.values;
        let warm = values.warm_bytes();
        let stuck = values.stuck_warm();
        let fixed = footprint
            .saturating_sub(values.large_bytes())
            .saturating_sub(warm);
        let reclaimable = warm.saturating_sub(stuck).min(values.retained());
        self.measured = Measured {
            footprint,
            fixed,
            stuck,
        };
        let over = fixed + stuck >= cap.bytes.saturating_add(cap.bytes / 4);
        if over {
            self.cold_from = INLINE_VALUE_LEN + 1;
        } else {
            self.cold_from = LARGE_MIN;
            self.credit = 0;
        }
        self.room = if over || fixed + stuck + reclaimable >= cap.bytes {
            Room::Capped(cap.eviction, cap.pinned)
        } else {
            Room::Free
        };
    }

    pub(crate) fn over_line(&self) -> bool {
        self.cold_from < LARGE_MIN
    }

    pub(crate) fn enforce_cap(&mut self, now: Time) {
        if let Some(cap) = self.cap {
            self.measure(cap);
            if cap.eviction == Eviction::Count {
                self.evict_large(cap.bytes, UPKEEP_VICTIMS, now);
            }
        }
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn large_size(&self, key: Key<'_>, len: usize) -> Option<usize> {
        let cap = self.cap.as_ref()?;
        if cap.pinned.contains(&key) {
            return None;
        }
        slot_size(len)
    }

    pub(crate) fn make_large_room(&mut self, size: usize, now: Time) -> bool {
        let Some(cap) = self.cap else {
            return true;
        };
        if self.measured.fixed + self.measured.stuck + self.values.retained() + size > cap.bytes {
            return false;
        }
        if cap.eviction == Eviction::Count {
            self.evict_large(cap.bytes.saturating_sub(size), WRITE_VICTIMS, now);
        }
        self.held() + size <= cap.bytes.saturating_add(LARGE_OVERSHOOT.min(cap.bytes / 8))
    }

    fn reserved(&self) -> usize {
        let values = &self.values;
        self.measured.fixed + self.measured.stuck + values.large_bytes() + values.retained()
    }

    fn held(&self) -> usize {
        let values = &self.values;
        self.measured.fixed
            + values.large_bytes()
            + values
                .warm_bytes()
                .max(self.measured.stuck + values.retained())
    }

    fn evict_large(&mut self, limit: usize, victims: usize, now: Time) {
        for _ in 0..victims {
            if self.reserved() <= limit || !self.evict_oldest(now) {
                return;
            }
            self.large_evicted += 1;
        }
    }

    fn evict_oldest(&mut self, now: Time) -> bool {
        let Some(key) = self.values.take_oldest_key() else {
            return false;
        };
        if let Some(key) = Key::new(key.as_bytes()) {
            self.delete(key, now);
        }
        if self.values.drop_orphan() {
            self.large_orphaned += 1;
        }
        true
    }

    pub fn memory_use(&self) -> MemoryUse {
        MemoryUse {
            footprint: self.measured.footprint,
            large_bytes: self.values.large_bytes(),
            large_evicted: self.large_evicted,
            large_orphaned: self.large_orphaned,
        }
    }
}
