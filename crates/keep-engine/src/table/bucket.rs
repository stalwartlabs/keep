/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::hint::black_box;

use crate::entry::Stored;
use crate::time::Time;

#[repr(C, align(64))]
pub struct Bucket<E, const L: usize, const H: usize> {
    header: [u8; H],
    entries: [E; L],
}

const SATURATED: u8 = u8::MAX;

#[inline]
pub fn lanes(mut mask: u32) -> impl Iterator<Item = usize> {
    std::iter::from_fn(move || {
        (mask != 0).then(|| {
            let lane = mask.trailing_zeros() as usize;
            mask &= mask - 1;
            lane
        })
    })
}

impl<E: Stored, const L: usize, const H: usize> Bucket<E, L, H> {
    const ALL: u32 = (1 << L) - 1;
    const LINES: usize = size_of::<Self>() / 64;

    pub fn empty() -> Self {
        Bucket {
            header: [0; H],
            entries: std::array::from_fn(|_| E::vacant()),
        }
    }

    #[inline]
    pub fn tagged(&self, tag: u8) -> u32 {
        let mut mask = 0;
        for (lane, &byte) in self.header.iter().take(L).enumerate() {
            mask |= u32::from(byte == tag) << lane;
        }
        mask
    }

    #[inline]
    pub fn occupied(&self) -> u32 {
        !self.tagged(0) & Self::ALL
    }

    #[inline]
    pub fn outbound(&self) -> u8 {
        self.header.last().copied().unwrap_or(0)
    }

    #[inline]
    pub fn bump(&mut self) {
        if let Some(count) = self.header.last_mut() {
            *count = count.saturating_add(1);
        }
    }

    #[inline]
    pub fn unbump(&mut self) {
        if let Some(count) = self.header.last_mut()
            && *count != SATURATED
        {
            *count = count.saturating_sub(1);
        }
    }

    #[inline]
    pub fn touch_lines(&self) {
        for line in 1..Self::LINES {
            if let Some(entry) = self.entries.get((line * 64 - H) / size_of::<E>()) {
                black_box(entry.deadline());
            }
        }
    }

    #[inline]
    pub fn find(&self, tag: u8, probe: &E::Probe<'_>) -> Option<usize> {
        lanes(self.tagged(tag)).find(|&lane| {
            self.entries
                .get(lane)
                .is_some_and(|entry| entry.matches(probe))
        })
    }

    #[inline]
    pub fn entry(&self, lane: usize) -> Option<&E> {
        self.entries.get(lane)
    }

    #[inline]
    pub fn entry_mut(&mut self, lane: usize) -> Option<&mut E> {
        self.entries.get_mut(lane)
    }

    #[inline]
    pub fn expired(&self, occupied: u32, now: Time) -> u32 {
        let mut mask = 0;
        for (lane, entry) in self.entries.iter().enumerate() {
            mask |= u32::from(entry.deadline().is_expired(now)) << lane;
        }
        mask & occupied
    }

    pub fn expired_lane(&self, now: Time) -> Option<usize> {
        lanes(self.occupied()).find(|&lane| {
            self.entries
                .get(lane)
                .is_some_and(|entry| entry.deadline().is_expired(now))
        })
    }

    pub fn put(&mut self, lane: usize, tag: u8, entry: E) {
        if let (Some(slot_tag), Some(slot)) =
            (self.header.get_mut(lane), self.entries.get_mut(lane))
        {
            *slot_tag = tag;
            *slot = entry;
        }
    }

    pub fn take(&mut self, lane: usize) -> Option<E> {
        let tag = self.header.get_mut(lane).filter(|tag| **tag != 0)?;
        *tag = 0;
        let slot = self.entries.get_mut(lane)?;
        Some(std::mem::replace(slot, E::vacant()))
    }

    pub fn into_entries(self) -> impl Iterator<Item = E> {
        let occupied = self.occupied();
        self.entries
            .into_iter()
            .enumerate()
            .filter_map(move |(lane, entry)| (occupied & (1 << lane) != 0).then_some(entry))
    }

    pub fn for_each(&self, f: &mut impl FnMut(&E)) {
        lanes(self.occupied())
            .filter_map(|lane| self.entries.get(lane))
            .for_each(f);
    }
}
