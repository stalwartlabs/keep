/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::num::NonZeroU16;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use super::records::Buckets;

#[derive(Debug)]
#[repr(align(128))]
struct Board {
    interest: [AtomicU64; 4],
    consumed: Box<[AtomicU64]>,
}

#[derive(Debug, Default)]
#[repr(align(128))]
struct Subscribers(AtomicU32);

#[derive(Debug)]
pub struct Hub {
    boards: Box<[Board]>,
    subscribers: Subscribers,
}

impl Hub {
    pub fn new(shards: NonZeroU16) -> Hub {
        let count = usize::from(shards.get());
        Hub {
            boards: (0..count)
                .map(|_| Board {
                    interest: Default::default(),
                    consumed: (0..count).map(|_| AtomicU64::new(0)).collect(),
                })
                .collect(),
            subscribers: Subscribers::default(),
        }
    }

    pub(super) fn shards(&self) -> usize {
        self.boards.len()
    }

    #[inline]
    pub(super) fn wants(&self, target: usize, buckets: &Buckets) -> bool {
        self.boards.get(target).is_some_and(|board| {
            board
                .interest
                .iter()
                .zip(buckets.0)
                .any(|(word, wanted)| wanted != 0 && word.load(Ordering::Acquire) & wanted != 0)
        })
    }

    pub(super) fn show(&self, shard: usize, bucket: usize, wanted: bool) {
        let (word, bit) = Buckets::word_and_bit(bucket);
        if let Some(cell) = self
            .boards
            .get(shard)
            .and_then(|board| board.interest.get(word))
        {
            let old = cell.load(Ordering::Relaxed);
            let new = if wanted { old | bit } else { old & !bit };
            cell.store(new, Ordering::Release);
        }
    }

    #[inline]
    pub fn subscribers(&self) -> u32 {
        self.subscribers.0.load(Ordering::Relaxed)
    }

    pub(super) fn subscriber_joined(&self) {
        self.subscribers.0.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn subscriber_left(&self) {
        self.subscribers.0.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    pub(super) fn consumed(&self, receiver: usize, source: usize) -> u64 {
        self.cell(receiver, source)
            .map_or(0, |cell| cell.load(Ordering::Acquire))
    }

    #[inline]
    pub(super) fn consume(&self, receiver: usize, source: usize, len: usize) {
        if let Some(cell) = self.cell(receiver, source) {
            let total = cell.load(Ordering::Relaxed).wrapping_add(len as u64);
            cell.store(total, Ordering::Release);
        }
    }

    #[inline]
    fn cell(&self, receiver: usize, source: usize) -> Option<&AtomicU64> {
        self.boards.get(receiver)?.consumed.get(source)
    }
}
