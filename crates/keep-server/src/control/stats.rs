/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
#[repr(align(128))]
pub struct ControlStats {
    epoch: AtomicU64,
    awaiting: AtomicU64,
    fenced: AtomicU64,
    lapses: AtomicU64,
    refused: AtomicU64,
    unsent: AtomicU64,
    run_outs: AtomicU64,
    settled: AtomicU64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct ControlCounts {
    pub epoch: u64,
    pub awaiting: u64,
    pub fenced: bool,
    pub lapses: u64,
    pub refused: u64,
    pub unsent: u64,
    pub run_outs: u64,
    pub settled: bool,
}

impl ControlStats {
    pub fn counts(&self) -> ControlCounts {
        ControlCounts {
            epoch: self.epoch.load(Ordering::Relaxed),
            awaiting: self.awaiting.load(Ordering::Relaxed),
            fenced: self.fenced.load(Ordering::Relaxed) != 0,
            lapses: self.lapses.load(Ordering::Relaxed),
            refused: self.refused.load(Ordering::Relaxed),
            unsent: self.unsent.load(Ordering::Relaxed),
            run_outs: self.run_outs.load(Ordering::Relaxed),
            settled: self.settled.load(Ordering::Relaxed) != 0,
        }
    }
}

impl ControlStats {
    pub(super) fn store(&self, counts: &ControlCounts) {
        self.epoch.store(counts.epoch, Ordering::Relaxed);
        self.awaiting.store(counts.awaiting, Ordering::Relaxed);
        self.fenced
            .store(u64::from(counts.fenced), Ordering::Relaxed);
        self.lapses.store(counts.lapses, Ordering::Relaxed);
        self.refused.store(counts.refused, Ordering::Relaxed);
        self.unsent.store(counts.unsent, Ordering::Relaxed);
        self.run_outs.store(counts.run_outs, Ordering::Relaxed);
        self.settled
            .store(u64::from(counts.settled), Ordering::Relaxed);
    }
}
