/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

#[cfg(not(loom))]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[cfg(loom)]
use loom::sync::atomic::AtomicU64;

use crate::membership::Membership;
use crate::time::Nanos;

const AGE_MARGIN: Duration = Duration::from_secs(1);

#[derive(Debug, Default)]
pub struct Lease {
    until: AtomicU64,
    floor: AtomicU64,
}

impl Lease {
    pub fn new() -> Lease {
        Lease::default()
    }

    pub fn publish(&self, until: Nanos, floor: u64) {
        self.floor.store(floor, Ordering::Relaxed);
        self.until.store(until.0, Ordering::Release);
    }

    #[inline]
    pub fn valid(&self, now: Nanos, epoch: u64) -> bool {
        now.0 < self.until.load(Ordering::Acquire) && epoch >= self.floor.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub enum FenceMode {
    #[default]
    Serving,
    Fenced,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct FenceState {
    pub mode: FenceMode,
    pub lapses: u64,
}

impl FenceState {
    #[inline]
    pub fn fenced(self) -> bool {
        self.mode == FenceMode::Fenced
    }
}

#[derive(Clone, Debug)]
pub struct SelfFence {
    delay: Duration,
    quarantine: Duration,
    members: usize,
    until: Nanos,
    established: bool,
    state: FenceState,
    clock_losses: u64,
    recent: Vec<Nanos>,
}

impl SelfFence {
    pub fn new(membership: &Membership) -> SelfFence {
        let limits = membership.limits();
        let members = membership.members().len();
        let alone = members <= 1;
        SelfFence {
            delay: limits.self_fence(),
            quarantine: limits.quarantine(),
            members,
            until: if alone { Nanos::MAX } else { Nanos::ZERO },
            established: alone,
            state: FenceState {
                mode: if alone {
                    FenceMode::Serving
                } else {
                    FenceMode::Fenced
                },
                lapses: 0,
            },
            clock_losses: 0,
            recent: Vec::with_capacity(members),
        }
    }

    pub fn state(&self) -> FenceState {
        self.state
    }

    pub fn lease_end(&self) -> Nanos {
        if self.state.fenced() {
            Nanos::ZERO
        } else {
            self.until
        }
    }

    pub fn update(&mut self, now: Nanos, membership: &Membership) -> bool {
        if membership.clock_losses() != self.clock_losses {
            self.clock_losses = membership.clock_losses();
            self.until = self.until.min(now);
        }
        if membership.far(now) {
            self.until = self.until.min(now);
        }
        self.recent.clear();
        self.recent.extend(membership.confirmations().flatten());
        let age = membership.cluster_age(now);
        self.settle(now, membership.placing(), age)
    }

    fn settle(&mut self, now: Nanos, placing: usize, age: Duration) -> bool {
        let before = self.state;
        let needed = self.members / 2;
        if needed == 0 {
            return false;
        }
        let lapsed = self.established && now >= self.until;
        if lapsed && !self.state.fenced() {
            self.state.lapses += 1;
        }
        if lapsed {
            self.state.mode = FenceMode::Fenced;
        }
        if self.recent.len() >= needed {
            let (_, nth, _) = self
                .recent
                .select_nth_unstable_by(needed - 1, |a, b| b.cmp(a));
            self.until = self.until.max(nth.plus(self.delay));
        }
        if now < self.until {
            if !self.established && age + AGE_MARGIN >= self.quarantine {
                self.state.lapses += 1;
            }
            self.established = true;
            if self.state.fenced() && placing > self.members / 2 {
                self.state.mode = FenceMode::Serving;
            }
        } else {
            self.state.mode = FenceMode::Fenced;
        }
        self.state != before
    }
}
