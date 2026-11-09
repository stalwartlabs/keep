/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::sys;

#[derive(Clone, Copy, Debug)]
pub struct Clock {
    base: Instant,
    unix_base_ns: u64,
    mono_base_ns: u64,
}

impl Clock {
    pub fn new(base: Instant) -> Self {
        let lead = base.saturating_duration_since(Instant::now());
        let unix_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        Clock {
            base,
            unix_base_ns: duration_ns(unix_now + lead),
            mono_base_ns: sys::monotonic_ns() + duration_ns(lead),
        }
    }

    #[inline]
    pub fn now_ns(&self) -> u64 {
        duration_ns(self.base.elapsed())
    }

    pub fn base(&self) -> Instant {
        self.base
    }

    pub fn unix_base_ns(&self) -> u64 {
        self.unix_base_ns
    }

    pub fn mono_base_ns(&self) -> u64 {
        self.mono_base_ns
    }

    pub fn instant_at(&self, ns: u64) -> Instant {
        self.base + Duration::from_nanos(ns)
    }
}

#[inline]
pub fn duration_ns(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

pub fn unix_now_ns() -> u64 {
    duration_ns(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO),
    )
}
