/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const NANOS_PER_SEC: u64 = 1_000_000_000;
pub const NANOS_PER_MILLI: u64 = 1_000_000;

#[derive(Clone, Copy, Debug)]
pub struct Clock {
    origin: Instant,
    origin_unix_ns: u64,
}

impl Clock {
    pub fn start() -> Self {
        let origin = Instant::now();
        let origin_unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, duration_ns);
        Clock {
            origin,
            origin_unix_ns,
        }
    }

    #[inline]
    pub fn now(&self) -> u64 {
        self.origin_unix_ns + duration_ns(self.origin.elapsed())
    }

    pub fn instant_at(&self, unix_ns: u64) -> Instant {
        self.origin + Duration::from_nanos(unix_ns.saturating_sub(self.origin_unix_ns))
    }
}

#[inline]
pub fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Clone, Copy, Debug)]
pub struct Window {
    pub start: u64,
    pub end: u64,
}

impl Window {
    pub fn new(start: u64, length: Duration) -> Self {
        Window {
            start,
            end: start + duration_ns(length),
        }
    }

    #[inline]
    pub fn contains(&self, at: u64) -> bool {
        at >= self.start && at < self.end
    }

    pub fn length_ns(&self) -> u64 {
        self.end - self.start
    }
}
