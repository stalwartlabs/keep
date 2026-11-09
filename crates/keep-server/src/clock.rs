/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use keep_engine::Time;
use keep_rt::clock::Nanos;

const NS_PER_SEC: u64 = 1_000_000_000;

#[derive(Clone, Copy, Debug)]
pub struct EngineClock {
    start: Nanos,
}

impl EngineClock {
    pub fn starting_at(start: Nanos) -> EngineClock {
        EngineClock { start }
    }

    #[inline]
    pub fn time(self, now: Nanos) -> Time {
        let elapsed = now.raw().saturating_sub(self.start.raw()) / NS_PER_SEC;
        Time::START.advance(u32::try_from(elapsed).unwrap_or(u32::MAX))
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-server/clock.rs"]
mod tests;
