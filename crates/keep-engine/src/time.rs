/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::num::NonZeroU32;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Time(NonZeroU32);

impl Time {
    pub const START: Time = Time(NonZeroU32::MIN);

    pub fn from_secs(secs: u32) -> Option<Time> {
        NonZeroU32::new(secs).map(Time)
    }

    #[inline]
    pub fn secs(self) -> u32 {
        self.0.get()
    }

    pub fn advance(self, secs: u32) -> Time {
        Time(self.0.saturating_add(secs))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Ttl(NonZeroU32);

impl Ttl {
    pub fn from_secs(secs: u32) -> Option<Ttl> {
        NonZeroU32::new(secs).map(Ttl)
    }

    #[inline]
    pub fn secs(self) -> u32 {
        self.0.get()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Deadline(u32);

impl Deadline {
    pub const NEVER: Deadline = Deadline(0);

    #[inline]
    pub fn after(now: Time, ttl: Option<Ttl>) -> Deadline {
        match ttl {
            Some(ttl) => Deadline(now.secs().saturating_add(ttl.secs()).saturating_add(1)),
            None => Deadline::NEVER,
        }
    }

    #[inline]
    pub fn from_raw(raw: u32) -> Deadline {
        Deadline(raw)
    }

    #[inline]
    pub fn raw(self) -> u32 {
        self.0
    }

    #[inline]
    pub fn is_never(self) -> bool {
        self.0 == 0
    }

    #[inline]
    pub fn is_expired(self, now: Time) -> bool {
        self.0 != 0 && self.0 <= now.secs()
    }

    pub fn remaining(self, now: Time) -> Option<Ttl> {
        Ttl::from_secs(self.0.saturating_sub(now.secs()))
    }

    pub fn reported(self, now: Time) -> Option<Ttl> {
        let left = self.remaining(now)?;
        Ttl::from_secs(left.secs() - 1).or(Some(left))
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-engine/time.rs"]
mod tests;
