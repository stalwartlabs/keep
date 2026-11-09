/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt;
use std::time::Duration;

const QUARANTINE_MARGIN: Duration = Duration::from_secs(1);
const ENGINE_GRANULARITY: Duration = Duration::from_secs(1);
const CLOCK_RATE_DIVISOR: u32 = 1_200;
const NANOS_PER_MILLI: u128 = 1_000_000;
const LOCK_WINDOW_MARGIN: Duration = Duration::from_millis(250);
const DEFAULT_SELF_FENCE: Duration = Duration::from_secs(1);
const DEFAULT_MAX_STOP: Duration = Duration::from_secs(30);

const _: () = assert!(!LOCK_WINDOW_MARGIN.is_zero());
const _: () =
    assert!(LockLimits::DEFAULT.lock_window.as_nanos() > LockLimits::DEFAULT.self_fence.as_nanos());

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct LockLimits {
    max_lock_ttl_secs: u32,
    self_fence: Duration,
    max_stop: Duration,
    quarantine: Duration,
    lock_window: Duration,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LimitsError {
    ZeroTtl,
    ZeroSelfFence,
    SelfFenceTooLong,
    QuarantineTooShort {
        quarantine: Duration,
        minimum: Duration,
    },
}

impl fmt::Display for LimitsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LimitsError::ZeroTtl => f.write_str("max_lock_ttl must be at least 1 s"),
            LimitsError::ZeroSelfFence => f.write_str("the self-fence delay must not be zero"),
            LimitsError::SelfFenceTooLong => f.write_str("the self-fence delay is too long"),
            LimitsError::QuarantineTooShort {
                quarantine,
                minimum,
            } => write!(
                f,
                "quarantine {} ms is below max_lock_ttl + 1 s + 2 x self-fence + max_stop + max(1 s, (2 x max_lock_ttl + 2 s + 3 x self-fence + max_stop) / 1,199) = {} ms",
                quarantine.as_millis(),
                minimum.as_millis()
            ),
        }
    }
}

impl std::error::Error for LimitsError {}

impl LockLimits {
    pub const DEFAULT: LockLimits = LockLimits {
        max_lock_ttl_secs: 600,
        self_fence: DEFAULT_SELF_FENCE,
        max_stop: DEFAULT_MAX_STOP,
        quarantine: Duration::from_millis(634_031),
        lock_window: DEFAULT_SELF_FENCE.saturating_add(LOCK_WINDOW_MARGIN),
    };

    pub fn new(
        max_lock_ttl_secs: u32,
        self_fence: Duration,
        max_stop: Duration,
        quarantine: Option<Duration>,
    ) -> Result<LockLimits, LimitsError> {
        if max_lock_ttl_secs == 0 {
            return Err(LimitsError::ZeroTtl);
        }
        if self_fence.is_zero() {
            return Err(LimitsError::ZeroSelfFence);
        }
        let lock_window = self_fence
            .checked_add(LOCK_WINDOW_MARGIN)
            .ok_or(LimitsError::SelfFenceTooLong)?;
        let ttl = Duration::from_secs(u64::from(max_lock_ttl_secs));
        let owned = ttl
            .saturating_add(ENGINE_GRANULARITY)
            .saturating_add(self_fence);
        let undrifted = owned.saturating_add(self_fence).saturating_add(max_stop);
        let drift_millis = owned
            .saturating_add(undrifted)
            .as_nanos()
            .div_ceil(u128::from(CLOCK_RATE_DIVISOR - 1) * NANOS_PER_MILLI);
        let drift = Duration::from_millis(u64::try_from(drift_millis).unwrap_or(u64::MAX));
        let minimum = undrifted.saturating_add(QUARANTINE_MARGIN.max(drift));
        let quarantine = quarantine.unwrap_or(minimum);
        if quarantine < minimum {
            return Err(LimitsError::QuarantineTooShort {
                quarantine,
                minimum,
            });
        }
        Ok(LockLimits {
            max_lock_ttl_secs,
            self_fence,
            max_stop,
            quarantine,
            lock_window,
        })
    }

    #[inline]
    pub fn admits_ttl(&self, ttl_secs: u32) -> bool {
        ttl_secs <= self.max_lock_ttl_secs
    }

    pub fn max_lock_ttl_secs(&self) -> u32 {
        self.max_lock_ttl_secs
    }

    pub fn self_fence(&self) -> Duration {
        self.self_fence
    }

    pub fn max_stop(&self) -> Duration {
        self.max_stop
    }

    pub fn quarantine(&self) -> Duration {
        self.quarantine
    }

    pub fn max_hold(&self) -> Duration {
        self.quarantine.saturating_sub(self.max_stop) / 2
    }

    pub fn lock_break(&self) -> Duration {
        self.self_fence
    }

    pub fn lock_window(&self) -> Duration {
        self.lock_window
    }
}

impl Default for LockLimits {
    fn default() -> LockLimits {
        LockLimits::DEFAULT
    }
}
