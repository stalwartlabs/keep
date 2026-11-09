/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

#[cfg(target_os = "linux")]
use std::fs;
use std::io;
#[cfg(not(loom))]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[cfg(loom)]
use loom::sync::atomic::AtomicU64;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Nanos(u64);

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
compile_error!("the server clock needs CLOCK_MONOTONIC_RAW: Linux or macOS only");

const NS_PER_SEC: u64 = 1_000_000_000;
const NS_PER_MS: u64 = 1_000_000;

const SUSPECT_MIN: u64 = 100 * NS_PER_MS;

const SLEW_SHARE: u64 = 8;

const UNSET: u64 = u64::MAX;

fn read(clock: libc::clockid_t) -> io::Result<u64> {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(clock, &mut ts) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let secs = u64::try_from(ts.tv_sec).unwrap_or_default();
    let nanos = u64::try_from(ts.tv_nsec).unwrap_or_default();
    Ok(secs.saturating_mul(NS_PER_SEC).saturating_add(nanos))
}

impl Nanos {
    pub const ZERO: Nanos = Nanos(0);

    pub fn now() -> Nanos {
        Nanos(read(libc::CLOCK_MONOTONIC_RAW).unwrap_or_default())
    }

    pub fn check() -> io::Result<()> {
        read(libc::CLOCK_MONOTONIC_RAW)?;
        boot()?;
        suspended_total()?;
        Ok(())
    }

    pub const fn from_raw(nanos: u64) -> Nanos {
        Nanos(nanos)
    }

    pub const fn raw(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn after(self, span: Duration) -> Nanos {
        let span = u64::try_from(span.as_nanos()).unwrap_or(u64::MAX);
        Nanos(self.0.saturating_add(span))
    }

    pub fn until(self, later: Nanos) -> Duration {
        Duration::from_nanos(later.0.saturating_sub(self.0))
    }
}

#[cfg(target_os = "linux")]
pub fn boot() -> io::Result<u64> {
    read(libc::CLOCK_BOOTTIME)
}

#[cfg(target_vendor = "apple")]
pub fn boot() -> io::Result<u64> {
    read(libc::CLOCK_MONOTONIC_RAW)
}

#[cfg(target_os = "linux")]
pub fn suspended_total() -> io::Result<u64> {
    let boot = read(libc::CLOCK_BOOTTIME)?;
    Ok(boot.saturating_sub(read(libc::CLOCK_MONOTONIC)?))
}

#[cfg(target_vendor = "apple")]
pub fn suspended_total() -> io::Result<u64> {
    Ok(0)
}

#[cfg(target_os = "linux")]
pub fn kernel_correction_ppm() -> io::Result<i64> {
    const SCALED_PPM: i64 = 1 << 16;
    const TICK_USEC: i64 = 10_000;
    const PPM_PER_TICK_USEC: i64 = 100;
    let mut timex: libc::timex = unsafe { std::mem::zeroed() };
    if unsafe { libc::adjtimex(&mut timex) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let (freq, tick): (i64, i64) = (timex.freq, timex.tick);
    Ok(freq / SCALED_PPM + (tick - TICK_USEC) * PPM_PER_TICK_USEC)
}

#[cfg(target_vendor = "apple")]
pub fn kernel_correction_ppm() -> io::Result<i64> {
    Ok(0)
}

#[cfg(target_os = "linux")]
pub fn source() -> io::Result<String> {
    const CURRENT: &str = "/sys/devices/system/clocksource/clocksource0/current_clocksource";
    Ok(fs::read_to_string(CURRENT)?.trim().to_owned())
}

#[cfg(target_vendor = "apple")]
pub fn source() -> io::Result<String> {
    Ok("mach".to_owned())
}

#[derive(Debug, Default)]
pub struct SuspendWatch {
    seen: Option<u64>,
}

impl SuspendWatch {
    pub fn observe(&mut self, total: u64) -> Duration {
        let seen = self.seen.get_or_insert(total);
        let grew = total.saturating_sub(*seen);
        *seen = (*seen).max(total);
        Duration::from_nanos(grew)
    }
}

#[derive(Debug)]
pub struct SuspendMark {
    raw: AtomicU64,
    boot: AtomicU64,
}

impl Default for SuspendMark {
    fn default() -> SuspendMark {
        SuspendMark {
            raw: AtomicU64::new(UNSET),
            boot: AtomicU64::new(UNSET),
        }
    }
}

impl SuspendMark {
    pub fn publish(&self, raw: Nanos, boot: u64) {
        self.boot.store(boot, Ordering::Release);
        self.raw.store(raw.0, Ordering::Release);
    }

    #[inline]
    pub fn passed(&self, raw: Nanos, boot: u64) -> bool {
        let marked_raw = self.raw.load(Ordering::Acquire);
        let marked_boot = self.boot.load(Ordering::Acquire);
        marked_raw != UNSET && ran_ahead(marked_raw, marked_boot, raw.0, boot)
    }
}

fn ran_ahead(raw_then: u64, boot_then: u64, raw_now: u64, boot_now: u64) -> bool {
    let raw = raw_now.saturating_sub(raw_then);
    let boot = boot_now.saturating_sub(boot_then);
    boot > raw + raw / SLEW_SHARE + SUSPECT_MIN
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-rt/clock.rs"]
mod tests;
