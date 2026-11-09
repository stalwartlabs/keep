/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use serde::Serialize;
use std::mem::MaybeUninit;

#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct ProcessUsage {
    pub cpu_ns: u64,
    pub voluntary_switches: u64,
    pub involuntary_switches: u64,
}

impl ProcessUsage {
    pub fn now() -> Self {
        let mut usage = MaybeUninit::<libc::rusage>::zeroed();
        let usage = match unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } {
            0 => unsafe { usage.assume_init() },
            _ => return ProcessUsage::default(),
        };
        ProcessUsage {
            cpu_ns: timeval_ns(usage.ru_utime) + timeval_ns(usage.ru_stime),
            voluntary_switches: u64::try_from(usage.ru_nvcsw).unwrap_or_default(),
            involuntary_switches: u64::try_from(usage.ru_nivcsw).unwrap_or_default(),
        }
    }

    pub fn since(&self, earlier: &ProcessUsage) -> ProcessUsage {
        ProcessUsage {
            cpu_ns: self.cpu_ns.saturating_sub(earlier.cpu_ns),
            voluntary_switches: self
                .voluntary_switches
                .saturating_sub(earlier.voluntary_switches),
            involuntary_switches: self
                .involuntary_switches
                .saturating_sub(earlier.involuntary_switches),
        }
    }
}

pub fn thread_cpu_ns() -> u64 {
    let mut spec = MaybeUninit::<libc::timespec>::zeroed();
    match unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, spec.as_mut_ptr()) } {
        0 => {
            let spec = unsafe { spec.assume_init() };
            u64::try_from(spec.tv_sec).unwrap_or_default() * 1_000_000_000
                + u64::try_from(spec.tv_nsec).unwrap_or_default()
        }
        _ => 0,
    }
}

#[cfg(target_os = "linux")]
pub fn tighten_timer_slack() {
    unsafe {
        libc::prctl(libc::PR_SET_TIMERSLACK, 1 as libc::c_ulong);
    }
}

#[cfg(not(target_os = "linux"))]
pub fn tighten_timer_slack() {}

fn timeval_ns(value: libc::timeval) -> u64 {
    u64::try_from(value.tv_sec).unwrap_or_default() * 1_000_000_000
        + u64::try_from(value.tv_usec).unwrap_or_default() * 1_000
}
