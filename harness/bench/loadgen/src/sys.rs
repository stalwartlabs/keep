/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::io;

pub fn thread_cpu_ns() -> u64 {
    clock_ns(libc::CLOCK_THREAD_CPUTIME_ID)
}

pub fn process_cpu_ns() -> u64 {
    clock_ns(libc::CLOCK_PROCESS_CPUTIME_ID)
}

pub fn monotonic_ns() -> u64 {
    clock_ns(libc::CLOCK_MONOTONIC)
}

fn clock_ns(clock: libc::clockid_t) -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { libc::clock_gettime(clock, &mut ts) };
    if rc != 0 {
        return 0;
    }
    let secs = u64::try_from(ts.tv_sec).unwrap_or(0);
    let nanos = u64::try_from(ts.tv_nsec).unwrap_or(0);
    secs * 1_000_000_000 + nanos
}

#[cfg(target_os = "linux")]
pub fn pin_current_thread(cpu: usize) -> io::Result<()> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe { libc::CPU_SET(cpu, &mut set) };
    let rc = unsafe { libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
pub fn pin_current_thread(_cpu: usize) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "thread pinning is only supported on Linux",
    ))
}

#[cfg(target_os = "linux")]
pub fn tcp_retrans_segs() -> Option<u64> {
    let snmp = std::fs::read_to_string("/proc/net/snmp").ok()?;
    let mut tcp = snmp.lines().filter(|line| line.starts_with("Tcp:"));
    let header = tcp.next()?;
    let values = tcp.next()?;
    header
        .split_ascii_whitespace()
        .zip(values.split_ascii_whitespace())
        .find_map(|(name, value)| (name == "RetransSegs").then(|| value.parse().ok())?)
}

#[cfg(not(target_os = "linux"))]
pub fn tcp_retrans_segs() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
pub fn thread_id() -> i64 {
    i64::from(unsafe { libc::gettid() })
}

#[cfg(not(target_os = "linux"))]
pub fn thread_id() -> i64 {
    -1
}

pub fn hostname() -> String {
    let mut buf = [0u8; 256];
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return String::from("unknown");
    }
    let len = memchr::memchr(0, &buf).unwrap_or(buf.len());
    String::from_utf8_lossy(buf.get(..len).unwrap_or_default()).into_owned()
}

pub fn ensure_open_files(needed: u64) -> Result<(), String> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(format!("getrlimit: {}", io::Error::last_os_error()));
    }
    let needed = libc::rlim_t::try_from(needed).unwrap_or(libc::rlim_t::MAX);
    if limit.rlim_cur >= needed {
        return Ok(());
    }
    if needed > limit.rlim_max {
        return Err(format!(
            "{needed} open files needed but the hard limit is {}",
            limit.rlim_max
        ));
    }
    limit.rlim_cur = needed;
    match unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } {
        0 => Ok(()),
        _ => Err(format!(
            "cannot raise the open file limit to {needed}: {}",
            io::Error::last_os_error()
        )),
    }
}

pub fn available_cpus() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}
