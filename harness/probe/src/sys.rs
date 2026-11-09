/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    args::CpuList,
    error::{Context, Result},
};
use std::{io, os::fd::AsFd};

#[cfg(target_os = "linux")]
const MAX_PINNABLE_CPU: usize = 1024;

pub fn pin(cpus: Option<&CpuList>, slot: usize) -> Result<Option<usize>> {
    let Some(cpu) = cpus.and_then(|list| list.cpu_for(slot)) else {
        return Ok(None);
    };
    pin_to(cpu)?;
    Ok(Some(cpu))
}

pub fn pin_to(cpu: usize) -> Result<()> {
    pin_current_thread(cpu).context("pin thread to CPU")
}

#[cfg(target_os = "linux")]
fn pin_current_thread(cpu: usize) -> io::Result<()> {
    if cpu >= MAX_PINNABLE_CPU {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
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
fn pin_current_thread(_cpu: usize) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "CPU pinning needs Linux",
    ))
}

pub fn reduce_timer_slack() {
    #[cfg(target_os = "linux")]
    unsafe {
        libc::prctl(libc::PR_SET_TIMERSLACK, 1 as libc::c_ulong);
    }
}

pub fn set_busy_poll(socket: &impl AsFd, usecs: u32) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        socket2::SockRef::from(socket)
            .set_busy_poll(usecs)
            .context("SO_BUSY_POLL (needs root, or net.core.busy_read at least as high)")
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (socket.as_fd(), usecs);
        Err(crate::error::Error::Unsupported("SO_BUSY_POLL needs Linux"))
    }
}

pub fn incoming_cpu(socket: &impl AsFd) -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        socket2::SockRef::from(socket).cpu_affinity().ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = socket.as_fd();
        None
    }
}

pub fn incoming_napi_id(socket: &impl AsFd) -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let mut value: libc::c_uint = 0;
        let mut len = size_of::<libc::c_uint>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                socket.as_fd().as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_INCOMING_NAPI_ID,
                (&raw mut value).cast(),
                &mut len,
            )
        };
        (rc == 0).then_some(value)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = socket.as_fd();
        None
    }
}

#[derive(Clone, Copy)]
pub struct CpuTimes {
    total: u64,
    steal: u64,
}

impl CpuTimes {
    pub fn read() -> Option<Self> {
        let stat = std::fs::read_to_string("/proc/stat").ok()?;
        let line = stat.lines().next()?.strip_prefix("cpu ")?;
        let mut total = 0u64;
        let mut steal = 0u64;
        for (index, field) in line.split_ascii_whitespace().take(8).enumerate() {
            let ticks: u64 = field.parse().ok()?;
            total += ticks;
            if index == 7 {
                steal = ticks;
            }
        }
        Some(Self { total, steal })
    }

    pub fn steal_pct_since(&self, earlier: &Self) -> Option<f64> {
        let total = self.total.checked_sub(earlier.total).filter(|t| *t > 0)?;
        let steal = self.steal.saturating_sub(earlier.steal);
        Some(100.0 * steal as f64 / total as f64)
    }
}
