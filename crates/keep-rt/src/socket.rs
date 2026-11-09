/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::io;
use std::os::fd::{AsFd, BorrowedFd};
use std::time::Duration;

use socket2::SockRef;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Tuning {
    pub rto_min: Option<Duration>,
    pub user_timeout: Option<Duration>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Applied {
    pub rto_min: bool,
    pub user_timeout: bool,
}

impl Tuning {
    pub const KEEP: Tuning = Tuning {
        rto_min: Some(Duration::from_millis(5)),
        user_timeout: Some(Duration::from_secs(1)),
    };

    pub const NONE: Tuning = Tuning {
        rto_min: None,
        user_timeout: None,
    };

    pub fn apply(&self, socket: impl AsFd) -> io::Result<Applied> {
        self.apply_fd(socket.as_fd())
    }

    fn apply_fd(&self, socket: BorrowedFd<'_>) -> io::Result<Applied> {
        let socket = SockRef::from(&socket);
        socket.set_tcp_nodelay(true)?;
        Ok(Applied {
            rto_min: self
                .rto_min
                .is_some_and(|rto| set_rto_min(&socket, rto).is_ok()),
            user_timeout: self
                .user_timeout
                .is_some_and(|timeout| set_user_timeout(&socket, timeout).is_ok()),
        })
    }
}

pub fn grow_recv_buffer(socket: impl AsFd, bytes: usize) -> io::Result<usize> {
    grow_recv_buffer_fd(socket.as_fd(), bytes)
}

fn grow_recv_buffer_fd(socket: BorrowedFd<'_>, bytes: usize) -> io::Result<usize> {
    let socket = SockRef::from(&socket);
    let current = socket.recv_buffer_size()?;
    if current < bytes {
        socket.set_recv_buffer_size(bytes)?;
    }
    socket.recv_buffer_size()
}

#[cfg(target_os = "linux")]
const TCP_RTO_MIN_US: libc::c_int = 45;

#[cfg(target_os = "linux")]
fn set_rto_min(socket: &SockRef<'_>, rto: Duration) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let value = libc::c_int::try_from(rto.as_micros()).unwrap_or(libc::c_int::MAX);
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_TCP,
            TCP_RTO_MIN_US,
            (&raw const value).cast(),
            size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
pub fn rto_min(socket: impl AsFd) -> io::Result<Duration> {
    use std::os::fd::AsRawFd;

    let mut value: libc::c_int = 0;
    let mut len = size_of::<libc::c_int>() as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            socket.as_fd().as_raw_fd(),
            libc::IPPROTO_TCP,
            TCP_RTO_MIN_US,
            (&raw mut value).cast(),
            &mut len,
        )
    };
    if result == 0 {
        Ok(Duration::from_micros(
            u64::try_from(value).unwrap_or_default(),
        ))
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn set_user_timeout(socket: &SockRef<'_>, timeout: Duration) -> io::Result<()> {
    socket.set_tcp_user_timeout(Some(timeout))
}

#[cfg(not(target_os = "linux"))]
fn set_rto_min(_socket: &SockRef<'_>, _rto: Duration) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}

#[cfg(not(target_os = "linux"))]
fn set_user_timeout(_socket: &SockRef<'_>, _timeout: Duration) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}
