/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::ptr;
use std::thread;
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};

use super::BusyPoll;

pub const READABLE: u32 = libc::EPOLLIN as u32;
pub const WRITABLE: u32 = libc::EPOLLOUT as u32;
pub const CLOSED: u32 = (libc::EPOLLHUP | libc::EPOLLERR) as u32;
const EPIOCSPARAMS: libc::Ioctl = 0x4008_8a01;
const NANOS_PER_MILLI: u128 = 1_000_000;

#[repr(C)]
struct EpollParams {
    busy_poll_usecs: u32,
    busy_poll_budget: u16,
    prefer_busy_poll: u8,
    pad: u8,
}

const _: () = assert!(size_of::<EpollParams>() == 8);

fn cvt(result: libc::c_int) -> io::Result<libc::c_int> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

fn cvt_len(result: isize) -> io::Result<usize> {
    usize::try_from(result).map_err(|_| io::Error::last_os_error())
}

pub fn epoll_create() -> io::Result<OwnedFd> {
    let raw = cvt(unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) })?;
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

pub fn ctl(epoll: &OwnedFd, op: libc::c_int, fd: RawFd, events: u32, token: u64) -> io::Result<()> {
    let mut event = libc::epoll_event { events, u64: token };
    cvt(unsafe { libc::epoll_ctl(epoll.as_raw_fd(), op, fd, &mut event) }).map(drop)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Timer {
    Precise,
    Millis,
}

pub fn wait(
    epoll: &OwnedFd,
    events: &mut [libc::epoll_event],
    timeout: Option<Duration>,
    timer: &mut Timer,
) -> io::Result<usize> {
    let max = libc::c_int::try_from(events.len()).unwrap_or(libc::c_int::MAX);
    if *timer == Timer::Precise
        && let Some(timeout) = timeout.filter(|timeout| !timeout.is_zero())
    {
        let spec = libc::timespec {
            tv_sec: libc::time_t::try_from(timeout.as_secs()).unwrap_or(libc::time_t::MAX),
            tv_nsec: libc::c_long::from(timeout.subsec_nanos()),
        };
        let ready = unsafe {
            libc::syscall(
                libc::SYS_epoll_pwait2,
                epoll.as_raw_fd(),
                events.as_mut_ptr(),
                max,
                &raw const spec,
                ptr::null::<libc::sigset_t>(),
                0usize,
            )
        };
        match cvt_len(isize::try_from(ready).unwrap_or(-1)) {
            Ok(ready) => return Ok(ready),
            Err(err) if err.kind() == io::ErrorKind::Interrupted => return Ok(0),
            Err(err) if matches!(err.raw_os_error(), Some(libc::ENOSYS | libc::EPERM)) => {
                fall_back(&err, timer);
            }
            Err(err) => return Err(err),
        }
    }
    let ready = unsafe {
        libc::epoll_wait(
            epoll.as_raw_fd(),
            events.as_mut_ptr(),
            max,
            timeout_millis(timeout),
        )
    };
    match cvt(ready) {
        Ok(ready) => Ok(usize::try_from(ready).unwrap_or_default()),
        Err(err) if err.kind() == io::ErrorKind::Interrupted => Ok(0),
        Err(err) => Err(err),
    }
}

#[cold]
#[inline(never)]
fn fall_back(err: &io::Error, timer: &mut Timer) {
    *timer = Timer::Millis;
    let thread = thread::current();
    eprintln!(
        "keep: {}: epoll_pwait2 refused ({err}); timed waits round up to whole milliseconds",
        thread.name().unwrap_or("shard")
    );
}

fn timeout_millis(timeout: Option<Duration>) -> libc::c_int {
    timeout.map_or(-1, |timeout| {
        libc::c_int::try_from(timeout.as_nanos().div_ceil(NANOS_PER_MILLI))
            .unwrap_or(libc::c_int::MAX)
    })
}

#[cold]
#[inline(never)]
pub fn keep_default_slack(err: &io::Error) {
    let thread = thread::current();
    eprintln!(
        "keep: {}: timer slack refused ({err}); timed waits keep the inherited slack",
        thread.name().unwrap_or("shard")
    );
}

pub fn set_timer_slack(slack: Duration) -> io::Result<()> {
    let nanos = libc::c_ulong::try_from(slack.as_nanos().max(1)).unwrap_or(libc::c_ulong::MAX);
    cvt(unsafe { libc::prctl(libc::PR_SET_TIMERSLACK, nanos) }).map(drop)
}

pub fn set_busy_poll(epoll: &OwnedFd, busy_poll: BusyPoll) -> io::Result<()> {
    let params = EpollParams {
        busy_poll_usecs: busy_poll.usecs,
        busy_poll_budget: busy_poll.budget,
        prefer_busy_poll: u8::from(busy_poll.prefer),
        pad: 0,
    };
    cvt(unsafe { libc::ioctl(epoll.as_raw_fd(), EPIOCSPARAMS, &raw const params) }).map(drop)
}

pub fn accept(listener: BorrowedFd<'_>) -> io::Result<OwnedFd> {
    let raw = cvt(unsafe {
        libc::accept4(
            listener.as_raw_fd(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
        )
    })?;
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

pub fn connect(addr: SocketAddr) -> io::Result<OwnedFd> {
    let socket = Socket::new(
        Domain::for_address(addr),
        Type::STREAM.nonblocking().cloexec(),
        Some(Protocol::TCP),
    )?;
    socket.set_tcp_nodelay(true)?;
    match socket.connect(&addr.into()) {
        Ok(()) => {}
        Err(err) if err.raw_os_error() == Some(libc::EINPROGRESS) => {}
        Err(err) => return Err(err),
    }
    Ok(socket.into())
}

pub fn shutdown_write(fd: &OwnedFd) -> io::Result<()> {
    cvt(unsafe { libc::shutdown(fd.as_raw_fd(), libc::SHUT_WR) }).map(drop)
}

pub fn recv(fd: &OwnedFd, buf: &mut [u8]) -> io::Result<usize> {
    cvt_len(unsafe { libc::recv(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len(), 0) })
}

pub fn send(fd: &OwnedFd, data: &[u8]) -> io::Result<usize> {
    cvt_len(unsafe {
        libc::send(
            fd.as_raw_fd(),
            data.as_ptr().cast(),
            data.len(),
            libc::MSG_NOSIGNAL,
        )
    })
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-rt/shard/sys.rs"]
mod tests;
