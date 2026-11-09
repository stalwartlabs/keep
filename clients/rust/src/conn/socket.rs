/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::io::{self, Read as _, Write as _};
use std::net::Shutdown;
use std::time::Duration;

use socket2::SockRef;
#[cfg(any(target_os = "linux", target_os = "android"))]
use socket2::{SockAddr, SockAddrStorage};
use tokio::io::Interest;
use tokio::net::TcpStream;

use super::session::Session;

pub(crate) struct Socket {
    stream: TcpStream,
    tls: Option<Box<Session>>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Sent {
    All,
    Blocked(usize),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Close {
    Graceful,
    Reset,
}

pub(crate) struct Raw<'a>(pub &'a TcpStream);

impl io::Read for Raw<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        read_draining(self.0, buf)
    }
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    target_os = "freebsd"
))]
fn read_draining(stream: &TcpStream, buf: &mut [u8]) -> io::Result<usize> {
    let len = buf.len();
    let mut drained = 0;
    let result = stream.try_io(Interest::READABLE, || {
        let read = (&*SockRef::from(stream)).read(buf)?;
        if 0 < read && read < len {
            drained = read;
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Ok(read)
    });
    match result {
        Err(err) if err.kind() == io::ErrorKind::WouldBlock && drained > 0 => Ok(drained),
        other => other,
    }
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    target_os = "freebsd"
)))]
fn read_draining(stream: &TcpStream, buf: &mut [u8]) -> io::Result<usize> {
    stream.try_read(buf)
}

impl io::Write for Raw<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.try_write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Socket {
    pub fn new(stream: TcpStream, tls: Option<Session>) -> Socket {
        Socket {
            stream,
            tls: tls.map(Box::new),
        }
    }

    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        match &self.tls {
            None => Raw(&self.stream).read(buf),
            Some(tls) => tls.read(&self.stream, buf),
        }
    }

    pub fn send(&self, out: &[u8]) -> io::Result<Sent> {
        match &self.tls {
            None => send_plain(&self.stream, out),
            Some(tls) => tls.send(&self.stream, out),
        }
    }

    pub async fn readable(&self) -> io::Result<()> {
        self.stream.readable().await
    }

    pub async fn writable(&self) -> io::Result<()> {
        self.stream.writable().await
    }

    pub fn close(&self, close: Close) {
        let socket = SockRef::from(&self.stream);
        if close == Close::Reset {
            let _ = socket.set_linger(Some(Duration::ZERO));
            let _ = disconnect(&socket);
        }
        let _ = socket.shutdown(Shutdown::Both);
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn disconnect(socket: &SockRef<'_>) -> io::Result<()> {
    let len = libc::socklen_t::try_from(size_of::<libc::sockaddr>()).unwrap_or_default();
    let unspecified = unsafe { SockAddr::new(SockAddrStorage::zeroed(), len) };
    socket.connect(&unspecified)
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn disconnect(_socket: &SockRef<'_>) -> io::Result<()> {
    Err(io::ErrorKind::Unsupported.into())
}

fn send_plain(stream: &TcpStream, out: &[u8]) -> io::Result<Sent> {
    let mut written = 0;
    while let Some(rest) = out.get(written..).filter(|rest| !rest.is_empty()) {
        match Raw(stream).write(rest) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => written += n,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                return Ok(Sent::Blocked(written));
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(Sent::All)
}

pub(crate) fn tune(
    stream: &TcpStream,
    rto_min: Option<Duration>,
    user_timeout: Option<Duration>,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let socket = SockRef::from(stream);
    if let Some(rto_min) = rto_min {
        let _ = set_rto_min(&socket, rto_min);
    }
    if let Some(user_timeout) = user_timeout {
        set_user_timeout(&socket, user_timeout)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn set_rto_min(socket: &SockRef<'_>, rto_min: Duration) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    const TCP_RTO_MIN_US: libc::c_int = 45;
    let value = libc::c_int::try_from(rto_min.as_micros()).unwrap_or(libc::c_int::MAX);
    let len = libc::socklen_t::try_from(size_of::<libc::c_int>()).unwrap_or_default();
    let rc = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_TCP,
            TCP_RTO_MIN_US,
            (&raw const value).cast(),
            len,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn set_rto_min(_socket: &SockRef<'_>, _rto_min: Duration) -> io::Result<()> {
    Err(io::ErrorKind::Unsupported.into())
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "fuchsia"))]
fn set_user_timeout(socket: &SockRef<'_>, timeout: Duration) -> io::Result<()> {
    socket.set_tcp_user_timeout(Some(timeout))
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "fuchsia")))]
fn set_user_timeout(_socket: &SockRef<'_>, _timeout: Duration) -> io::Result<()> {
    Ok(())
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-client/conn/socket.rs"]
pub(super) mod tests;
