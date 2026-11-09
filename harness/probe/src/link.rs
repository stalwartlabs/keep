/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    args::{Proto, Wait},
    clock::Clock,
    error::{Context, Result},
    sys,
};
use serde::Serialize;
use std::{
    hint::spin_loop,
    io::{self, Read, Write},
    mem,
    net::{Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpStream, UdpSocket},
    os::fd::{AsFd, BorrowedFd},
    time::Duration,
};

pub enum Recv {
    Message,
    TimedOut,
}

enum Io {
    Tcp(TcpStream),
    Udp(UdpSocket),
}

pub struct Link {
    io: Io,
    spin: bool,
}

#[derive(Serialize)]
pub struct RxInfo {
    pub local: Option<SocketAddr>,
    pub cpu: Option<usize>,
    pub incoming_cpu: Option<usize>,
    pub napi_id: Option<u32>,
}

impl Link {
    pub fn connect(
        proto: Proto,
        target: SocketAddr,
        wait: Wait,
        busy_poll_us: u32,
        udp_timeout: Duration,
    ) -> Result<Self> {
        let io = match proto {
            Proto::Tcp => {
                let stream = TcpStream::connect(target).context("TCP connect")?;
                stream.set_nodelay(true).context("TCP_NODELAY")?;
                Io::Tcp(stream)
            }
            Proto::Udp => {
                let any: SocketAddr = if target.is_ipv4() {
                    (Ipv4Addr::UNSPECIFIED, 0).into()
                } else {
                    (Ipv6Addr::UNSPECIFIED, 0).into()
                };
                let socket = UdpSocket::bind(any).context("UDP bind")?;
                socket.connect(target).context("UDP connect")?;
                if !wait.spins() {
                    socket
                        .set_read_timeout(Some(udp_timeout))
                        .context("SO_RCVTIMEO")?;
                }
                Io::Udp(socket)
            }
        };
        let link = Self {
            io,
            spin: wait.spins(),
        };
        match wait {
            Wait::Block => {}
            Wait::Spin => link.set_nonblocking()?,
            Wait::BusyPoll => sys::set_busy_poll(&link, busy_poll_us)?,
        }
        Ok(link)
    }

    fn set_nonblocking(&self) -> Result<()> {
        match &self.io {
            Io::Tcp(s) => s.set_nonblocking(true),
            Io::Udp(s) => s.set_nonblocking(true),
        }
        .context("O_NONBLOCK")
    }

    pub fn try_clone(&self) -> Result<Self> {
        let io = match &self.io {
            Io::Tcp(s) => Io::Tcp(s.try_clone().context("dup TCP socket")?),
            Io::Udp(s) => Io::Udp(s.try_clone().context("dup UDP socket")?),
        };
        Ok(Self {
            io,
            spin: self.spin,
        })
    }

    #[inline]
    pub fn send(&mut self, msg: &[u8]) -> io::Result<()> {
        match &mut self.io {
            Io::Tcp(stream) => write_all(stream, msg),
            Io::Udp(socket) => loop {
                match socket.send(msg) {
                    Ok(n) if n == msg.len() => return Ok(()),
                    Ok(_) => return Err(io::ErrorKind::WriteZero.into()),
                    Err(e) if retryable(&e) => spin_loop(),
                    Err(e) => return Err(e),
                }
            },
        }
    }

    #[inline]
    pub fn recv(&mut self, msg: &mut [u8], clock: &Clock, deadline: u64) -> io::Result<Recv> {
        match &mut self.io {
            Io::Tcp(stream) => read_exact(stream, msg).map(|()| Recv::Message),
            Io::Udp(socket) => loop {
                match socket.recv(msg) {
                    Ok(n) if n == msg.len() => return Ok(Recv::Message),
                    Ok(_) => {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "short datagram"));
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e)
                        if matches!(
                            e.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                        ) =>
                    {
                        if !self.spin || clock.now() >= deadline {
                            return Ok(Recv::TimedOut);
                        }
                        spin_loop();
                    }
                    Err(e) => return Err(e),
                }
            },
        }
    }

    pub fn shutdown(&self) {
        if let Io::Tcp(stream) = &self.io {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    pub fn rx_info(&self, cpu: Option<usize>) -> RxInfo {
        let local = match &self.io {
            Io::Tcp(s) => s.local_addr().ok(),
            Io::Udp(s) => s.local_addr().ok(),
        };
        RxInfo {
            local,
            cpu,
            incoming_cpu: sys::incoming_cpu(self),
            napi_id: sys::incoming_napi_id(self),
        }
    }
}

impl AsFd for Link {
    fn as_fd(&self) -> BorrowedFd<'_> {
        match &self.io {
            Io::Tcp(s) => s.as_fd(),
            Io::Udp(s) => s.as_fd(),
        }
    }
}

#[inline]
fn retryable(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    )
}

#[inline]
pub fn write_all(w: &mut impl Write, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        match w.write(buf) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => buf = buf.get(n..).unwrap_or_default(),
            Err(e) if retryable(&e) => spin_loop(),
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[inline]
pub fn read_exact(r: &mut impl Read, mut buf: &mut [u8]) -> io::Result<()> {
    while !buf.is_empty() {
        match r.read(buf) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => buf = mem::take(&mut buf).get_mut(n..).unwrap_or_default(),
            Err(e) if retryable(&e) => spin_loop(),
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
