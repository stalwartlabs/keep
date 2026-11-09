/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

#[cfg(target_os = "linux")]
mod bufring;
mod poll;
mod tls;
#[cfg(target_os = "linux")]
mod uring;

use std::io;
use std::net::TcpStream;
use std::time::Duration;

use rustls::ClientConnection;

use crate::clock::Clock;
use crate::conn::{Conn, ConnError};
use crate::protocol::Protocol;
use crate::stats::Recorder;
use crate::tls::Stream;

use self::poll::PollDriver;
use self::tls::TlsDriver;
#[cfg(target_os = "linux")]
use self::uring::UringDriver;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoKind {
    Poll,
    Uring,
    Tls,
}

impl IoKind {
    pub fn name(self) -> &'static str {
        match self {
            IoKind::Poll => "epoll",
            IoKind::Uring => "io_uring",
            IoKind::Tls => "epoll+tls",
        }
    }
}

pub enum Driver {
    Poll(PollDriver),
    Tls(TlsDriver),
    #[cfg(target_os = "linux")]
    Uring(Box<UringDriver>),
}

impl Driver {
    pub fn new(kind: IoKind, streams: Vec<Stream>) -> io::Result<Self> {
        match kind {
            IoKind::Poll => PollDriver::new(plain(streams)?).map(Driver::Poll),
            IoKind::Tls => TlsDriver::new(encrypted(streams)?).map(Driver::Tls),
            #[cfg(target_os = "linux")]
            IoKind::Uring => {
                UringDriver::new(plain(streams)?).map(|driver| Driver::Uring(Box::new(driver)))
            }
            #[cfg(not(target_os = "linux"))]
            IoKind::Uring => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring is only available on Linux",
            )),
        }
    }

    #[inline]
    pub fn flush(
        &mut self,
        index: usize,
        conn: &mut Conn,
        clock: &Clock,
        rec: &mut Recorder,
    ) -> Result<(), ConnError> {
        match self {
            Driver::Poll(driver) => driver.flush(index, conn, clock, rec),
            Driver::Tls(driver) => driver.flush(index, conn, clock, rec),
            #[cfg(target_os = "linux")]
            Driver::Uring(driver) => driver.flush(index, conn, clock, rec),
        }
    }

    #[inline]
    pub fn poll(&mut self, timeout: Duration, rec: &mut Recorder) -> Result<usize, ConnError> {
        match self {
            Driver::Poll(driver) => driver.poll(timeout, rec),
            Driver::Tls(driver) => driver.poll(timeout, rec),
            #[cfg(target_os = "linux")]
            Driver::Uring(driver) => driver.poll(timeout, rec),
        }
    }

    #[inline]
    pub fn handle(
        &mut self,
        ready: usize,
        conns: &mut [Conn],
        protocol: &Protocol,
        clock: &Clock,
        rec: &mut Recorder,
    ) -> Result<(), ConnError> {
        match self {
            Driver::Poll(driver) => driver.handle(ready, conns, protocol, clock, rec),
            Driver::Tls(driver) => driver.handle(ready, conns, protocol, clock, rec),
            #[cfg(target_os = "linux")]
            Driver::Uring(driver) => driver.handle(ready, conns, protocol, clock, rec),
        }
    }
}

fn plain(streams: Vec<Stream>) -> io::Result<Vec<TcpStream>> {
    streams
        .into_iter()
        .map(|stream| match stream {
            Stream::Plain(stream) => Ok(stream),
            Stream::Tls(_) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a TLS stream needs the TLS driver",
            )),
        })
        .collect()
}

fn encrypted(streams: Vec<Stream>) -> io::Result<Vec<(TcpStream, ClientConnection)>> {
    streams
        .into_iter()
        .map(|stream| match stream {
            Stream::Tls(stream) => Ok((stream.sock, stream.conn)),
            Stream::Plain(_) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the TLS driver needs TLS streams",
            )),
        })
        .collect()
}
