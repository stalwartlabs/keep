/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::io::{self, Read, Write};
use std::net::TcpStream as StdStream;
use std::time::Duration;

use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use rustls::ClientConnection;

use crate::clock::Clock;
use crate::conn::{Conn, ConnError};
use crate::protocol::Protocol;
use crate::stats::Recorder;

const EVENTS: usize = 1024;
const READ_CHUNK: usize = 16 << 10;

struct TlsStream {
    sock: TcpStream,
    tls: ClientConnection,
}

pub struct TlsDriver {
    poll: Poll,
    events: Events,
    streams: Vec<TlsStream>,
    ready: Vec<(usize, bool)>,
    chunk: Box<[u8]>,
}

impl TlsDriver {
    pub fn new(streams: Vec<(StdStream, ClientConnection)>) -> io::Result<Self> {
        let poll = Poll::new()?;
        let streams = streams
            .into_iter()
            .enumerate()
            .map(|(i, (stream, tls))| {
                stream.set_nonblocking(true)?;
                let mut sock = TcpStream::from_std(stream);
                poll.registry().register(
                    &mut sock,
                    Token(i),
                    Interest::READABLE | Interest::WRITABLE,
                )?;
                Ok(TlsStream { sock, tls })
            })
            .collect::<io::Result<Vec<_>>>()?;
        Ok(TlsDriver {
            poll,
            events: Events::with_capacity(EVENTS),
            streams,
            ready: Vec::with_capacity(EVENTS),
            chunk: vec![0; READ_CHUNK].into_boxed_slice(),
        })
    }

    #[inline]
    pub fn flush(
        &mut self,
        index: usize,
        conn: &mut Conn,
        clock: &Clock,
        rec: &mut Recorder,
    ) -> Result<(), ConnError> {
        match self.streams.get_mut(index) {
            Some(stream) => write_out(stream, conn, clock, rec),
            None => Ok(()),
        }
    }

    #[inline]
    pub fn poll(&mut self, timeout: Duration, rec: &mut Recorder) -> Result<usize, ConnError> {
        if let Err(e) = self.poll.poll(&mut self.events, Some(timeout))
            && e.kind() != io::ErrorKind::Interrupted
        {
            return Err(e.into());
        }
        let ready = !self.events.is_empty();
        rec.polled(ready);
        if ready {
            rec.syscall();
        }
        self.ready.clear();
        self.ready.extend(self.events.iter().map(|event| {
            (
                event.token().0,
                event.is_readable() || event.is_read_closed() || event.is_error(),
            )
        }));
        Ok(self.ready.len())
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
        let Some(&(token, readable)) = self.ready.get(ready) else {
            return Ok(());
        };
        let (Some(conn), Some(stream)) = (conns.get_mut(token), self.streams.get_mut(token)) else {
            return Ok(());
        };
        if readable {
            read_in(stream, &mut self.chunk, conn, protocol, clock, rec)?;
        }
        if conn.has_unsent() || stream.tls.wants_write() {
            write_out(stream, conn, clock, rec)?;
        }
        Ok(())
    }
}

#[inline]
fn write_out(
    stream: &mut TlsStream,
    conn: &mut Conn,
    clock: &Clock,
    rec: &mut Recorder,
) -> Result<(), ConnError> {
    conn.stamp_new(clock.now_ns(), rec);
    loop {
        while let Some(pending) = conn.unsent() {
            let accepted = stream.tls.writer().write(pending)?;
            if accepted == 0 {
                break;
            }
            conn.consume_sent(accepted);
        }
        if !stream.tls.wants_write() {
            return Ok(());
        }
        let written = stream.tls.write_tls(&mut stream.sock);
        rec.wrote(*written.as_ref().unwrap_or(&0));
        rec.syscall();
        match written {
            Ok(0) => return Err(ConnError::Closed),
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
}

#[inline]
fn read_in(
    stream: &mut TlsStream,
    chunk: &mut [u8],
    conn: &mut Conn,
    protocol: &Protocol,
    clock: &Clock,
    rec: &mut Recorder,
) -> Result<(), ConnError> {
    loop {
        let received = stream.sock.read(chunk);
        rec.read(*received.as_ref().unwrap_or(&0));
        rec.syscall();
        match received {
            Ok(0) => return Err(ConnError::Closed),
            Ok(n) => {
                let mut input = chunk.get(..n).unwrap_or_default();
                while !input.is_empty() {
                    if stream.tls.read_tls(&mut input)? == 0 {
                        return Err(ConnError::Closed);
                    }
                    stream.tls.process_new_packets().map_err(ConnError::Tls)?;
                    drain_plaintext(&mut stream.tls, conn, protocol, clock, rec)?;
                }
                if n < chunk.len() {
                    return Ok(());
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
}

#[inline]
fn drain_plaintext(
    tls: &mut ClientConnection,
    conn: &mut Conn,
    protocol: &Protocol,
    clock: &Clock,
    rec: &mut Recorder,
) -> Result<(), ConnError> {
    loop {
        match tls.reader().read(conn.read_space()) {
            Ok(0) => return Err(ConnError::Closed),
            Ok(n) => conn.received(n, protocol, clock.now_ns(), rec)?,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(e) => return Err(e.into()),
        }
    }
}
