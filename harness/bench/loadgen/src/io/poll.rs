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

use crate::clock::Clock;
use crate::conn::{Conn, ConnError};
use crate::protocol::Protocol;
use crate::stats::Recorder;

const EVENTS: usize = 1024;

pub struct PollDriver {
    poll: Poll,
    events: Events,
    streams: Vec<TcpStream>,
    ready: Vec<(usize, bool)>,
}

impl PollDriver {
    pub fn new(streams: Vec<StdStream>) -> io::Result<Self> {
        let poll = Poll::new()?;
        let streams = streams
            .into_iter()
            .enumerate()
            .map(|(i, stream)| {
                stream.set_nonblocking(true)?;
                let mut stream = TcpStream::from_std(stream);
                poll.registry().register(
                    &mut stream,
                    Token(i),
                    Interest::READABLE | Interest::WRITABLE,
                )?;
                Ok(stream)
            })
            .collect::<io::Result<Vec<_>>>()?;
        Ok(PollDriver {
            poll,
            events: Events::with_capacity(EVENTS),
            streams,
            ready: Vec::with_capacity(EVENTS),
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
        match self.streams.get(index) {
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
        let (Some(conn), Some(stream)) = (conns.get_mut(token), self.streams.get(token)) else {
            return Ok(());
        };
        if readable {
            read_in(stream, conn, protocol, clock, rec)?;
        }
        if conn.has_unsent() {
            write_out(stream, conn, clock, rec)?;
        }
        Ok(())
    }
}

#[inline]
fn write_out(
    mut stream: &TcpStream,
    conn: &mut Conn,
    clock: &Clock,
    rec: &mut Recorder,
) -> Result<(), ConnError> {
    conn.stamp_new(clock.now_ns(), rec);
    while let Some(pending) = conn.unsent() {
        let written = stream.write(pending);
        rec.wrote(*written.as_ref().unwrap_or(&0));
        rec.syscall();
        match written {
            Ok(0) => return Err(ConnError::Closed),
            Ok(n) => conn.consume_sent(n),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

#[inline]
fn read_in(
    mut stream: &TcpStream,
    conn: &mut Conn,
    protocol: &Protocol,
    clock: &Clock,
    rec: &mut Recorder,
) -> Result<(), ConnError> {
    loop {
        let spare = conn.read_space();
        let spare_len = spare.len();
        let received = stream.read(spare);
        rec.read(*received.as_ref().unwrap_or(&0));
        rec.syscall();
        match received {
            Ok(0) => return Err(ConnError::Closed),
            Ok(n) => {
                conn.received(n, protocol, clock.now_ns(), rec)?;
                if n < spare_len {
                    return Ok(());
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
}
