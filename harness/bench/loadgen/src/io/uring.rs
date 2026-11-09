/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::io;
use std::net::{Shutdown, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use std::time::Duration;

use io_uring::types::{CancelBuilder, Fixed, SubmitArgs, Timespec};
use io_uring::{IoUring, cqueue, opcode, squeue};

use super::bufring::BufRing;
use crate::clock::Clock;
use crate::conn::{Conn, ConnError};
use crate::protocol::Protocol;
use crate::stats::Recorder;

const BUF_GROUP: u16 = 0;
const RECV_BUF_SIZE: u32 = 4096;
const MIN_BUFS: usize = 64;
const MAX_BUFS: usize = 4096;
const MIN_SQ: usize = 256;
const MAX_SQ: usize = 32768;
const CQ_FACTOR: u32 = 4;
const CANCEL_TOKEN: u64 = u64::MAX;
const DRAIN_ROUNDS: u32 = 50;
const DRAIN_WAIT: Duration = Duration::from_millis(20);
const PUSH_ATTEMPTS: u32 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Recv,
    Send,
}

#[derive(Clone, Copy, Debug)]
struct Token(u64);

impl Token {
    fn new(kind: Kind, index: usize) -> Self {
        Token(
            ((index as u64) << 1)
                | match kind {
                    Kind::Recv => 0,
                    Kind::Send => 1,
                },
        )
    }

    fn decode(raw: u64) -> Option<(Kind, usize)> {
        if raw == CANCEL_TOKEN {
            return None;
        }
        let kind = if raw & 1 == 0 { Kind::Recv } else { Kind::Send };
        Some((kind, usize::try_from(raw >> 1).ok()?))
    }
}

#[derive(Clone, Copy, Debug)]
struct Completion {
    token: u64,
    result: i32,
    flags: u32,
}

struct Socket {
    stream: TcpStream,
    sending: Vec<u8>,
    sent: usize,
    send_busy: bool,
}

pub struct UringDriver {
    ring: IoUring,
    bufs: BufRing,
    sockets: Vec<Socket>,
    ready: Vec<Completion>,
    outstanding: usize,
}

impl UringDriver {
    pub fn new(streams: Vec<TcpStream>) -> io::Result<Self> {
        let count = streams.len();
        let sq = (count * 2 + 64).next_power_of_two().clamp(MIN_SQ, MAX_SQ);
        let sq = u32::try_from(sq).unwrap_or(MAX_SQ as u32);
        let ring = IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_taskrun_flag()
            .setup_submit_all()
            .setup_cqsize(sq * CQ_FACTOR)
            .build(sq)?;
        let fds: Vec<RawFd> = streams
            .iter()
            .map(|stream| {
                stream.set_nonblocking(false)?;
                Ok(stream.as_raw_fd())
            })
            .collect::<io::Result<_>>()?;
        ring.submitter().register_files(&fds)?;
        let buf_count = (count * 2).next_power_of_two().clamp(MIN_BUFS, MAX_BUFS);
        let bufs = BufRing::new(
            &ring.submitter(),
            BUF_GROUP,
            u16::try_from(buf_count).unwrap_or(1 << 12),
            RECV_BUF_SIZE,
        )?;
        let mut driver = UringDriver {
            ring,
            bufs,
            sockets: streams
                .into_iter()
                .map(|stream| Socket {
                    stream,
                    sending: Vec::new(),
                    sent: 0,
                    send_busy: false,
                })
                .collect(),
            ready: Vec::with_capacity(sq as usize * CQ_FACTOR as usize),
            outstanding: 0,
        };
        for index in 0..count {
            driver.arm_recv(index)?;
        }
        Ok(driver)
    }

    #[inline]
    pub fn flush(
        &mut self,
        index: usize,
        conn: &mut Conn,
        clock: &Clock,
        rec: &mut Recorder,
    ) -> Result<(), ConnError> {
        let Some(socket) = self.sockets.get_mut(index) else {
            conn.clear_dirty();
            return Ok(());
        };
        if socket.send_busy || !conn.has_unsent() {
            conn.clear_dirty();
            return Ok(());
        }
        conn.stamp_new(clock.now_ns(), rec);
        conn.swap_unsent(&mut socket.sending);
        socket.sent = 0;
        rec.wrote(socket.sending.len());
        self.push_send(index)
    }

    #[inline]
    pub fn poll(&mut self, timeout: Duration, rec: &mut Recorder) -> Result<usize, ConnError> {
        let wait = !timeout.is_zero();
        let enter = {
            let sq = self.ring.submission();
            wait || !sq.is_empty() || sq.taskrun() || sq.cq_overflow()
        };
        if enter {
            rec.syscall();
            let result = if wait {
                let timespec = Timespec::from(timeout);
                let args = SubmitArgs::new().timespec(&timespec);
                self.ring.submitter().submit_with_args(1, &args)
            } else {
                self.ring.submit()
            };
            if let Err(e) = result
                && !transient(&e)
            {
                return Err(e.into());
            }
        }
        self.ready.clear();
        self.ready
            .extend(self.ring.completion().map(|cqe| Completion {
                token: cqe.user_data(),
                result: cqe.result(),
                flags: cqe.flags(),
            }));
        rec.polled(!self.ready.is_empty());
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
        let Some(&done) = self.ready.get(ready) else {
            return Ok(());
        };
        let Some((kind, index)) = Token::decode(done.token) else {
            return Ok(());
        };
        let Some(conn) = conns.get_mut(index) else {
            return Ok(());
        };
        match kind {
            Kind::Recv => self.on_recv(index, conn, done, protocol, clock, rec)?,
            Kind::Send => self.on_send(index, done)?,
        }
        if conn.has_unsent() {
            self.flush(index, conn, clock, rec)?;
        }
        Ok(())
    }

    #[inline]
    fn on_recv(
        &mut self,
        index: usize,
        conn: &mut Conn,
        done: Completion,
        protocol: &Protocol,
        clock: &Clock,
        rec: &mut Recorder,
    ) -> Result<(), ConnError> {
        let more = cqueue::more(done.flags);
        if !more {
            self.outstanding = self.outstanding.saturating_sub(1);
        }
        match usize::try_from(done.result) {
            Ok(0) => return Err(ConnError::Closed),
            Ok(len) => {
                let Some(bid) = cqueue::buffer_select(done.flags) else {
                    return Err(io::Error::other("receive completion without a buffer").into());
                };
                rec.read(len);
                let processed =
                    conn.on_data(self.bufs.slice(bid, len), protocol, clock.now_ns(), rec);
                self.bufs.recycle(bid);
                processed?;
            }
            Err(_) if done.result == -libc::ENOBUFS => rec.rearmed(),
            Err(_) => return Err(io::Error::from_raw_os_error(-done.result).into()),
        }
        if !more {
            self.arm_recv(index)?;
        }
        Ok(())
    }

    #[inline]
    fn on_send(&mut self, index: usize, done: Completion) -> Result<(), ConnError> {
        self.outstanding = self.outstanding.saturating_sub(1);
        let Some(socket) = self.sockets.get_mut(index) else {
            return Ok(());
        };
        socket.send_busy = false;
        let sent = match usize::try_from(done.result) {
            Ok(0) => return Err(ConnError::Closed),
            Ok(sent) => sent,
            Err(_) => return Err(io::Error::from_raw_os_error(-done.result).into()),
        };
        socket.sent += sent;
        if socket.sent < socket.sending.len() {
            return self.push_send(index);
        }
        socket.sending.clear();
        socket.sent = 0;
        Ok(())
    }

    fn arm_recv(&mut self, index: usize) -> io::Result<()> {
        let entry = opcode::RecvMulti::new(fixed(index)?, self.bufs.group())
            .build()
            .user_data(Token::new(Kind::Recv, index).0);
        self.push(&entry)?;
        self.outstanding += 1;
        Ok(())
    }

    #[inline]
    fn push_send(&mut self, index: usize) -> Result<(), ConnError> {
        let Some(socket) = self.sockets.get_mut(index) else {
            return Ok(());
        };
        let rest = socket.sending.get(socket.sent..).unwrap_or_default();
        let len = u32::try_from(rest.len()).map_err(|_| ConnError::Backlog)?;
        let entry = opcode::Send::new(fixed(index)?, rest.as_ptr(), len)
            .build()
            .user_data(Token::new(Kind::Send, index).0);
        socket.send_busy = true;
        self.push(&entry)?;
        self.outstanding += 1;
        Ok(())
    }

    #[inline]
    fn push(&mut self, entry: &squeue::Entry) -> io::Result<()> {
        for _ in 0..PUSH_ATTEMPTS {
            if unsafe { self.ring.submission().push(entry) }.is_ok() {
                return Ok(());
            }
            if let Err(e) = self.ring.submit()
                && !transient(&e)
            {
                return Err(e);
            }
        }
        Err(io::Error::other("submission queue stays full"))
    }
}

impl Drop for UringDriver {
    fn drop(&mut self) {
        for socket in &self.sockets {
            let _ = socket.stream.shutdown(Shutdown::Both);
        }
        let cancel = opcode::AsyncCancel2::new(CancelBuilder::any())
            .build()
            .user_data(CANCEL_TOKEN);
        if self.push(&cancel).is_err() {
            return;
        }
        let timespec = Timespec::from(DRAIN_WAIT);
        for _ in 0..DRAIN_ROUNDS {
            if self.outstanding == 0 {
                return;
            }
            let args = SubmitArgs::new().timespec(&timespec);
            if let Err(e) = self.ring.submitter().submit_with_args(1, &args)
                && !transient(&e)
            {
                return;
            }
            for cqe in self.ring.completion() {
                let terminal = match Token::decode(cqe.user_data()) {
                    Some((Kind::Recv, _)) => !cqueue::more(cqe.flags()),
                    Some((Kind::Send, _)) => true,
                    None => false,
                };
                if terminal {
                    self.outstanding = self.outstanding.saturating_sub(1);
                }
            }
        }
    }
}

fn fixed(index: usize) -> io::Result<Fixed> {
    u32::try_from(index)
        .map(Fixed)
        .map_err(|_| io::Error::other("too many connections for one ring"))
}

fn transient(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::ETIME | libc::EINTR | libc::EBUSY | libc::EAGAIN)
    )
}
