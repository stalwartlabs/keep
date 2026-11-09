/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use keep_proto::{BusyScope, Decoded, Frame, HEADER_LEN, Response, ServerFrame};

use super::slots::Completion;
use super::{Conn, QUIET_ID, Role, Socket};
use crate::answer::Answer;
use crate::error::{Cause, Error};

const READ_BUFFER: usize = 16 << 10;

pub(super) struct ReadSide {
    buf: Vec<u8>,
    start: usize,
    end: usize,
    need: usize,
    wakers: Vec<Waker>,
}

impl ReadSide {
    fn new() -> Self {
        ReadSide {
            buf: Vec::new(),
            start: 0,
            end: 0,
            need: HEADER_LEN,
            wakers: Vec::new(),
        }
    }

    fn spare(&mut self) -> &mut [u8] {
        if self.start == self.end {
            self.start = 0;
            self.end = 0;
        }
        let capacity = self.need.max(READ_BUFFER);
        if self.end == self.buf.len() || self.start + capacity > self.buf.len() {
            self.buf.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
            if self.end == self.buf.len() || capacity > self.buf.len() {
                self.buf.resize(capacity.max(self.buf.len() * 2), 0);
            }
        }
        self.buf.get_mut(self.end..).unwrap_or_default()
    }
}

enum Dispatched {
    Continue,
    Failed,
}

impl Conn {
    fn dispatch(&self, side: &mut ReadSide, read: usize, epoch: u32) -> Dispatched {
        let ReadSide {
            buf,
            start,
            end,
            need,
            wakers,
        } = side;
        *end += read;
        let mut st = self.lock();
        if st.epoch != epoch {
            return Dispatched::Failed;
        }
        st.stats.reads += 1;
        st.stats.bytes_in += read as u64;
        let data = buf.get(*start..*end).unwrap_or_default();
        let mut frames = Frame::split_all(data);
        let mut failure = None;
        let mut answered = 0;
        let mut streams = false;
        {
            let state = &mut *st;
            let mut parsed = frames
                .by_ref()
                .map(|frame| frame.ok().and_then(|frame| ServerFrame::parse(&frame).ok()));
            let mut pending = None;
            while let Some(next) = pending.take().or_else(|| parsed.next()) {
                let Some(frame) = next else {
                    failure = Some(Error::Unknown(Cause::Protocol));
                    break;
                };
                match frame {
                    ServerFrame::Response(response) => {
                        state.stats.frames_in += 1;
                        match state.slots.complete(response.id, |expect| {
                            Answer::decode(expect, &response)
                                .inspect_err(|error| self.refused(error, &response))
                        }) {
                            Completion::Wake(waker) => {
                                answered += 1;
                                wakers.extend(waker);
                            }
                            Completion::Discarded => answered += 1,
                            Completion::Unknown if response.id == QUIET_ID => {
                                state.stats.quiet_errors += 1;
                            }
                            Completion::Unknown => state.stats.unknown_replies += 1,
                        }
                        if !state.routes.is_empty()
                            && let Some(waker) = state.routes.response(response.id, response.status)
                        {
                            streams = true;
                            wakers.extend(waker);
                        }
                    }
                    ServerFrame::TopologyChanged { epoch_low } => {
                        state.stats.events += 1;
                        if self.epochs.push(epoch_low) {
                            self.signals.nudge();
                        }
                    }
                    ServerFrame::Event(first) => {
                        let Some((shared, lane)) = state.routes.deliver(first.topic) else {
                            state.stats.events += 1;
                            state.stats.stray_events += 1;
                            continue;
                        };
                        let mut inbox = shared.lock();
                        let mut event = first;
                        loop {
                            state.stats.events += 1;
                            wakers.extend(inbox.event(
                                lane,
                                event.seq,
                                event.payload.as_bytes(),
                                shared,
                            ));
                            match parsed.next() {
                                Some(Some(ServerFrame::Event(next)))
                                    if next.topic == first.topic =>
                                {
                                    event = next;
                                }
                                other => {
                                    pending = other;
                                    break;
                                }
                            }
                        }
                    }
                    ServerFrame::Heartbeat(mark) => match state.routes.heartbeat(mark) {
                        Some(waker) => wakers.extend(waker),
                        None => state.stats.stray_events += 1,
                    },
                    ServerFrame::End(mark) => match state.routes.end(mark) {
                        Some(waker) => {
                            streams = true;
                            wakers.extend(waker);
                        }
                        None => state.stats.stray_events += 1,
                    },
                    ServerFrame::GoAway { .. } => {
                        state.stats.goaways += 1;
                        self.drain(state, wakers);
                    }
                }
            }
        }
        *start = *end - frames.remainder().len();
        *need = match Frame::decode(frames.remainder()) {
            Ok(Decoded::Incomplete { needed }) => needed,
            _ => HEADER_LEN,
        };
        self.publish_outstanding(&st);
        if streams {
            self.signals.streams.notify_one();
        }
        if let Some(error) = failure {
            self.fail(st, epoch, error);
            wakers.drain(..).for_each(Waker::wake);
            return Dispatched::Failed;
        }
        self.answered(&mut st, answered);
        if self.held_flush(&st) {
            st.writer = Role::Caller;
            self.flush(st);
        } else {
            drop(st);
        }
        wakers.drain(..).for_each(Waker::wake);
        Dispatched::Continue
    }

    fn fail_read(&self, epoch: u32, error: io::Result<usize>) {
        let cause = match error {
            Ok(_) => Cause::Closed,
            Err(err) => Cause::Io(err.kind()),
        };
        self.fail(self.lock(), epoch, Error::Unknown(cause));
    }

    #[cold]
    fn refused(&self, error: &Error, response: &Response<'_>) {
        if let Error::Moved(epoch) = *error
            && self.epochs.stale(epoch)
        {
            self.signals.nudge();
        } else if response.busy_scope() == Some(BusyScope::Server) {
            self.gate.trip();
        }
    }
}

pub(crate) struct Reply<'a> {
    conn: &'a Conn,
    id: u32,
    done: bool,
}

impl<'a> Reply<'a> {
    pub fn new(conn: &'a Conn, id: u32) -> Self {
        Reply {
            conn,
            id,
            done: false,
        }
    }
}

impl Future for Reply<'_> {
    type Output = Result<Answer, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut st = this.conn.lock();
        if let Some(result) = st.slots.take(this.id) {
            this.done = true;
            return Poll::Ready(result);
        }
        st.slots.park(this.id, cx.waker());
        Poll::Pending
    }
}

impl Drop for Reply<'_> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let mut st = self.conn.lock();
        st.slots.abandon(self.id);
        self.conn.publish_outstanding(&st);
    }
}

pub(super) async fn reader(conn: Arc<Conn>, socket: Arc<Socket>, epoch: u32) {
    let mut side = ReadSide::new();
    loop {
        match socket.try_read(side.spare()) {
            Ok(read) if read > 0 => {
                if let Dispatched::Failed = conn.dispatch(&mut side, read, epoch) {
                    return;
                }
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                if let Err(err) = socket.readable().await {
                    conn.fail_read(epoch, Err(err));
                    return;
                }
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            other => {
                conn.fail_read(epoch, other);
                return;
            }
        }
    }
}
