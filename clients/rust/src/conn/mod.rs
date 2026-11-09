/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

mod gate;
mod open;
mod read;
mod routes;
mod session;
mod slots;
mod socket;
mod tls;
mod write;

use std::collections::VecDeque;
use std::mem;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::Waker;
use std::time::{Duration, Instant};

use keep_proto::{Command, Decoded, Frame, ReplyMode, Request, ShardInfo};
use tokio::runtime::Handle;
use tokio::sync::Notify;

use crate::answer::{Answer, Expect};
use crate::error::{Cause, Error};
use crate::stats::Stats;

pub(crate) use gate::{Gate, Gated, Pass, Waiting};
pub(crate) use open::{Handshake, Opener, Openers};
pub(crate) use read::Reply;
use routes::Routes;
pub(crate) use routes::{Settled, Want};
use slots::Slots;
pub(crate) use socket::Close;
use socket::{Sent, Socket};
#[cfg(keep_bench)]
pub(crate) use tls::Connector;
pub(crate) use tls::TlsSetup;

const DIRECT_FLUSH_ROUNDS: usize = 4;
const TRACKED_BATCHES: usize = 16;
const DRAIN_LIMIT: Duration = Duration::from_secs(3);
const PUBLISH_BACKLOG: usize = 16 << 20;
const SILENT_HEARTBEATS: u32 = 3;

pub(crate) const QUIET_ID: u32 = 0;

pub(crate) struct Signals {
    pub topology: Notify,
    pub streams: Notify,
    rounds: AtomicU64,
    views: AtomicU64,
    pub clock: Instant,
}

const NUDGED: u64 = 1;
const UNSETTLED: u64 = 2;
const ROUND_SHIFT: u32 = 2;

impl Signals {
    pub fn new() -> Signals {
        Signals {
            topology: Notify::new(),
            streams: Notify::new(),
            rounds: AtomicU64::new(0),
            views: AtomicU64::new(0),
            clock: Instant::now(),
        }
    }

    #[inline]
    pub fn now_ns(&self) -> u64 {
        u64::try_from(self.clock.elapsed().as_nanos()).unwrap_or(u64::MAX) | 1
    }

    pub fn nudge(&self) {
        self.rounds.fetch_or(NUDGED, Ordering::SeqCst);
        self.topology.notify_one();
    }

    pub fn unsettled(&self) {
        self.rounds.fetch_or(UNSETTLED, Ordering::SeqCst);
    }

    pub fn wanted(&self) -> bool {
        self.rounds.load(Ordering::SeqCst) & (NUDGED | UNSETTLED) != 0
    }

    pub fn start_round(&self) -> u64 {
        let previous = self
            .rounds
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |state| {
                Some(((state >> ROUND_SHIFT) + 1) << ROUND_SHIFT)
            })
            .unwrap_or_else(|state| state);
        (previous >> ROUND_SHIFT) + 1
    }

    pub fn ticket(&self) -> u64 {
        let state = self.rounds.load(Ordering::SeqCst);
        (state >> ROUND_SHIFT) + (state & NUDGED)
    }

    pub fn started(&self) -> u64 {
        self.rounds.load(Ordering::Relaxed) >> ROUND_SHIFT
    }

    pub fn viewed(&self) {
        self.views.fetch_add(1, Ordering::SeqCst);
    }

    pub fn views(&self) -> u64 {
        self.views.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
pub(crate) struct Epochs {
    fetched: Mutex<Fetched>,
    pushed: AtomicU64,
}

#[derive(Default)]
struct Fetched {
    life: u64,
    epoch: Option<u64>,
    missed: bool,
}

impl Epochs {
    const PUSHED: u64 = 1 << u32::BITS;
    const LOW: u64 = Epochs::PUSHED - 1;

    fn lock(&self) -> MutexGuard<'_, Fetched> {
        self.fetched.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn life(&self) -> u64 {
        self.lock().life
    }

    pub fn fetched(&self, life: u64, epoch: u64) {
        let mut fetched = self.lock();
        if fetched.life == life {
            fetched.epoch = Some(fetched.epoch.map_or(epoch, |known| known.max(epoch)));
            fetched.missed = false;
        }
    }

    pub fn missed(&self) {
        self.lock().missed = true;
    }

    pub fn forget(&self) -> bool {
        let mut fetched = self.lock();
        fetched.life += 1;
        fetched.epoch = None;
        mem::take(&mut fetched.missed)
    }

    pub fn stale(&self, epoch: u64) -> bool {
        self.lock().epoch != Some(epoch)
    }

    pub fn push(&self, epoch_low: u32) -> bool {
        self.pushed
            .store(Epochs::PUSHED | u64::from(epoch_low), Ordering::SeqCst);
        self.push_pending()
    }

    pub fn push_pending(&self) -> bool {
        let pushed = self.pushed.load(Ordering::SeqCst);
        if pushed == 0 {
            return false;
        }
        if self
            .lock()
            .epoch
            .is_some_and(|epoch| epoch & Epochs::LOW == pushed & Epochs::LOW)
        {
            let _ = self
                .pushed
                .compare_exchange(pushed, 0, Ordering::SeqCst, Ordering::SeqCst);
            return false;
        }
        true
    }

    pub fn drop_push(&self) {
        self.pushed.store(0, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub(crate) enum Phase {
    Down = 0,
    Healthy = 1,
    Draining = 2,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Flush {
    Now,
    Later,
}

pub(crate) struct Conn {
    me: Weak<Conn>,
    server: SocketAddr,
    runtime: Handle,
    state: Mutex<State>,
    writer: Notify,
    down: Arc<Notify>,
    signals: Arc<Signals>,
    epochs: Arc<Epochs>,
    gate: Arc<Gate>,
    phase: AtomicU8,
    outstanding: AtomicU32,
    stalled_since: AtomicU64,
    last_write: AtomicU64,
}

struct State {
    socket: Option<Arc<Socket>>,
    epoch: u32,
    failure: Option<Error>,
    closed: bool,
    retired: bool,
    draining: Option<u64>,
    slots: Slots,
    wbuf: Vec<u8>,
    spare: Vec<u8>,
    sending: usize,
    backlog: Option<usize>,
    writer: Role,
    queued_frames: u64,
    sent_frames: u64,
    answered_frames: u64,
    inflight: VecDeque<u64>,
    routes: Routes,
    heartbeat: Option<Duration>,
    silence: Silence,
    stats: Stats,
}

#[derive(Clone, Copy, Default)]
struct Silence {
    reads: u64,
    since: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Role {
    Idle,
    Caller,
    Background,
}

impl State {
    fn accepting(&self) -> bool {
        self.failure.is_none() && self.draining.is_none()
    }
}

fn release_unsent(
    slots: &mut Slots,
    mut unsent: &[u8],
    wakers: &mut Vec<Waker>,
    mut quiet: impl FnMut(&[u8]),
) {
    while let Ok(Decoded::Complete { frame, len }) = Frame::decode(unsent)
        && let Some((bytes, tail)) = unsent.split_at_checked(len)
    {
        unsent = tail;
        if frame.id == QUIET_ID {
            quiet(bytes);
        } else if let slots::Completion::Wake(waker) =
            slots.complete(frame.id, |_| Err(Error::Unavailable))
        {
            wakers.extend(waker);
        }
    }
}

impl Conn {
    pub fn new(
        server: SocketAddr,
        down: Arc<Notify>,
        signals: Arc<Signals>,
        epochs: Arc<Epochs>,
        gate: Arc<Gate>,
    ) -> Arc<Conn> {
        let conn = Arc::new_cyclic(|me| Conn {
            me: me.clone(),
            server,
            runtime: Handle::current(),
            state: Mutex::new(State {
                socket: None,
                epoch: 0,
                failure: Some(Error::Unavailable),
                closed: false,
                retired: false,
                draining: None,
                slots: Slots::new(),
                wbuf: Vec::with_capacity(4096),
                spare: Vec::with_capacity(4096),
                sending: 0,
                backlog: None,
                writer: Role::Idle,
                queued_frames: 0,
                sent_frames: 0,
                answered_frames: 0,
                inflight: VecDeque::with_capacity(TRACKED_BATCHES),
                routes: Routes::default(),
                heartbeat: None,
                silence: Silence::default(),
                stats: Stats::default(),
            }),
            writer: Notify::new(),
            down,
            signals,
            epochs,
            gate,
            phase: AtomicU8::new(Phase::Down as u8),
            outstanding: AtomicU32::new(0),
            stalled_since: AtomicU64::new(0),
            last_write: AtomicU64::new(0),
        });
        conn.runtime.spawn(write::writer(Arc::clone(&conn)));
        conn
    }

    pub fn install(&self, handshake: &mut Option<Handshake>) -> bool {
        let mut st = self.lock();
        if st.closed || st.retired || st.socket.is_some() {
            return false;
        }
        let Some(handshake) = handshake.take() else {
            return false;
        };
        let socket = Arc::new(handshake.socket);
        let now = self.signals.now_ns();
        st.epoch = st.epoch.wrapping_add(1);
        st.socket = Some(Arc::clone(&socket));
        st.failure = None;
        st.heartbeat = handshake
            .info
            .heartbeat_ms
            .filter(|ms| *ms > 0)
            .map(|ms| Duration::from_millis(u64::from(ms)));
        st.silence = Silence {
            reads: st.stats.reads,
            since: now,
        };
        st.stats.connects += 1;
        self.last_write.store(now, Ordering::Relaxed);
        self.set_phase(Phase::Healthy);
        let epoch = st.epoch;
        drop(st);
        if let Some(conn) = self.me.upgrade() {
            self.runtime.spawn(read::reader(conn, socket, epoch));
        }
        let mut st = self.lock();
        if st.epoch == epoch && !st.routes.is_empty() {
            self.resubscribe(&mut st);
            self.kick(st);
        }
        true
    }

    pub async fn call(&self, command: &Command<'_>, expect: Expect) -> Result<Answer, Error> {
        let id = self.submit(command, expect, Flush::Now)?;
        Reply::new(self, id).await
    }

    #[inline]
    fn set_phase(&self, phase: Phase) {
        self.phase.store(phase as u8, Ordering::Release);
    }

    #[inline]
    pub fn server(&self) -> SocketAddr {
        self.server
    }

    #[inline]
    pub fn gate(&self) -> &Gate {
        &self.gate
    }

    pub fn views(&self) -> u64 {
        self.signals.views()
    }

    pub fn pass(&self, until: Instant, seen: u64) -> Waiting<impl Future<Output = Pass<'_>>> {
        self.gate.pass(until, seen, &self.signals)
    }

    #[inline]
    pub fn is_healthy(&self) -> bool {
        self.phase.load(Ordering::Relaxed) == Phase::Healthy as u8
    }

    #[inline]
    pub fn is_down(&self) -> bool {
        self.phase.load(Ordering::Relaxed) == Phase::Down as u8
    }

    #[inline]
    pub fn outstanding(&self) -> u32 {
        self.outstanding.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn stalled(&self, escape: Duration) -> bool {
        let since = self.stalled_since.load(Ordering::Relaxed);
        since != 0
            && self.signals.now_ns().saturating_sub(since)
                > u64::try_from(escape.as_nanos()).unwrap_or(u64::MAX)
    }

    pub fn stats(&self) -> Stats {
        self.lock().stats
    }

    pub fn submit(
        &self,
        command: &Command<'_>,
        expect: Expect,
        flush: Flush,
    ) -> Result<u32, Error> {
        let mut st = self.lock();
        if !st.accepting() {
            return Err(Error::Unavailable);
        }
        let id = st.slots.insert(expect).ok_or(Error::Unavailable)?;
        Request {
            id,
            reply: ReplyMode::Always,
            command: *command,
        }
        .encode(&mut st.wbuf);
        st.stats.frames_out += 1;
        st.queued_frames += 1;
        self.publish_outstanding(&st);
        if flush == Flush::Now {
            self.kick(st);
        }
        Ok(id)
    }

    pub fn submit_quiet(&self, command: &Command<'_>) -> Result<(), Error> {
        let mut st = self.lock();
        if !st.accepting() || st.wbuf.len() + st.sending >= PUBLISH_BACKLOG {
            return Err(Error::Unavailable);
        }
        Request {
            id: QUIET_ID,
            reply: ReplyMode::ErrorsOnly,
            command: *command,
        }
        .encode(&mut st.wbuf);
        st.stats.frames_out += 1;
        self.kick(st);
        Ok(())
    }

    pub fn uncork(&self) {
        let st = self.lock();
        self.kick(st);
    }

    fn kick(&self, mut st: MutexGuard<'_, State>) {
        if st.writer == Role::Idle && st.inflight.is_empty() && !st.wbuf.is_empty() {
            st.writer = Role::Caller;
            self.flush(st);
        }
    }

    fn flush<'a>(&'a self, mut st: MutexGuard<'a, State>) {
        for _ in 0..DIRECT_FLUSH_ROUNDS {
            let Some(socket) = st.socket.clone() else {
                st.writer = Role::Idle;
                return;
            };
            if st.wbuf.is_empty() && st.backlog.is_none() {
                st.writer = Role::Idle;
                return;
            }
            let epoch = st.epoch;
            let mut out = self.take_batch(&mut st);
            drop(st);
            let result = socket.send(&out);
            st = self.lock();
            if st.epoch != epoch {
                return;
            }
            match result {
                Ok(Sent::All) => {
                    st.sending = 0;
                    st.stats.writes += 1;
                    st.stats.bytes_out += out.len() as u64;
                    out.clear();
                    st.spare = out;
                }
                Ok(Sent::Blocked(written)) => {
                    st.sending = 0;
                    st.stats.writes += u64::from(written > 0);
                    st.stats.bytes_out += written as u64;
                    st.stats.write_stalls += 1;
                    out.drain(..written);
                    st.backlog = Some(out.len());
                    out.extend_from_slice(&st.wbuf);
                    st.wbuf.clear();
                    st.spare = mem::replace(&mut st.wbuf, out);
                    st.writer = Role::Background;
                    drop(st);
                    self.writer.notify_one();
                    return;
                }
                Err(err) => {
                    self.fail(st, epoch, Error::Unknown(Cause::Io(err.kind())));
                    return;
                }
            }
        }
        st.writer = Role::Background;
        drop(st);
        self.writer.notify_one();
    }

    fn take_batch(&self, st: &mut State) -> Vec<u8> {
        let frames = mem::take(&mut st.queued_frames);
        st.sent_frames += frames;
        if frames > 0 {
            if st.inflight.is_empty() {
                let now = self.signals.now_ns();
                self.stalled_since.store(now, Ordering::Relaxed);
                self.last_write.store(now, Ordering::Relaxed);
            }
            let end = st.sent_frames;
            let full = st.inflight.len() >= TRACKED_BATCHES;
            match st.inflight.back_mut() {
                Some(last) if full => *last = end,
                _ => st.inflight.push_back(end),
            }
        }
        let spare = mem::take(&mut st.spare);
        let batch = mem::replace(&mut st.wbuf, spare);
        st.sending = batch.len();
        st.backlog = None;
        batch
    }

    fn answered(&self, st: &mut State, frames: u64) {
        if frames == 0 {
            return;
        }
        st.answered_frames += frames;
        let mut done = false;
        while st
            .inflight
            .front()
            .is_some_and(|end| *end <= st.answered_frames)
        {
            st.inflight.pop_front();
            done = true;
        }
        if done {
            let since = match st.inflight.is_empty() {
                true => 0,
                false => self.signals.now_ns(),
            };
            self.stalled_since.store(since, Ordering::Relaxed);
        }
    }

    fn held_flush(&self, st: &State) -> bool {
        st.writer == Role::Idle && !st.wbuf.is_empty() && st.inflight.is_empty()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn publish_outstanding(&self, st: &State) {
        self.outstanding.store(
            u32::try_from(st.slots.outstanding()).unwrap_or(u32::MAX),
            Ordering::Relaxed,
        );
    }

    fn fail(&self, st: MutexGuard<'_, State>, epoch: u32, error: Error) {
        self.end(st, epoch, error, Close::Reset);
    }

    fn end(&self, mut st: MutexGuard<'_, State>, epoch: u32, error: Error, close: Close) {
        if st.epoch != epoch || st.failure.is_some() {
            return;
        }
        if let Some(socket) = st.socket.take() {
            socket.close(close);
        }
        st.failure = Some(error.clone());
        st.epoch = st.epoch.wrapping_add(1);
        st.writer = Role::Idle;
        let mut wakers = Vec::new();
        let wbuf = mem::take(&mut st.wbuf);
        let unsent = wbuf.get(st.backlog.unwrap_or(0)..).unwrap_or_default();
        release_unsent(&mut st.slots, unsent, &mut wakers, |_| {});
        st.wbuf = wbuf;
        st.wbuf.clear();
        st.backlog = None;
        st.sending = 0;
        st.draining = None;
        st.queued_frames = 0;
        st.sent_frames = 0;
        st.answered_frames = 0;
        st.inflight.clear();
        st.slots.fail_all(&error, &mut wakers);
        let streams = st.routes.fail(&mut wakers);
        self.publish_outstanding(&st);
        self.set_phase(Phase::Down);
        self.stalled_since.store(0, Ordering::Relaxed);
        drop(st);
        wakers.into_iter().for_each(Waker::wake);
        self.down.notify_one();
        if streams {
            self.signals.streams.notify_one();
        }
    }

    fn drain(&self, st: &mut State, wakers: &mut Vec<Waker>) {
        if self.stop_writing(st, wakers) {
            self.signals.nudge();
        }
    }

    fn stop_writing(&self, st: &mut State, wakers: &mut Vec<Waker>) -> bool {
        if st.draining.is_some() {
            return false;
        }
        st.draining = Some(self.signals.now_ns());
        self.set_phase(Phase::Draining);
        let mut kept = mem::take(&mut st.spare);
        kept.clear();
        let wbuf = mem::take(&mut st.wbuf);
        let (started, rest) = wbuf
            .split_at_checked(st.backlog.unwrap_or(0))
            .unwrap_or((&[], &wbuf));
        kept.extend_from_slice(started);
        release_unsent(&mut st.slots, rest, wakers, |quiet| {
            kept.extend_from_slice(quiet)
        });
        st.wbuf = kept;
        st.spare = wbuf;
        st.spare.clear();
        st.queued_frames = 0;
        self.publish_outstanding(st);
        true
    }

    pub fn retire(&self) {
        let mut st = self.lock();
        st.retired = true;
        if st.failure.is_some() {
            st.closed = true;
            return;
        }
        let mut wakers = Vec::new();
        self.stop_writing(&mut st, &mut wakers);
        drop(st);
        wakers.into_iter().for_each(Waker::wake);
    }

    pub fn close_retired(&self) {
        self.close(|st| match st.slots.outstanding() {
            0 => (Close::Graceful, Cause::Closed),
            _ => (Close::Reset, Cause::TimedOut),
        });
    }

    pub fn abort(&self, error: Error) {
        let st = self.lock();
        let epoch = st.epoch;
        self.fail(st, epoch, error);
    }

    pub fn watchdog(&self, limit: Duration) {
        match self.phase.load(Ordering::Relaxed) {
            phase if phase == Phase::Healthy as u8 => {
                if self.stalled(limit) {
                    self.abort(Error::Unknown(Cause::TimedOut));
                    return;
                }
                let mut st = self.lock();
                let Some(heartbeat) = st.heartbeat.filter(|_| !st.routes.is_empty()) else {
                    return;
                };
                let now = self.signals.now_ns();
                if st.stats.reads != st.silence.reads || !st.routes.has_live() {
                    st.silence = Silence {
                        reads: st.stats.reads,
                        since: now,
                    };
                    return;
                }
                let quiet = (heartbeat * SILENT_HEARTBEATS).max(limit);
                if now.saturating_sub(st.silence.since)
                    > u64::try_from(quiet.as_nanos()).unwrap_or(u64::MAX)
                {
                    let epoch = st.epoch;
                    self.fail(st, epoch, Error::Unknown(Cause::TimedOut));
                }
            }
            phase if phase == Phase::Draining as u8 => {
                let st = self.lock();
                let limit = u64::try_from(DRAIN_LIMIT.as_nanos()).unwrap_or(u64::MAX);
                if st
                    .draining
                    .is_some_and(|since| self.signals.now_ns().saturating_sub(since) > limit)
                {
                    let epoch = st.epoch;
                    self.fail(st, epoch, Error::Unknown(Cause::Closed));
                }
            }
            _ => {}
        }
    }

    pub fn keepalive(&self, idle: Duration) {
        let now = self.signals.now_ns();
        let idle = u64::try_from(idle.as_nanos()).unwrap_or(u64::MAX);
        if !self.is_healthy() || now.saturating_sub(self.last_write.load(Ordering::Relaxed)) < idle
        {
            return;
        }
        let mut st = self.lock();
        if !st.accepting() || !st.inflight.is_empty() || !st.wbuf.is_empty() {
            return;
        }
        let Some(id) = st.slots.insert(Expect::Status) else {
            return;
        };
        st.slots.abandon(id);
        Request {
            id,
            reply: ReplyMode::Always,
            command: Command::Ping,
        }
        .encode(&mut st.wbuf);
        st.stats.frames_out += 1;
        st.stats.pings += 1;
        st.queued_frames += 1;
        self.kick(st);
    }

    pub fn shutdown(&self, close: Close) {
        self.close(|_| (close, Cause::Closed));
    }

    fn close(&self, how: impl FnOnce(&State) -> (Close, Cause)) {
        let mut st = self.lock();
        st.closed = true;
        let epoch = st.epoch;
        if st.failure.is_none() {
            let (close, cause) = how(&st);
            self.end(st, epoch, Error::Unknown(cause), close);
        } else {
            drop(st);
        }
        self.writer.notify_one();
    }

    fn is_closed(&self) -> bool {
        self.lock().closed
    }
}

impl Handshake {
    pub fn shard(&self) -> Option<ShardInfo> {
        self.info.shard
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-client/conn/test_support.rs"]
mod test_support;

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-client/conn/mod.rs"]
mod tests;
