/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

mod forward;
mod handoff;
mod kv;
mod route;
mod session;
mod upkeep;

use std::mem;
use std::sync::Arc;
use std::time::Duration;

use keep_cluster::Nanos as ClusterNanos;
use keep_engine::{KeepEngine, Time};
use keep_proto::{
    BusyScope, Command, CommandError, Decoded, Frame, Op, Request, Response, ServerFrame,
    ServerInfo, ShardInfo, Slot, Status,
};
use keep_rt::clock::Nanos;
use keep_rt::{ConnId, Conns, Ctx, Flow, Flush, Io, Service};

pub use forward::{Batch, Entries, Msg, Replies};
pub use handoff::{EXPORT_CHUNK, ExportChunk, HandoffMsg, SLOT_TAG};
pub use kv::{Handled, Limits, execute, key_of};
pub use session::Session;

use crate::clock::EngineClock;
use crate::cluster::{Published, cluster_time};
use crate::config::Settings;
use crate::metrics::{ShardCounts, ShardStats};
use crate::stream::{self, Hub, Stream, Topics};
use forward::Lanes;
use handoff::ShardHandoff;
use kv::Reply;
use route::{Admit, Placement, Router, moved};
use session::{Fatal, Greeting, Stage, go_away};
use upkeep::Upkeep;

pub const IN_FLIGHT: u32 = 1 << 16;

pub const OUTPUT_CAP: usize = 16 << 20;

pub const REPLY_CAP: usize = 2 * OUTPUT_CAP;

const HEARTBEAT_MS: u32 = 100;

const TICK: Duration = Duration::from_secs(1);
const SCAN_EVERY: Duration = Duration::from_secs(5);
const IDLE: Duration = Duration::from_secs(60);
const HANDSHAKE: Duration = Duration::from_secs(10);
const GRACE: Duration = Duration::from_secs(1);
pub const BARRIER: Duration = Duration::from_secs(1);
const DRAIN_POLL: Duration = Duration::from_millis(10);
const RETRY: Duration = Duration::from_millis(1);
const PARTIAL_KEEP: usize = 64 << 10;

struct Client<'a> {
    id: ConnId,
    stage: &'a mut Stage,
    forwarded: &'a mut u32,
    topics: &'a mut Topics,
    out: &'a mut Vec<u8>,
    start: usize,
    unsent: usize,
    now: Time,
    instant: ClusterNanos,
    serving: bool,
    counts: ShardCounts,
}

impl Client<'_> {
    #[inline]
    fn pending(&self) -> usize {
        self.unsent + self.out.len().saturating_sub(self.start)
    }

    #[cold]
    fn busy(&mut self, id: u32, scope: BusyScope) {
        self.counts.busy += 1;
        Response::encode_busy(self.out, id, scope);
    }
}

struct Core {
    engine: KeepEngine,
    router: Router,
    placement: Placement,
    clock: EngineClock,
    limits: Limits,
    greeting: Greeting,
    lanes: Lanes,
    upkeep: Upkeep,
    handoff: ShardHandoff,
    stream: Stream,
    executed: Option<u64>,
    locked: bool,
    frozen: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Drain {
    Serving,
    Grace(Nanos),
    Barrier(Nanos),
}

pub struct ShardService {
    core: Core,
    stats: Arc<ShardStats>,
    drain: Drain,
    next_scan: Nanos,
    victims: Vec<ConnId>,
    control: usize,
    unacked: bool,
}

impl ShardService {
    pub fn over_tls(mut self) -> ShardService {
        self.core.greeting.over_tls();
        self
    }

    pub fn new(
        id: u16,
        settings: &Settings,
        seed: u64,
        stats: Arc<ShardStats>,
        published: Arc<Published>,
        hub: Arc<Hub>,
    ) -> ShardService {
        let mut engine = KeepEngine::new(seed);
        engine.set_cap(settings.cap);
        let info = ServerInfo {
            node: Some(settings.node),
            shard: ShardInfo::new(id, settings.shards.get()),
            in_flight: Some(IN_FLIGHT),
            heartbeat_ms: Some(HEARTBEAT_MS),
            extensions: 0,
            placement: Some(settings.secret.fingerprint()),
            challenge: None,
        };
        ShardService {
            core: Core {
                engine,
                router: Router::new(settings.secret.hasher(), settings.shards, id),
                placement: Placement::new(published),
                clock: EngineClock::starting_at(settings.start),
                limits: settings.limits,
                greeting: Greeting::new(&info, settings.secret),
                lanes: Lanes::new(usize::from(settings.shards.get()) + 1),
                upkeep: Upkeep::new(settings.start),
                handoff: ShardHandoff::new(usize::from(settings.shards.get())),
                stream: Stream::new(
                    id,
                    hub,
                    Duration::from_millis(u64::from(HEARTBEAT_MS)),
                    settings.stream_coalesce,
                    settings.stream_coalesce_fanout,
                ),
                executed: None,
                locked: false,
                frozen: false,
            },
            stats,
            drain: Drain::Serving,
            next_scan: settings.start.after(SCAN_EVERY),
            victims: Vec::new(),
            control: usize::from(settings.shards.get()),
            unacked: false,
        }
    }

    pub fn engine(&self) -> &KeepEngine {
        &self.core.engine
    }

    fn reap(&mut self, now: Nanos, conns: &mut Conns<'_, Session>) {
        let victims = &mut self.victims;
        conns.for_each(|io| {
            let limit = if io.state.stage.is_authenticated() {
                IDLE
            } else {
                HANDSHAKE
            };
            if io.state.seen.after(limit) <= now {
                victims.push(io.id);
            }
        });
        for id in victims.drain(..) {
            conns.close(id);
        }
    }

    fn drain(&mut self, cx: &mut Ctx<'_, Msg>, conns: &mut Conns<'_, Session>) {
        if self.drain != Drain::Serving {
            return;
        }
        let end = cx.now().after(GRACE);
        self.drain = Drain::Grace(end);
        let victims = &mut self.victims;
        conns.for_each(|io| {
            go_away(io.out, Status::Busy, "shutdown");
            if io.state.stage.is_authenticated() {
                io.state.stage = Stage::Closing;
            } else {
                victims.push(io.id);
            }
        });
        for id in victims.drain(..) {
            conns.drain(id);
        }
        cx.wake_at(end.min(cx.now().after(DRAIN_POLL)));
    }

    fn end_grace(&mut self, now: Nanos, conns: &mut Conns<'_, Session>) {
        let victims = &mut self.victims;
        conns.for_each(|io| {
            if io.state.topics.is_empty() && io.state.forwarded == 0 {
                victims.push(io.id);
            } else {
                io.state.stage = Stage::Done;
            }
        });
        for id in victims.drain(..) {
            conns.drain(id);
        }
        self.core.stream.seal();
        self.drain = Drain::Barrier(now.after(BARRIER));
    }

    fn close_answered(&mut self, conns: &mut Conns<'_, Session>) {
        let ended = self.core.stream.ended();
        let victims = &mut self.victims;
        conns.for_each(|io| {
            if io.state.stage == Stage::Done
                && io.state.forwarded == 0
                && (io.state.topics.is_empty() || ended)
            {
                victims.push(io.id);
            }
        });
        for id in victims.drain(..) {
            conns.drain(id);
        }
    }

    #[cold]
    fn freeze(&mut self, cx: &mut Ctx<'_, Msg>) {
        self.core.frozen = true;
        self.ack_freeze(cx);
    }

    #[cold]
    #[inline(never)]
    fn ack_freeze(&mut self, cx: &mut Ctx<'_, Msg>) {
        self.unacked = cx.mesh().send(self.control, Msg::Frozen).is_err();
        if self.unacked {
            let retry = cx.now().after(RETRY);
            cx.wake_at(retry);
        }
    }

    fn close_all(&mut self, conns: &mut Conns<'_, Session>) {
        let victims = &mut self.victims;
        conns.for_each(|io| victims.push(io.id));
        for id in victims.drain(..) {
            conns.drain(id);
        }
    }
}

impl Core {
    #[inline]
    fn receive(
        &mut self,
        cx: &mut Ctx<'_, Msg>,
        client: &mut Client<'_>,
        input: &mut Vec<u8>,
        mut data: &[u8],
    ) -> Result<(), Fatal> {
        if !input.is_empty() {
            while let Ok(Decoded::Incomplete { needed }) = Frame::decode(input) {
                if needed > client.stage.frame_limit() {
                    return Err(too_long(client.out));
                }
                if data.is_empty() {
                    return Ok(());
                }
                let want = needed.saturating_sub(input.len());
                let (head, rest) = data.split_at(want.min(data.len()));
                if head.len() == want {
                    input.reserve_exact(want);
                }
                input.extend_from_slice(head);
                data = rest;
            }
            let used = self.serve(cx, client, input)?;
            input.drain(..used.min(input.len()));
            if input.is_empty() && input.capacity() > PARTIAL_KEEP {
                *input = Vec::new();
            }
        }
        let used = self.serve(cx, client, data)?;
        input.extend_from_slice(data.get(used..).unwrap_or_default());
        Ok(())
    }

    #[inline]
    fn serve(
        &mut self,
        cx: &mut Ctx<'_, Msg>,
        client: &mut Client<'_>,
        buf: &[u8],
    ) -> Result<usize, Fatal> {
        let mut rest = buf;
        loop {
            match Frame::decode(rest) {
                Ok(Decoded::Complete { frame, len }) => {
                    let Some((raw, tail)) = rest.split_at_checked(len) else {
                        break;
                    };
                    self.frame(cx, client, &frame, raw)?;
                    rest = tail;
                }
                Ok(Decoded::Incomplete { needed }) => {
                    if needed > client.stage.frame_limit() {
                        return Err(too_long(client.out));
                    }
                    break;
                }
                Err(_) => {
                    return Err(go_away(client.out, Status::BadRequest, "value too large"));
                }
            }
        }
        Ok(buf.len() - rest.len())
    }

    #[inline]
    fn frame(
        &mut self,
        cx: &mut Ctx<'_, Msg>,
        client: &mut Client<'_>,
        frame: &Frame<'_>,
        raw: &[u8],
    ) -> Result<(), Fatal> {
        client.counts.requests += 1;
        match *client.stage {
            Stage::Ready => {}
            Stage::Closing if frame.code == Op::Publish as u8 => {}
            Stage::Closing => {
                client.busy(frame.id, BusyScope::Request);
                return Ok(());
            }
            Stage::Done => return Ok(()),
            Stage::Hello | Stage::Auth(_) => {
                return self.greeting.step(client.stage, frame, client.out);
            }
        }
        let request = match Request::parse(frame) {
            Ok(request) => request,
            Err(err) => return refuse(client.out, frame.id, err),
        };
        let Some(key) = key_of(&request.command) else {
            self.keyless(cx, client, &request);
            return Ok(());
        };
        if !client.serving {
            client.busy(request.id, BusyScope::Server);
            return Ok(());
        }
        if client.pending() > OUTPUT_CAP {
            client.busy(request.id, BusyScope::Request);
            return Ok(());
        }
        let hash = self.router.hash(key);
        let slot = hash.slot();
        let gate = match self.placement.admit(slot, &request.command, client.instant) {
            Admit::Serve(gate) if !self.frozen => gate,
            Admit::Fenced => {
                client.busy(request.id, BusyScope::Server);
                return Ok(());
            }
            Admit::Serve(_) | Admit::Busy => {
                client.busy(request.id, BusyScope::Request);
                return Ok(());
            }
            Admit::Moved { owner, epoch } => {
                moved(client.out, request.id, owner, epoch);
                return Ok(());
            }
        };
        let shard = self.router.shard_of(hash);
        if shard == self.router.me() {
            execute(
                &mut self.engine,
                &request,
                client.now,
                gate,
                self.limits,
                client.out,
            );
            self.executed.get_or_insert(self.placement.epoch());
            self.locked |= locks(&request.command);
        } else {
            self.forward(client, usize::from(shard), slot, raw, request.id);
        }
        Ok(())
    }

    #[inline]
    fn forward(&mut self, client: &mut Client<'_>, target: usize, slot: Slot, raw: &[u8], id: u32) {
        if *client.forwarded >= IN_FLIGHT || !self.lanes.admits(target, raw.len()) {
            client.busy(id, BusyScope::Request);
            return;
        }
        self.lanes.forward(target, client.id, slot, raw);
        *client.forwarded += 1;
        client.counts.forwarded += 1;
    }

    fn keyless(&mut self, cx: &mut Ctx<'_, Msg>, client: &mut Client<'_>, request: &Request<'_>) {
        let reply = Reply::to(request);
        match request.command {
            Command::Ping => reply.status(client.out, Status::Ok),
            Command::Topology => reply.value(client.out, self.placement.topology(), 0),
            Command::DeletePrefix { prefix } => {
                let status = self.delete_prefix(cx, prefix.as_bytes());
                reply.status(client.out, status);
            }
            Command::Publish { topic, payload } => {
                let status = self.stream.publish(topic, payload, self.limits.max_value);
                reply.status(client.out, status);
            }
            Command::Subscribe { topic } => {
                let status = self.stream.subscribe(cx, client.id, client.topics, topic);
                reply.status(client.out, status);
            }
            Command::Unsubscribe { topic } => {
                match self.stream.unsubscribe(client.id, client.topics, topic) {
                    Some(mark) => {
                        reply.status(client.out, Status::Ok);
                        ServerFrame::End(mark).encode(client.out);
                    }
                    None => reply.status(client.out, Status::NotFound),
                }
            }
            Command::Hello { .. }
            | Command::Auth { .. }
            | Command::Leave
            | Command::Get { .. }
            | Command::Exists { .. }
            | Command::Set { .. }
            | Command::Delete { .. }
            | Command::Incr { .. }
            | Command::TryLock { .. }
            | Command::Unlock { .. }
            | Command::Renew { .. } => reply.status(client.out, Status::BadRequest),
        }
    }

    #[cold]
    fn delete_prefix(&mut self, cx: &mut Ctx<'_, Msg>, prefix: &[u8]) -> Status {
        self.engine.delete_prefix(prefix);
        self.upkeep.purge_started();
        let mut delivered = true;
        for target in self.router.others() {
            delivered &= self.lanes.flush_requests(target, cx.mesh())
                && cx.mesh().send(target, Msg::Purge(prefix.into())).is_ok();
        }
        if delivered { Status::Ok } else { Status::Busy }
    }

    fn answer(&mut self, cx: &Ctx<'_, Msg>, from: usize, batch: Batch) -> ShardCounts {
        self.placement.refresh();
        let instant = cluster_time(cx.now());
        let serving = self.placement.serving(instant);
        let mut run = Forwarded {
            engine: &mut self.engine,
            placement: &self.placement,
            now: self.clock.time(cx.now()),
            instant,
            limits: self.limits,
            frozen: self.frozen,
            executed: false,
            locked: false,
        };
        let mut counts = ShardCounts::default();
        if let Some(replies) = self.lanes.replies(from, batch.len()) {
            for (conn, slot, raw) in batch.entries() {
                let at = replies.open(conn, 0);
                if !serving {
                    refuse_forwarded(raw, replies.bytes_mut(), BusyScope::Server);
                    counts.busy += 1;
                } else if replies.len() <= REPLY_CAP {
                    run.request(raw, slot, replies.bytes_mut());
                    counts.remote += 1;
                } else {
                    refuse_forwarded(raw, replies.bytes_mut(), BusyScope::Request);
                    counts.busy += 1;
                }
                replies.close(at);
            }
        }
        if run.executed {
            self.executed.get_or_insert(self.placement.epoch());
        }
        self.locked |= run.locked;
        self.lanes.recycle(batch);
        counts
    }

    fn deliver(&mut self, from: usize, replies: Replies, conns: &mut Conns<'_, Session>) {
        let Replies {
            batch,
            settled,
            discarded,
        } = replies;
        self.lanes.settle(from, settled);
        for (conn, _, reply) in batch.entries() {
            if let Some(io) = conns.get(conn) {
                io.state.forwarded = io.state.forwarded.saturating_sub(1);
                if discarded {
                    conns.drain(conn);
                } else {
                    io.out.extend_from_slice(reply);
                }
            }
        }
        self.lanes.recycle(batch);
    }

    fn announce(&mut self, conns: &mut Conns<'_, Session>) {
        self.placement.refresh();
        if let Some(epoch) = self.placement.announce() {
            let frame = ServerFrame::TopologyChanged {
                epoch_low: epoch as u32,
            };
            conns.for_each(|io| {
                if io.state.stage.is_authenticated() {
                    frame.encode(io.out);
                }
            });
        }
    }
}

struct Forwarded<'a> {
    engine: &'a mut KeepEngine,
    placement: &'a Placement,
    now: Time,
    instant: ClusterNanos,
    limits: Limits,
    frozen: bool,
    executed: bool,
    locked: bool,
}

impl Forwarded<'_> {
    #[inline]
    fn request(&mut self, raw: &[u8], slot: u16, out: &mut Vec<u8>) {
        let Ok(Decoded::Complete { frame, .. }) = Frame::decode(raw) else {
            return;
        };
        let request = match Request::parse(&frame) {
            Ok(request) => request,
            Err(_) => return Response::encode_status(out, frame.id, Status::BadRequest),
        };
        let Some(slot) = Slot::new(slot) else {
            return Response::encode_status(out, request.id, Status::BadRequest);
        };
        match self.placement.admit(slot, &request.command, self.instant) {
            Admit::Serve(gate) if !self.frozen => {
                execute(self.engine, &request, self.now, gate, self.limits, out);
                self.executed = true;
                self.locked |= locks(&request.command);
            }
            Admit::Fenced => Response::encode_busy(out, request.id, BusyScope::Server),
            Admit::Serve(_) | Admit::Busy => {
                Response::encode_busy(out, request.id, BusyScope::Request);
            }
            Admit::Moved { owner, epoch } => moved(out, request.id, owner, epoch),
        }
    }
}

#[cold]
#[inline(never)]
fn refuse_forwarded(raw: &[u8], out: &mut Vec<u8>, scope: BusyScope) {
    if let Ok(Decoded::Complete { frame, .. }) = Frame::decode(raw) {
        Response::encode_busy(out, frame.id, scope);
    }
}

#[cold]
#[inline(never)]
fn refuse(out: &mut Vec<u8>, id: u32, err: CommandError) -> Result<(), Fatal> {
    if err.is_fatal() {
        return Err(go_away(out, Status::BadRequest, "protocol violation"));
    }
    Response::encode_status(out, id, Status::BadRequest);
    Ok(())
}

#[cold]
#[inline(never)]
fn too_long(out: &mut Vec<u8>) -> Fatal {
    go_away(
        out,
        Status::BadRequest,
        "frame too long before authentication",
    )
}

impl Service for ShardService {
    type Conn = Session;
    type Msg = Msg;

    fn accept(&mut self, cx: &mut Ctx<'_, Msg>, _id: ConnId) -> Option<Session> {
        (self.drain == Drain::Serving).then(|| Session::new(cx.now()))
    }

    #[inline]
    fn read(&mut self, cx: &mut Ctx<'_, Msg>, io: Io<'_, Session>, data: &[u8]) -> Flow {
        let unsent = io.pending();
        let Io { id, state, out, .. } = io;
        let start = out.len();
        let Session {
            input,
            stage,
            forwarded,
            seen,
            topics,
        } = state;
        let core = &mut self.core;
        core.placement.refresh();
        let instant = cluster_time(cx.now());
        let mut client = Client {
            id,
            stage,
            forwarded,
            topics,
            out,
            start,
            unsent,
            now: core.clock.time(cx.now()),
            instant,
            serving: core.placement.serving(instant),
            counts: ShardCounts::default(),
        };
        let result = core.receive(cx, &mut client, input, data);
        if client.stage.is_authenticated() {
            *seen = cx.now();
        }
        self.stats.add(&client.counts);
        match result {
            Ok(()) => Flow::Continue,
            Err(Fatal) => Flow::Drain,
        }
    }

    fn closed(&mut self, _id: ConnId, conn: Session) {
        if !conn.topics.is_empty() {
            self.core.stream.closed(conn.topics);
        }
    }

    fn message(
        &mut self,
        cx: &mut Ctx<'_, Msg>,
        from: usize,
        msg: Msg,
        conns: &mut Conns<'_, Session>,
    ) {
        match msg {
            Msg::Forward(batch) => {
                let counts = self.core.answer(cx, from, batch);
                self.stats.add(&counts);
            }
            Msg::Replies(replies) => self.core.deliver(from, replies, conns),
            Msg::Purge(prefix) => {
                self.core.engine.delete_prefix(&prefix);
                self.core.upkeep.purge_started();
            }
            Msg::Drain => self.drain(cx, conns),
            Msg::Freeze => self.freeze(cx),
            Msg::Frozen => {}
            Msg::View => self.core.announce(conns),
            Msg::Stream(msg) => self.core.stream.message(from, msg, conns, cx.now()),
            Msg::Handoff(msg) => self.core.handoff.message(msg),
        }
    }

    fn tick(&mut self, cx: &mut Ctx<'_, Msg>, conns: &mut Conns<'_, Session>) -> Option<Nanos> {
        let now = cx.now();
        let next = match self.drain {
            Drain::Serving => {
                if now >= self.next_scan {
                    self.reap(now, conns);
                    self.next_scan = now.after(SCAN_EVERY);
                }
                now.after(TICK)
            }
            Drain::Grace(end) if end > now && !conns.is_empty() => end.min(now.after(DRAIN_POLL)),
            Drain::Grace(_) => {
                self.end_grace(now, conns);
                now.after(DRAIN_POLL)
            }
            Drain::Barrier(limit) if limit <= now => {
                self.close_all(conns);
                cx.stop();
                return None;
            }
            Drain::Barrier(_)
                if conns.is_empty()
                    && self.core.stream.settled()
                    && self.core.stream.markers_in() =>
            {
                cx.stop();
                return None;
            }
            Drain::Barrier(limit) => {
                self.close_answered(conns);
                limit.min(now.after(DRAIN_POLL))
            }
        };
        let beat = self.core.stream.beat(conns, now);
        Some(beat.map_or(next, |beat| beat.min(next)))
    }

    fn end_turn(&mut self, cx: &mut Ctx<'_, Msg>, conns: &mut Conns<'_, Session>) -> Flush {
        if self.unacked {
            self.ack_freeze(cx);
        }
        let Core {
            engine,
            router,
            lanes,
            upkeep,
            handoff,
            clock,
            placement,
            stream,
            executed,
            locked,
            ..
        } = &mut self.core;
        let mut flush = Flush::Send;
        let locked = mem::take(locked);
        if let Some(epoch) = executed.take()
            && let now = cx.read_clock()
            && !placement.sendable(now, locked.then(|| cx.read_boot()), epoch)
        {
            lanes.discard_replies();
            flush = Flush::Discard;
        }
        if !lanes.flush(cx.mesh()) {
            let retry = cx.now().after(RETRY);
            cx.wake_at(retry);
        }
        stream.end_turn(cx, conns);
        let counts = stream.take_counts();
        if counts != stream::Counts::default() {
            self.stats.add(&ShardCounts {
                published: counts.published,
                delivered: counts.delivered,
                skipped: counts.skipped,
                slow: counts.slow,
                skip_runs: counts.skip_runs,
                windows: counts.windows,
                held_ns: counts.held_ns,
                ..ShardCounts::default()
            });
        }
        upkeep.run(engine, *clock, &self.stats, cx);
        if !handoff.is_idle() {
            handoff.step(engine, router, placement, clock.time(cx.now()), cx);
        }
        flush
    }
}

#[inline]
fn locks(command: &Command<'_>) -> bool {
    matches!(
        command,
        Command::TryLock { .. } | Command::Renew { .. } | Command::Unlock { .. }
    )
}
