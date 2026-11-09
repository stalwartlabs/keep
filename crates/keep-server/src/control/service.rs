/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt::Write;
use std::fs::{self, File};
use std::io;
use std::mem;
use std::net::{SocketAddr, UdpSocket};
use std::num::NonZeroU16;
use std::os::fd::{AsFd, BorrowedFd};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use keep_cluster::{
    Answer, FarPeer, HandoffAction, Heartbeat, MemberEvent, Nanos as ClusterNanos, PeerFrame,
    SlotSet, ViewError,
};
use keep_proto::{Decoded, Frame, KeyHasher, Liveness, NodeId, Nonce};
use keep_rt::clock::{Nanos, SuspendWatch};
use keep_rt::{ConnId, Conns, Ctx, Flow, Flush, Io, Service};

use crate::cluster::{cluster_time, rt_time};
use crate::control::Control;
use crate::control::peer::{Exports, Fault, Imports, PeerConn, Queued, Stage};
use crate::control::stats::{ControlCounts, ControlStats};
use crate::shard::{HandoffMsg, Msg};

const DATAGRAMS_PER_READY: usize = 1024;

const AWAITING_LOG: Duration = Duration::from_secs(1);

const REAP_EVERY: Duration = Duration::from_secs(1);

const HANDSHAKE_IDLE: Duration = Duration::from_secs(10);

const LINK_IDLE: Duration = Duration::from_secs(60);

const LEAVE_LIMIT: Duration = Duration::from_secs(11);

const FREEZE_LIMIT: Duration = Duration::from_secs(1);

const SUSPEND_MIN: Duration = Duration::from_millis(1);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Shutdown {
    Serving,
    Freezing {
        sent: u16,
        frozen: u16,
        until: Nanos,
    },
    Leaving {
        until: Nanos,
    },
    Draining,
}

enum Purge {
    Idle,
    Starting { slots: Arc<SlotSet>, sent: usize },
    Walking { left: usize },
}

pub trait HeartbeatPort {
    fn send_to(&mut self, bytes: &[u8], to: SocketAddr) -> io::Result<()>;
    fn recv(&mut self, into: &mut [u8]) -> io::Result<usize>;
}

impl HeartbeatPort for UdpSocket {
    #[inline]
    fn send_to(&mut self, bytes: &[u8], to: SocketAddr) -> io::Result<()> {
        UdpSocket::send_to(self, bytes, to).map(drop)
    }

    #[inline]
    fn recv(&mut self, into: &mut [u8]) -> io::Result<usize> {
        UdpSocket::recv(self, into)
    }
}

pub struct ControlService<H = UdpSocket> {
    control: Control,
    socket: H,
    shards: NonZeroU16,
    hasher: KeyHasher,
    datagram: Box<[u8]>,
    stats: Arc<ControlStats>,
    counts: ControlCounts,
    incarnation: u32,
    far: (usize, u64),
    flagged: Option<NodeId>,
    next_log: Nanos,
    nudge: bool,
    failure: Option<ViewError>,
    dials: Box<[Option<ConnId>]>,
    exports: Exports,
    imports: Imports,
    purge: Purge,
    actions: Vec<HandoffAction>,
    now: ClusterNanos,
    next_reap: Nanos,
    victims: Vec<ConnId>,
    shutdown: Shutdown,
    suspends: SuspendWatch,
    ready_file: Option<Box<Path>>,
}

impl ControlService<UdpSocket> {
    pub fn heartbeat_fd(&self) -> BorrowedFd<'_> {
        self.socket.as_fd()
    }
}

impl<H: HeartbeatPort> ControlService<H> {
    pub fn new(
        control: Control,
        socket: H,
        shards: NonZeroU16,
        hasher: KeyHasher,
        stats: Arc<ControlStats>,
    ) -> ControlService<H> {
        let members = control.members();
        let count = usize::from(shards.get());
        let incarnation = control.incarnation();
        ControlService {
            datagram: vec![0u8; Heartbeat::len(members) + 1].into_boxed_slice(),
            control,
            socket,
            shards,
            hasher,
            stats,
            counts: ControlCounts::default(),
            incarnation,
            far: (0, 0),
            flagged: None,
            next_log: Nanos::ZERO,
            nudge: false,
            failure: None,
            dials: vec![None; members].into_boxed_slice(),
            exports: Exports::new(count),
            imports: Imports::new(shards),
            purge: Purge::Idle,
            actions: Vec::new(),
            now: ClusterNanos::ZERO,
            next_reap: Nanos::ZERO,
            victims: Vec::new(),
            shutdown: Shutdown::Serving,
            suspends: SuspendWatch::default(),
            ready_file: None,
        }
    }

    pub fn set_ready_file(&mut self, path: Box<Path>) {
        mark_ready(&path, self.counts.settled);
        self.ready_file = Some(path);
    }

    pub fn heartbeats(&mut self) -> &mut H {
        &mut self.socket
    }

    pub fn control(&self) -> &Control {
        &self.control
    }

    pub fn control_mut(&mut self) -> &mut Control {
        &mut self.control
    }

    pub fn finish(self) -> io::Result<()> {
        match self.failure {
            None => Ok(()),
            Some(err) => Err(io::Error::other(err)),
        }
    }

    fn send_heartbeats(&mut self) {
        let ControlService {
            control,
            socket,
            counts,
            ..
        } = self;
        control.heartbeats(|to, bytes| {
            if socket.send_to(bytes, to).is_err() {
                counts.unsent += 1;
            }
        });
    }

    fn observe(&mut self, now: Nanos) {
        let fence = self.control.fence();
        let awaiting = self.control.awaiting().count() as u64;
        let fenced = fence.fenced();
        if fenced != self.counts.fenced {
            eprintln!(
                "keep: {} (lapses {})",
                if fenced { "fenced" } else { "serving" },
                fence.lapses
            );
        }
        if awaiting > 0 && now >= self.next_log {
            self.log_awaiting();
            self.next_log = now.after(AWAITING_LOG);
        } else if awaiting == 0 && self.counts.awaiting > 0 {
            eprintln!("keep: every live member echoed this server's restart; it places itself");
        }
        let far = self
            .control
            .far_peers()
            .fold((0, 0u64), |(count, sum), peer| {
                (
                    count + 1,
                    sum.wrapping_add(peer.node.0 ^ (u64::from(peer.members) << 48)),
                )
            });
        if far != self.far {
            self.far = far;
            self.log_far();
        }
        let flagged = self.control.flagged_by();
        if flagged.is_some() != self.flagged.is_some() {
            log_flagged(flagged);
        }
        self.flagged = flagged;
        let events = self.control.member_events();
        if !events.is_empty() {
            log_members(events, self.control.published().epoch());
        }
        let incarnation = self.control.incarnation();
        if incarnation != self.incarnation {
            self.incarnation = incarnation;
            eprintln!(
                "keep: a peer remembers a newer run of this server; continuing as incarnation {incarnation}"
            );
        }
        self.counts.epoch = self.control.published().epoch();
        self.counts.awaiting = awaiting;
        self.counts.fenced = fenced;
        self.counts.lapses = fence.lapses;
        self.counts.run_outs = self.control.run_outs();
        let settled = self.control.settled();
        if settled != self.counts.settled
            && let Some(path) = &self.ready_file
        {
            mark_ready(path, settled);
        }
        self.counts.settled = settled;
        self.stats.store(&self.counts);
    }

    #[cold]
    fn log_far(&self) {
        let ours = self.control.members();
        let mut far = self.control.far_peers().peekable();
        if far.peek().is_none() {
            eprintln!("keep: no peer runs a member list more than one member apart any more");
        }
        for FarPeer {
            node: NodeId(node),
            members,
        } in far
        {
            eprintln!(
                "keep: peer {node} runs a member list of {members} members against this server's {ours}, more than one member apart; fenced while it is heard"
            );
        }
    }

    #[cold]
    fn log_awaiting(&self) {
        let mut nodes = String::new();
        for NodeId(node) in self.control.awaiting() {
            if !nodes.is_empty() {
                nodes.push(' ');
            }
            nodes.push_str(&node.to_string());
        }
        eprintln!(
            "keep: waiting for members {nodes} to echo this server's restart; it does not place itself yet"
        );
    }

    fn reap(&mut self, now: Nanos, conns: &mut Conns<'_, PeerConn>) {
        let victims = &mut self.victims;
        conns.for_each(|io| {
            let limit = match io.state.stage {
                Stage::Dialing { .. } | Stage::Accepted { .. } => HANDSHAKE_IDLE,
                Stage::Puller { .. } | Stage::Server { .. } => LINK_IDLE,
            };
            if io.state.seen.after(limit) <= now {
                victims.push(io.id);
            }
        });
        for id in victims.drain(..) {
            conns.close(id);
        }
    }

    fn frame(
        &mut self,
        cx: &mut Ctx<'_, Msg>,
        id: ConnId,
        stage: &mut Stage,
        frame: &Frame<'_>,
        raw: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), Fault> {
        let peer = PeerFrame::parse(frame).map_err(|_| Fault)?;
        match stage {
            Stage::Accepted { handshake } => {
                if let Some(link) = self
                    .control
                    .step(handshake, &peer, out)
                    .map_err(|_| Fault)?
                {
                    *stage = Stage::Server { link, export: None };
                }
            }
            Stage::Dialing {
                member,
                handshake,
                queued,
            } => {
                let Some(link) = self
                    .control
                    .step(handshake, &peer, out)
                    .map_err(|_| Fault)?
                else {
                    return Ok(());
                };
                if link.index != *member {
                    return Err(Fault);
                }
                let queued = mem::take(queued);
                *stage = Stage::Puller { link };
                queued.write(out);
            }
            Stage::Puller { link } => {
                let from = link.index;
                match peer {
                    PeerFrame::Ready {
                        batch,
                        quarantine_ms,
                        data,
                        empty,
                    } => {
                        let answer = Answer {
                            data,
                            empty,
                            quarantine_ms,
                        };
                        self.control.ready(from, batch, &answer, self.now);
                    }
                    PeerFrame::Entry { batch, record } => {
                        let hash = self.hasher.hash(record.key);
                        if self.control.entry(from, batch, hash.slot())
                            && !self.imports.push(cx, hash, raw)
                        {
                            return Err(Fault);
                        }
                    }
                    PeerFrame::End { batch, count } => {
                        if !self.imports.end(cx, from, batch, count) {
                            return Err(Fault);
                        }
                    }
                    PeerFrame::Hello(_)
                    | PeerFrame::Proof(_)
                    | PeerFrame::Pull { .. }
                    | PeerFrame::Ack { .. } => return Err(Fault),
                }
            }
            Stage::Server { link, export } => match peer {
                PeerFrame::Pull {
                    batch,
                    slots,
                    since,
                } => {
                    if let Some(running) = export.take() {
                        self.exports.cancel(running);
                    }
                    let answer = self.control.answer(*link, batch, &slots, since, self.now);
                    let ready = PeerFrame::Ready {
                        batch,
                        quarantine_ms: answer.quarantine_ms,
                        data: answer.data,
                        empty: answer.empty,
                    };
                    ready.encode(out);
                    let PeerFrame::Ready { data, .. } = ready else {
                        return Ok(());
                    };
                    if data.is_empty() {
                        PeerFrame::End { batch, count: 0 }.encode(out);
                    } else {
                        *export = Some(self.exports.start(cx, id, batch, data).ok_or(Fault)?);
                    }
                }
                PeerFrame::Ack { batch } => self.control.acked(*link, batch),
                PeerFrame::Hello(_)
                | PeerFrame::Proof(_)
                | PeerFrame::Ready { .. }
                | PeerFrame::Entry { .. }
                | PeerFrame::End { .. } => return Err(Fault),
            },
        }
        Ok(())
    }

    fn handoff_actions(&mut self, cx: &mut Ctx<'_, Msg>, conns: &mut Conns<'_, PeerConn>) {
        let mut actions = mem::take(&mut self.actions);
        self.control.actions(&mut actions);
        for action in actions.drain(..) {
            match action {
                HandoffAction::Pull {
                    to,
                    batch,
                    slots,
                    since,
                } => self.send_to(
                    cx,
                    conns,
                    to,
                    Queued {
                        ack: None,
                        pull: Some((batch, slots, since)),
                    },
                ),
                HandoffAction::Ack { to, batch } => self.send_to(
                    cx,
                    conns,
                    to,
                    Queued {
                        ack: Some(batch),
                        pull: None,
                    },
                ),
                HandoffAction::Purge { slots } => {
                    self.purge = Purge::Starting {
                        slots: Arc::new(slots),
                        sent: 0,
                    };
                }
            }
        }
        self.actions = actions;
    }

    fn send_to(
        &mut self,
        cx: &mut Ctx<'_, Msg>,
        conns: &mut Conns<'_, PeerConn>,
        to: usize,
        frames: Queued,
    ) {
        if let Some(id) = self.dials.get(to).copied().flatten()
            && let Some(io) = conns.get(id)
        {
            match &mut io.state.stage {
                Stage::Dialing { queued, .. } => {
                    if let Some(ack) = frames.ack {
                        queued.ack = Some(ack);
                    }
                    if let Some(pull) = frames.pull {
                        queued.pull = Some(pull);
                    }
                }
                Stage::Puller { .. } => frames.write(io.out),
                Stage::Accepted { .. } | Stage::Server { .. } => {}
            }
            return;
        }
        let mut nonce = Nonce([0; Nonce::LEN]);
        let dialed = getrandom::fill(&mut nonce.0)
            .ok()
            .and_then(|()| self.control.initiate(to, nonce))
            .zip(self.control.peer_addr(to))
            .and_then(|((handshake, hello), addr)| {
                let stage = Stage::Dialing {
                    member: to,
                    handshake: Box::new(handshake),
                    queued: frames,
                };
                let id = conns.connect(addr, PeerConn::new(stage, cx.now())).ok()?;
                if let Some(io) = conns.get(id) {
                    PeerFrame::Hello(hello).encode(io.out);
                }
                Some(id)
            });
        match (dialed, self.dials.get_mut(to)) {
            (Some(id), Some(dial)) => *dial = Some(id),
            _ => self.control.disconnected(to, self.now),
        }
    }

    fn advance_purge(&mut self, cx: &mut Ctx<'_, Msg>) {
        let shards = usize::from(self.shards.get());
        let Purge::Starting { slots, sent } = &mut self.purge else {
            return;
        };
        if self.exports.intersects(slots) {
            return;
        }
        while *sent < shards {
            if !self.imports.flush(cx, *sent) {
                return;
            }
            let purge = HandoffMsg::Purge(slots.clone());
            if cx.mesh().send(*sent, Msg::Handoff(purge)).is_err() {
                return;
            }
            *sent += 1;
        }
        self.purge = Purge::Walking { left: shards };
    }

    fn drain(&mut self, cx: &mut Ctx<'_, Msg>) {
        match self.shutdown {
            Shutdown::Serving => {
                self.shutdown = Shutdown::Freezing {
                    sent: 0,
                    frozen: 0,
                    until: cx.now().after(FREEZE_LIMIT),
                };
                self.advance_freeze(cx);
            }
            Shutdown::Freezing { .. } | Shutdown::Leaving { .. } | Shutdown::Draining => {
                cx.stop();
            }
        }
    }

    fn advance_freeze(&mut self, cx: &mut Ctx<'_, Msg>) {
        let shards = self.shards.get();
        let Shutdown::Freezing {
            sent,
            frozen,
            until,
        } = &mut self.shutdown
        else {
            return;
        };
        let mesh = cx.mesh();
        while *sent < shards && mesh.send(usize::from(*sent), Msg::Freeze).is_ok() {
            *sent += 1;
        }
        let frozen = *frozen;
        if frozen < shards && cx.now() < *until {
            return;
        }
        if frozen < shards {
            log_freeze_limit(frozen, shards);
        }
        eprintln!("keep: leaving the cluster; handing over the slots");
        self.control.leave();
        self.shutdown = Shutdown::Leaving {
            until: cx.now().after(LEAVE_LIMIT),
        };
        cx.wake_at(cx.now());
    }

    fn advance_shutdown(&mut self, cx: &mut Ctx<'_, Msg>) {
        let Shutdown::Leaving { until } = self.shutdown else {
            return;
        };
        let handed = self.control.handed_over();
        if !handed && cx.now() < until {
            return;
        }
        let mesh = cx.mesh();
        let refused = (0..self.shards.get())
            .map(|shard| mesh.send(usize::from(shard), Msg::Drain).is_err())
            .fold(false, |refused, this| refused | this);
        if !refused {
            if handed {
                eprintln!("keep: slots handed over; draining the shards");
            } else {
                self.log_leave_limit(LEAVE_LIMIT.saturating_add(until.until(cx.now())));
            }
            self.shutdown = Shutdown::Draining;
        }
    }

    #[cold]
    fn log_leave_limit(&self, elapsed: Duration) {
        let current = self.control.published().current();
        let placement = current.view.placement();
        let mut receivers: Vec<(NodeId, usize)> = Vec::new();
        let mut unplaced = 0usize;
        for slot in self.control.held().iter() {
            let Some(owner) = placement.owner(slot) else {
                unplaced += 1;
                continue;
            };
            match receivers
                .iter_mut()
                .find(|(node, _)| *node == owner.member.id)
            {
                Some((_, count)) => *count += 1,
                None => receivers.push((owner.member.id, 1)),
            }
        }
        let mut unpulled = String::new();
        for (node, count) in &receivers {
            let _ = write!(unpulled, " node {} {count},", node.0);
        }
        let (exports, exported) = self.exports.running();
        let purge = match &self.purge {
            Purge::Idle => String::from("idle"),
            Purge::Starting { slots, sent } => {
                format!("deferred ({} slots, sent to {sent} shards)", slots.len())
            }
            Purge::Walking { left } => format!("walking ({left} shards left)"),
        };
        eprintln!(
            "keep: leave limit reached after {} ms at epoch {}: {} slots unpulled (by receiver:{} unplaced {unplaced}), {exports} exports running over {exported} slots, purge {purge}; draining the shards",
            elapsed.as_millis(),
            current.epoch(),
            self.control.held().len(),
            unpulled,
        );
    }

    fn lost_dial(&mut self, member: usize, id: ConnId) {
        if let Some(dial) = self.dials.get_mut(member)
            && *dial == Some(id)
        {
            *dial = None;
            self.control.disconnected(member, self.now);
        }
    }
}

impl<H: HeartbeatPort> Service for ControlService<H> {
    type Conn = PeerConn;
    type Msg = Msg;

    fn accept(&mut self, cx: &mut Ctx<'_, Msg>, _id: ConnId) -> Option<PeerConn> {
        let mut nonce = Nonce([0; Nonce::LEN]);
        getrandom::fill(&mut nonce.0).ok()?;
        let handshake = Box::new(self.control.accept(nonce));
        Some(PeerConn::new(Stage::Accepted { handshake }, cx.now()))
    }

    fn read(&mut self, cx: &mut Ctx<'_, Msg>, io: Io<'_, PeerConn>, data: &[u8]) -> Flow {
        self.now = cluster_time(cx.now());
        let Io { id, state, out, .. } = io;
        let PeerConn { input, stage, seen } = state;
        *seen = cx.now();
        input.extend_from_slice(data);
        let mut used = 0;
        let mut flow = Flow::Continue;
        while let Some(rest) = input.get(used..) {
            match Frame::decode(rest) {
                Ok(Decoded::Complete { frame, len }) => {
                    let Some(raw) = rest.get(..len) else {
                        break;
                    };
                    if self.frame(cx, id, stage, &frame, raw, out).is_err() {
                        flow = Flow::Close;
                        break;
                    }
                    used += len;
                }
                Ok(Decoded::Incomplete { needed }) => {
                    if needed > stage.frame_limit() {
                        flow = Flow::Close;
                    }
                    break;
                }
                Err(_) => {
                    flow = Flow::Close;
                    break;
                }
            }
        }
        input.drain(..used.min(input.len()));
        if flow == Flow::Continue && matches!(stage, Stage::Puller { .. }) && self.imports.full() {
            self.imports.hold(id);
            flow = Flow::Hold;
        }
        flow
    }

    fn closed(&mut self, id: ConnId, conn: PeerConn) {
        self.imports.forget(id);
        match conn.stage {
            Stage::Dialing { member, .. } => self.lost_dial(member, id),
            Stage::Puller { link } => self.lost_dial(link.index, id),
            Stage::Server { export, .. } => {
                if let Some(export) = export {
                    self.exports.cancel(export);
                }
            }
            Stage::Accepted { .. } => {}
        }
    }

    fn message(
        &mut self,
        cx: &mut Ctx<'_, Msg>,
        from: usize,
        msg: Msg,
        conns: &mut Conns<'_, PeerConn>,
    ) {
        self.now = cluster_time(cx.now());
        match msg {
            Msg::Drain => self.drain(cx),
            Msg::Frozen => {
                if let Shutdown::Freezing { frozen, .. } = &mut self.shutdown {
                    *frozen = frozen.saturating_add(1);
                }
                self.advance_freeze(cx);
            }
            Msg::Handoff(HandoffMsg::Chunk(chunk)) => self.exports.chunk(cx, conns, from, chunk),
            Msg::Handoff(HandoffMsg::Imported(buf)) => self.imports.imported(conns, from, buf),
            Msg::Handoff(HandoffMsg::Synced(token)) => {
                if let Some(ended) = self.imports.synced(token) {
                    self.control
                        .end(ended.from, ended.batch, ended.count, self.now);
                }
            }
            Msg::Handoff(HandoffMsg::Purged) => {
                if let Purge::Walking { left } = &mut self.purge {
                    *left = left.saturating_sub(1);
                    if *left == 0 {
                        self.purge = Purge::Idle;
                        self.control.purged();
                    }
                }
            }
            Msg::Forward(_)
            | Msg::Replies(_)
            | Msg::Purge(_)
            | Msg::Freeze
            | Msg::View
            | Msg::Stream(_)
            | Msg::Handoff(_) => {}
        }
    }

    fn ready(&mut self, cx: &mut Ctx<'_, Msg>, _source: usize, _conns: &mut Conns<'_, PeerConn>) {
        let now = cluster_time(cx.now());
        let ControlService {
            control,
            socket,
            datagram,
            counts,
            ..
        } = self;
        for _ in 0..DATAGRAMS_PER_READY {
            match socket.recv(datagram) {
                Ok(len) => {
                    let bytes = datagram.get(..len).unwrap_or_default();
                    if control.receive(bytes, now).is_err() {
                        counts.refused += 1;
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => break,
            }
        }
    }

    fn tick(&mut self, cx: &mut Ctx<'_, Msg>, conns: &mut Conns<'_, PeerConn>) -> Option<Nanos> {
        let now = cx.now();
        self.now = cluster_time(now);
        let boot = cx.read_boot();
        let suspended = self.suspends.observe(cx.suspended_total());
        if suspended >= SUSPEND_MIN {
            self.control.clock_lost(self.now);
            eprintln!(
                "keep: the system was suspended for {} ms, which the server clock does not count; clock loss declared",
                suspended.as_millis()
            );
        }
        self.advance_freeze(cx);
        let turn = match self.control.turn(self.now, boot) {
            Ok(turn) => turn,
            Err(err) => {
                self.failure = Some(err);
                cx.stop();
                return None;
            }
        };
        self.nudge |= turn.published;
        if self.nudge {
            let mesh = cx.mesh();
            self.nudge = (0..self.shards.get())
                .map(|shard| mesh.send(usize::from(shard), Msg::View).is_err())
                .fold(false, |refused, this| refused | this);
        }
        if turn.heartbeats {
            self.send_heartbeats();
        }
        self.advance_shutdown(cx);
        if now >= self.next_reap {
            self.reap(now, conns);
            self.next_reap = now.after(REAP_EVERY);
        }
        self.observe(now);
        Some(rt_time(self.control.next_deadline()).min(self.next_reap))
    }

    fn end_turn(&mut self, cx: &mut Ctx<'_, Msg>, conns: &mut Conns<'_, PeerConn>) -> Flush {
        self.now = cluster_time(cx.now());
        self.handoff_actions(cx, conns);
        self.advance_purge(cx);
        self.exports.advance(cx, conns);
        if self.control.wants_turn() {
            cx.wake_at(cx.now());
        }
        Flush::Send
    }
}

#[cold]
fn log_members(events: &[MemberEvent], epoch: u64) {
    for event in events {
        let liveness = match event.liveness {
            Liveness::Alive => "alive",
            Liveness::Suspect => "suspect",
            Liveness::Dead => "dead",
        };
        let locks = if event.lock_dead_since.is_some() {
            ", lock-dead"
        } else {
            ""
        };
        eprintln!(
            "keep: member {} {liveness}{locks}, {:?} (view epoch {epoch})",
            event.node.0, event.status
        );
    }
}

#[cold]
fn mark_ready(path: &Path, settled: bool) {
    let result = if settled {
        File::create(path).map(drop)
    } else {
        fs::remove_file(path).or_else(|err| match err.kind() {
            io::ErrorKind::NotFound => Ok(()),
            _ => Err(err),
        })
    };
    if let Err(err) = result {
        eprintln!("keep: ready file {}: {err}", path.display());
    }
}

#[cold]
fn log_freeze_limit(frozen: u16, shards: u16) {
    eprintln!(
        "keep: {frozen} of {shards} shards answered the freeze within {} ms; leaving anyway",
        FREEZE_LIMIT.as_millis()
    );
}

#[cold]
fn log_flagged(flagged: Option<NodeId>) {
    match flagged {
        Some(NodeId(node)) => eprintln!(
            "keep: peer {node} hears a member list more than one member apart from its own; fenced while such a flag is heard"
        ),
        None => eprintln!("keep: no peer flags a far member list any more"),
    }
}
