/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

mod peer;
mod service;
mod stats;

use std::fmt;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::str::FromStr;
use std::sync::Arc;

use keep_cluster::{
    Answer, DatagramError, FarPeer, FenceState, HandoffAction, HandoffLimits, Handoffs, Handshake,
    HeartbeatConfig, Life, LockLimits, MemberEvent, Membership, MembershipKey, Nanos, Ownership,
    PeerError, PeerFrame, PeerHello, PeerLink, Roster, SelfFence, SlotSet, Start, ViewError,
};
use keep_proto::{ClusterSecret, Liveness, Member, MemberStatus, NodeId, Nonce, PeerSecret, Slot};

use crate::cluster::{Published, Snapshot, rt_time};

pub use peer::PeerConn;
pub use service::{ControlService, HeartbeatPort};
pub use stats::{ControlCounts, ControlStats};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Peer {
    pub node: NodeId,
    pub client: SocketAddr,
    pub peer: SocketAddr,
    pub shards: NonZeroU16,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PeerSyntax {
    Shape,
    Node,
    Client,
    Peer,
    Shards,
}

impl fmt::Display for PeerSyntax {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let part = match self {
            PeerSyntax::Shape => "shape",
            PeerSyntax::Node => "node id (a non-zero u64)",
            PeerSyntax::Client => "client address",
            PeerSyntax::Peer => "peer address",
            PeerSyntax::Shards => "shard count",
        };
        write!(
            f,
            "invalid {part} in a member entry; expected node@client_ip:port/peer_ip:port/shards, the peer address may be a port alone"
        )
    }
}

impl std::error::Error for PeerSyntax {}

impl FromStr for Peer {
    type Err = PeerSyntax;

    fn from_str(text: &str) -> Result<Peer, PeerSyntax> {
        let (node, rest) = text.trim().split_once('@').ok_or(PeerSyntax::Shape)?;
        let (client, rest) = rest.split_once('/').ok_or(PeerSyntax::Shape)?;
        let (peer, shards) = rest.split_once('/').ok_or(PeerSyntax::Shape)?;
        let node = node
            .parse::<u64>()
            .ok()
            .filter(|&node| node != 0)
            .ok_or(PeerSyntax::Node)?;
        let client: SocketAddr = client.parse().map_err(|_| PeerSyntax::Client)?;
        let peer = match peer.parse::<SocketAddr>() {
            Ok(peer) => peer,
            Err(_) => SocketAddr::new(client.ip(), peer.parse().map_err(|_| PeerSyntax::Peer)?),
        };
        Ok(Peer {
            node: NodeId(node),
            client,
            peer,
            shards: shards.parse().map_err(|_| PeerSyntax::Shards)?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub me: NodeId,
    pub members: Vec<Peer>,
    pub peer_secret: PeerSecret,
    pub limits: LockLimits,
    pub start: Start,
    pub incarnation: u32,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Turn {
    pub published: bool,
    pub heartbeats: bool,
}

pub struct Control {
    membership: Membership,
    fence: SelfFence,
    ownership: Ownership,
    handoffs: Handoffs,
    peers: Box<[Peer]>,
    published: Arc<Published>,
    peer_secret: PeerSecret,
    rebuild: bool,
    events: Vec<MemberEvent>,
}

impl Control {
    pub fn start(config: ClusterConfig, now: Nanos) -> io::Result<Control> {
        let ClusterConfig {
            me,
            mut members,
            peer_secret,
            limits,
            start,
            incarnation,
        } = config;
        members.sort_unstable_by_key(|peer| peer.node);
        let ids: Vec<NodeId> = members.iter().map(|peer| peer.node).collect();
        let membership = Membership::new(
            HeartbeatConfig::DEFAULT,
            limits,
            MembershipKey::derive(&peer_secret),
            &ids,
            Life {
                node: me,
                status: MemberStatus::Active,
                incarnation,
            },
            now,
        )
        .map_err(io::Error::other)?;
        let fence = SelfFence::new(&membership);
        let mut ownership = Ownership::new(me, start, &limits);
        let mut handoffs = Handoffs::new(membership.me(), HandoffLimits::DEFAULT);
        let view = ownership
            .next(
                roster(&membership, &members),
                now,
                fence.state(),
                &mut handoffs,
            )
            .map_err(io::Error::other)?
            .ok_or_else(|| io::Error::other("the first view is empty"))?;
        let published = Arc::new(Published::new(Snapshot::new(view)));
        let (until, floor) = ownership.lease(&fence);
        published.lease().publish(until, floor);
        Ok(Control {
            membership,
            fence,
            ownership,
            handoffs,
            peers: members.into_boxed_slice(),
            published,
            peer_secret,
            rebuild: false,
            events: Vec::new(),
        })
    }

    pub fn published(&self) -> &Arc<Published> {
        &self.published
    }

    pub fn fence(&self) -> FenceState {
        self.fence.state()
    }

    pub fn members(&self) -> usize {
        self.peers.len()
    }

    pub fn incarnation(&self) -> u32 {
        self.membership.incarnation()
    }

    pub fn receive(&mut self, bytes: &[u8], now: Nanos) -> Result<(), DatagramError> {
        self.membership.receive(bytes, now)
    }

    pub fn clock_lost(&mut self, now: Nanos) {
        self.membership.clock_lost(now);
    }

    pub fn turn(&mut self, now: Nanos, boot: u64) -> Result<Turn, ViewError> {
        let tick = self.membership.tick(now);
        let mut changed = tick.checked && self.fence.update(now, &self.membership);
        let me = self.membership.me();
        self.events.clear();
        for event in self.membership.drain_events() {
            changed = true;
            if event.index != me {
                self.events.push(event);
            }
        }
        changed |= mem::take(&mut self.rebuild);
        if self.handoffs.next_deadline() <= now {
            changed |= self.handoffs.tick(now);
        }
        let mut published = false;
        if changed
            && let Some(view) = self.ownership.next(
                roster(&self.membership, &self.peers),
                now,
                self.fence.state(),
                &mut self.handoffs,
            )?
        {
            self.published.publish(Snapshot::new(view));
            published = true;
        }
        let (until, floor) = self.ownership.lease(&self.fence);
        self.published
            .publish_turn(until, floor, rt_time(now), boot);
        Ok(Turn {
            published,
            heartbeats: tick.heartbeats,
        })
    }

    pub fn member_events(&self) -> &[MemberEvent] {
        &self.events
    }

    pub fn heartbeats(&mut self, mut send: impl FnMut(SocketAddr, &[u8])) {
        let Control {
            membership, peers, ..
        } = self;
        for (to, peer) in peers.iter().enumerate() {
            if let Some(bytes) = membership.heartbeat(to) {
                send(peer.peer, bytes);
            }
        }
    }

    pub fn next_deadline(&self) -> Nanos {
        self.membership
            .next_deadline()
            .min(self.handoffs.next_deadline())
    }

    pub fn wants_turn(&self) -> bool {
        self.rebuild
    }

    pub fn peer_addr(&self, index: usize) -> Option<SocketAddr> {
        self.peers.get(index).map(|peer| peer.peer)
    }

    pub fn actions(&mut self, into: &mut Vec<HandoffAction>) {
        into.extend(self.handoffs.drain_actions());
    }

    pub fn initiate(&self, index: usize, nonce: Nonce) -> Option<(Handshake, PeerHello)> {
        let node = self.peers.get(index)?.node;
        Some(Handshake::initiate(&self.membership, node, nonce))
    }

    pub fn accept(&self, nonce: Nonce) -> Handshake {
        Handshake::accept(&self.membership, nonce)
    }

    pub fn step(
        &self,
        handshake: &mut Handshake,
        frame: &PeerFrame<'_>,
        out: &mut Vec<u8>,
    ) -> Result<Option<PeerLink>, PeerError> {
        handshake.step(frame, &self.peer_secret, &self.membership, out)
    }

    pub fn answer(
        &mut self,
        peer: PeerLink,
        batch: u32,
        slots: &SlotSet,
        since: (u32, u32),
        now: Nanos,
    ) -> Answer {
        self.handoffs.answer(peer, batch, slots, since, now)
    }

    pub fn acked(&mut self, peer: PeerLink, batch: u32) {
        self.handoffs.acked(peer, batch);
    }

    pub fn disconnected(&mut self, index: usize, now: Nanos) {
        self.handoffs.disconnected(index, now);
    }

    pub fn ready(&mut self, from: usize, batch: u32, answer: &Answer, now: Nanos) {
        self.rebuild |= self.handoffs.ready(from, batch, answer, now);
    }

    #[inline]
    pub fn entry(&mut self, from: usize, batch: u32, slot: Slot) -> bool {
        self.handoffs.entry(from, batch, slot)
    }

    pub fn end(&mut self, from: usize, batch: u32, count: u32, now: Nanos) {
        self.rebuild |= self.handoffs.end(from, batch, count, now);
    }

    pub fn purged(&mut self) {
        self.rebuild |= self.handoffs.purged();
    }

    pub fn leave(&mut self) {
        self.rebuild |= self.membership.set_status(MemberStatus::Leaving);
    }

    pub fn handed_over(&self) -> bool {
        let me = self.membership.me();
        let receivers = (0..self.peers.len()).any(|index| {
            index != me
                && self.membership.status(index) == Some(MemberStatus::Active)
                && self.membership.liveness(index) == Some(Liveness::Alive)
        });
        let isolated = self.fence.state().fenced()
            && self.membership.hearing() * 2 <= self.membership.members().len();
        !receivers || isolated || self.handoffs.held().is_empty()
    }

    pub fn settled(&self) -> bool {
        !self.fence.state().fenced() && self.membership.settled() && self.handoffs.settled()
    }

    pub fn run_outs(&self) -> u64 {
        self.handoffs.run_outs()
    }

    pub fn held(&self) -> &SlotSet {
        self.handoffs.held()
    }

    pub fn far_peers(&self) -> impl Iterator<Item = FarPeer> + '_ {
        self.membership.far_peers()
    }

    pub fn flagged_by(&self) -> Option<NodeId> {
        self.membership.flagged_by()
    }

    pub fn awaiting(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.membership
            .awaiting()
            .filter_map(|index| self.peers.get(index).map(|peer| peer.node))
    }
}

pub fn secrets_differ(cluster: &ClusterSecret, peer: &PeerSecret) -> bool {
    let difference = cluster
        .to_bytes()
        .iter()
        .zip(peer.to_bytes())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b));
    std::hint::black_box(difference) != 0
}

fn roster<'a>(membership: &'a Membership, peers: &[Peer]) -> Roster<'a> {
    let members = peers
        .iter()
        .enumerate()
        .map(|(index, peer)| Member {
            id: peer.node,
            ip: peer.client.ip(),
            port: peer.client.port(),
            shards: peer.shards,
            status: membership.status(index).unwrap_or(MemberStatus::Active),
            liveness: membership.liveness(index).unwrap_or(Liveness::Dead),
        })
        .collect();
    Roster {
        members,
        lock_deaths: membership.lock_deaths(),
        lives: membership.lives(),
        heard: membership.heard(),
        announced: membership.announced(),
    }
}
