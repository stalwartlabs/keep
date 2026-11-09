/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

mod datagram;

use std::fmt;
use std::mem;
use std::time::Duration;
use std::vec::Drain;

use keep_proto::{Liveness, MAX_MEMBERS, MemberStatus, NodeId};

pub use datagram::{DatagramError, Echo, Heartbeat, MembershipKey};

use crate::locks::LockLimits;
use crate::time::Nanos;
use datagram::{bit, mask_len};

const SENT_RING: u32 = 64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct HeartbeatConfig {
    pub interval: Duration,
    pub timeout: Duration,
}

impl HeartbeatConfig {
    pub const DEFAULT: HeartbeatConfig = HeartbeatConfig {
        interval: Duration::from_millis(25),
        timeout: Duration::from_millis(225),
    };
}

impl Default for HeartbeatConfig {
    fn default() -> HeartbeatConfig {
        HeartbeatConfig::DEFAULT
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Life {
    pub node: NodeId,
    pub status: MemberStatus,
    pub incarnation: u32,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Tick {
    pub checked: bool,
    pub heartbeats: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MemberEvent {
    pub index: usize,
    pub node: NodeId,
    pub liveness: Liveness,
    pub lock_dead_since: Option<Nanos>,
    pub status: MemberStatus,
    pub at: Nanos,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FarPeer {
    pub node: NodeId,
    pub members: u16,
}

#[derive(Clone, Copy, Debug)]
struct Foreign {
    node: NodeId,
    members: u16,
    digest: u64,
    far: bool,
    life: (u32, u32),
    heard: Nanos,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Window {
    Routing,
    Lock,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MembershipError {
    NotAMember(NodeId),
    DuplicateMember(NodeId),
    TooManyMembers(usize),
}

impl fmt::Display for MembershipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MembershipError::NotAMember(node) => {
                write!(f, "node {} is not in the member list", node.0)
            }
            MembershipError::DuplicateMember(node) => {
                write!(f, "node {} is listed twice", node.0)
            }
            MembershipError::TooManyMembers(count) => {
                write!(f, "{count} members exceed {MAX_MEMBERS}")
            }
        }
    }
}

impl std::error::Error for MembershipError {}

#[derive(Clone, Debug)]
struct Peer {
    heard: Option<Nanos>,
    confirmed: Option<Nanos>,
    reported: Option<Nanos>,
    unheard: Nanos,
    incarnation: u32,
    seq: u32,
    uptime: Duration,
    age: Duration,
    status: MemberStatus,
    liveness: Liveness,
    hears: Box<[u8]>,
    lock_hears: Box<[u8]>,
    echo: Option<(u32, u32)>,
    mark: Option<Mark>,
    excused: bool,
    certified: Nanos,
}

#[derive(Clone, Copy, Debug)]
struct Mark {
    incarnation: u32,
    seq: u32,
    at: Nanos,
}

impl Mark {
    fn elapsed(&self, beat: &Heartbeat<'_>, interval: Duration) -> Duration {
        if beat.incarnation == self.incarnation {
            return interval.saturating_mul(beat.seq.saturating_sub(self.seq));
        }
        let uptime = Duration::from_millis(u64::from(beat.uptime_ms));
        if beat.incarnation != self.incarnation.wrapping_add(1) {
            return uptime;
        }
        let steps = u64::from(u32::MAX - self.seq) + u64::from(beat.seq);
        let wrapped = Duration::from_nanos(
            u64::try_from(interval.as_nanos())
                .unwrap_or(u64::MAX)
                .saturating_mul(steps),
        );
        uptime.min(wrapped)
    }
}

impl Peer {
    fn report(&self, window: Window) -> &[u8] {
        match window {
            Window::Routing => &self.hears,
            Window::Lock => &self.lock_hears,
        }
    }
}

#[derive(Debug)]
pub struct Membership {
    config: HeartbeatConfig,
    limits: LockLimits,
    lock_window: Duration,
    key: MembershipKey,
    me: usize,
    ids: Box<[NodeId]>,
    member_list: u64,
    peers: Box<[Peer]>,
    status: MemberStatus,
    announced: (u32, u32),
    acknowledged: bool,
    listen: Option<Nanos>,
    incarnation: u32,
    seq: u32,
    sent: [Nanos; SENT_RING as usize],
    clock_losses: u64,
    started: Nanos,
    now: Nanos,
    uptime_ms: u32,
    age_ms: u32,
    next_send: Nanos,
    next_check: Nanos,
    events: Vec<MemberEvent>,
    hears: Vec<u8>,
    lock_hears: Vec<u8>,
    lock_dead: Box<[Option<Nanos>]>,
    lives: Box<[u32]>,
    heard: Box<[(u32, u32)]>,
    foreign: Vec<Foreign>,
    flagged: Option<(Nanos, NodeId)>,
    votes: Vec<u16>,
    lock_votes: Vec<u16>,
    seen: Vec<u8>,
    lock_seen: Vec<u8>,
    buf: Vec<u8>,
}

impl Membership {
    pub fn new(
        config: HeartbeatConfig,
        limits: LockLimits,
        key: MembershipKey,
        members: &[NodeId],
        life: Life,
        now: Nanos,
    ) -> Result<Membership, MembershipError> {
        let Life {
            node: me,
            status,
            incarnation,
        } = life;
        if members.len() > MAX_MEMBERS {
            return Err(MembershipError::TooManyMembers(members.len()));
        }
        let mut ids = members.to_vec();
        ids.sort_unstable();
        if let Some(node) = ids.windows(2).find_map(|pair| match pair {
            [a, b] if a == b => Some(*a),
            _ => None,
        }) {
            return Err(MembershipError::DuplicateMember(node));
        }
        let me_index = ids
            .binary_search(&me)
            .map_err(|_| MembershipError::NotAMember(me))?;
        let peer = Peer {
            heard: None,
            confirmed: None,
            reported: None,
            unheard: now,
            incarnation: 0,
            seq: 0,
            uptime: Duration::ZERO,
            age: Duration::ZERO,
            status: MemberStatus::Active,
            liveness: Liveness::Alive,
            hears: vec![0; mask_len(ids.len())].into(),
            lock_hears: vec![0; mask_len(ids.len())].into(),
            echo: None,
            mark: None,
            excused: false,
            certified: now,
        };
        let listen = (ids.len() > 1).then(|| now.plus(config.timeout));
        let mut membership = Membership {
            config,
            limits,
            lock_window: limits.lock_window().max(config.timeout),
            member_list: key.member_list(&ids),
            key,
            me: me_index,
            peers: vec![peer; ids.len()].into(),
            status,
            announced: (incarnation, 1),
            acknowledged: false,
            listen,
            incarnation,
            seq: 0,
            sent: [Nanos::ZERO; SENT_RING as usize],
            clock_losses: 0,
            started: now,
            now,
            uptime_ms: 0,
            age_ms: 0,
            next_send: listen.unwrap_or(now),
            next_check: now,
            events: Vec::new(),
            hears: Vec::with_capacity(mask_len(ids.len())),
            lock_hears: Vec::with_capacity(mask_len(ids.len())),
            lock_dead: vec![None; ids.len()].into(),
            lives: (0..ids.len())
                .map(|index| if index == me_index { incarnation } else { 0 })
                .collect(),
            heard: (0..ids.len())
                .map(|index| {
                    if index == me_index {
                        (incarnation, 0)
                    } else {
                        (0, 0)
                    }
                })
                .collect(),
            foreign: Vec::new(),
            flagged: None,
            votes: Vec::with_capacity(ids.len()),
            lock_votes: Vec::with_capacity(ids.len()),
            seen: Vec::with_capacity(mask_len(ids.len())),
            lock_seen: Vec::with_capacity(mask_len(ids.len())),
            buf: Vec::with_capacity(Heartbeat::len(ids.len())),
            ids: ids.into(),
        };
        membership.refresh_hears();
        Ok(membership)
    }

    pub fn members(&self) -> &[NodeId] {
        &self.ids
    }

    pub fn me(&self) -> usize {
        self.me
    }

    pub fn node(&self) -> NodeId {
        self.ids.get(self.me).copied().unwrap_or(NodeId(0))
    }

    pub fn incarnation(&self) -> u32 {
        self.incarnation
    }

    pub fn member_list(&self) -> u64 {
        self.member_list
    }

    pub fn limits(&self) -> &LockLimits {
        &self.limits
    }

    pub fn index_of(&self, node: NodeId) -> Option<usize> {
        self.ids.binary_search(&node).ok()
    }

    pub fn status(&self, member: usize) -> Option<MemberStatus> {
        if member == self.me {
            return Some(self.own_status());
        }
        self.peers.get(member).map(|peer| peer.status)
    }

    pub fn liveness(&self, member: usize) -> Option<Liveness> {
        if member == self.me {
            return Some(Liveness::Alive);
        }
        self.peers.get(member).map(|peer| peer.liveness)
    }

    pub fn lock_deaths(&self) -> &[Option<Nanos>] {
        &self.lock_dead
    }

    pub fn lives(&self) -> &[u32] {
        &self.lives
    }

    pub fn heard(&self) -> &[(u32, u32)] {
        &self.heard
    }

    pub fn announced(&self) -> (u32, u32) {
        self.announced
    }

    pub fn far(&self, now: Nanos) -> bool {
        self.flagged(now) || self.hears_far(now)
    }

    pub fn far_peers(&self) -> impl Iterator<Item = FarPeer> + '_ {
        let (now, timeout) = (self.now, self.config.timeout);
        self.foreign
            .iter()
            .filter(move |entry| entry.far && now.since(entry.heard) <= timeout)
            .map(|entry| FarPeer {
                node: entry.node,
                members: entry.members,
            })
    }

    pub fn flagged_by(&self) -> Option<NodeId> {
        self.flagged
            .filter(|(at, _)| self.now.since(*at) <= self.config.timeout)
            .map(|(_, node)| node)
    }

    pub fn hearing(&self) -> usize {
        self.hears
            .iter()
            .map(|byte| byte.count_ones() as usize)
            .sum()
    }

    pub fn settled(&self) -> bool {
        let timeout = self.config.timeout;
        self.now.since(self.started) >= self.lock_window
            && self.own_status() == MemberStatus::Active
            && !self.flagged(self.now)
            && !self
                .foreign
                .iter()
                .any(|entry| self.now.since(entry.heard) <= timeout)
            && self.peers.iter().enumerate().all(|(index, peer)| {
                index == self.me
                    || peer.liveness == Liveness::Dead
                    || peer.status == MemberStatus::Active
            })
    }

    pub fn placing(&self) -> usize {
        let me = usize::from(self.own_status() == MemberStatus::Active);
        let peers = self
            .peers
            .iter()
            .enumerate()
            .filter(|(index, peer)| {
                *index != self.me
                    && peer.status == MemberStatus::Active
                    && peer.liveness != Liveness::Dead
            })
            .count();
        me + peers
    }

    pub fn confirmations(&self) -> impl Iterator<Item = Option<Nanos>> + '_ {
        self.peers
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != self.me)
            .map(|(_, peer)| peer.confirmed)
    }

    pub fn cluster_age(&self, now: Nanos) -> Duration {
        self.peers
            .iter()
            .filter_map(|peer| Some(peer.age.saturating_add(now.since(peer.heard?))))
            .max()
            .unwrap_or_default()
    }

    pub fn clock_losses(&self) -> u64 {
        self.clock_losses
    }

    pub fn awaiting(&self) -> impl Iterator<Item = usize> + '_ {
        let active = self.status == MemberStatus::Active;
        let sent = (self.incarnation, self.seq);
        self.peers
            .iter()
            .enumerate()
            .filter(move |(index, peer)| {
                active
                    && *index != self.me
                    && !peer.excused
                    && !match peer.echo {
                        Some(echo) => echo >= self.announced && echo <= sent,
                        None => peer.heard.is_some(),
                    }
            })
            .map(|(index, _)| index)
    }

    pub fn set_status(&mut self, status: MemberStatus) -> bool {
        if self.status == MemberStatus::Leaving {
            return self.status == status;
        }
        if status == MemberStatus::Active && self.status != MemberStatus::Active {
            self.announced = match self.seq.checked_add(1) {
                Some(seq) => (self.incarnation, seq),
                None => (self.incarnation.saturating_add(1), 1),
            };
            self.acknowledged = false;
        }
        self.status = status;
        true
    }

    pub fn next_deadline(&self) -> Nanos {
        self.next_send.min(self.next_check)
    }

    pub fn drain_events(&mut self) -> Drain<'_, MemberEvent> {
        self.events.drain(..)
    }

    pub fn receive(&mut self, bytes: &[u8], now: Nanos) -> Result<(), DatagramError> {
        let beat = Heartbeat::decode(bytes, &self.key)?;
        if usize::from(beat.members) != self.ids.len() || beat.member_list != self.member_list {
            self.foreign(&beat, now);
            return Err(DatagramError::MemberList);
        }
        let index = self
            .index_of(beat.sender)
            .filter(|index| *index != self.me && self.ids.get(self.me) == Some(&beat.receiver))
            .ok_or(DatagramError::UnknownSender(beat.sender))?;
        let sent = beat.echo.and_then(|echo| self.sent_at(echo));
        let far = self.far(now);
        let interval = self.config.interval;
        let skip = self.limits.quarantine() / 2;
        let uptime = Duration::from_millis(u64::from(beat.uptime_ms));
        let Some(peer) = self.peers.get_mut(index) else {
            return Err(DatagramError::UnknownSender(beat.sender));
        };
        if peer.heard.is_some() && (beat.incarnation, beat.seq) <= (peer.incarnation, peer.seq) {
            return Err(DatagramError::Stale);
        }
        let skewed = peer.mark.is_some_and(|mark| {
            mark.elapsed(&beat, interval) > now.since(mark.at).saturating_add(skip)
        });
        peer.heard = Some(now);
        peer.echo = beat.echo.map(|echo| (echo.incarnation, echo.seq));
        peer.incarnation = beat.incarnation;
        if let Some(life) = self.lives.get_mut(index) {
            *life = beat.incarnation;
        }
        if let Some(heard) = self.heard.get_mut(index) {
            *heard = (beat.incarnation, beat.seq);
        }
        peer.seq = beat.seq;
        peer.uptime = uptime;
        peer.age = Duration::from_millis(u64::from(beat.cluster_age_ms));
        peer.reported = sent;
        if let Some(at) = sent
            && beat.echo.is_some_and(|echo| echo.confirms)
            && !far
        {
            peer.confirmed = Some(peer.confirmed.map_or(at, |old| old.max(at)));
        }
        for (mine, theirs) in peer.hears.iter_mut().zip(beat.hears) {
            *mine = *theirs;
        }
        for (mine, theirs) in peer.lock_hears.iter_mut().zip(beat.lock_hears) {
            *mine = *theirs;
        }
        if peer.status != beat.status {
            peer.status = beat.status;
            self.events.push(MemberEvent {
                index,
                node: beat.sender,
                liveness: peer.liveness,
                lock_dead_since: self.lock_dead.get(index).copied().flatten(),
                status: beat.status,
                at: now,
            });
        }
        if beat.far {
            self.flag(beat.sender, now);
        }
        if skewed {
            self.lose_clock();
            self.next_check = now;
        }
        Ok(())
    }

    pub fn tick(&mut self, now: Nanos) -> Tick {
        self.now = now;
        if let Some(until) = self.listen
            && (now >= until || self.heard_every_live_member())
        {
            self.listen = None;
            self.next_send = now;
        }
        if !self.acknowledged && self.status != MemberStatus::Leaving {
            self.renew(now);
        }
        let checked = now >= self.next_check;
        let heartbeats = now >= self.next_send;
        if checked || heartbeats {
            self.refresh_hears();
        }
        if checked {
            for peer in self.peers.iter_mut() {
                peer.mark = peer.heard.map(|at| Mark {
                    incarnation: peer.incarnation,
                    seq: peer.seq,
                    at,
                });
            }
            self.agree(now);
            let step = (self.config.interval / 4).max(Duration::from_millis(1));
            self.next_check = now.plus(step);
        }
        if heartbeats {
            self.advance(now);
        }
        Tick {
            checked,
            heartbeats,
        }
    }

    pub fn heartbeat(&mut self, to: usize) -> Option<&[u8]> {
        if to == self.me || self.listen.is_some() {
            return None;
        }
        let receiver = *self.ids.get(to)?;
        let peer = self.peers.get(to)?;
        let far = self.hears_far(self.now);
        let confirms = bit(&self.hears, to)
            && self.now >= peer.unheard.plus(self.config.timeout)
            && !far
            && !self.flagged(self.now);
        Heartbeat {
            sender: *self.ids.get(self.me)?,
            receiver,
            incarnation: self.incarnation,
            seq: self.seq,
            echo: peer.heard.map(|_| Echo {
                incarnation: peer.incarnation,
                seq: peer.seq,
                confirms,
            }),
            uptime_ms: self.uptime_ms,
            cluster_age_ms: self.age_ms,
            status: self.status,
            far,
            members: u16::try_from(self.ids.len()).ok()?,
            member_list: self.member_list,
            hears: &self.hears,
            lock_hears: &self.lock_hears,
        }
        .encode(&self.key, &mut self.buf);
        Some(&self.buf)
    }

    fn advance(&mut self, now: Nanos) {
        let advanced = match self.seq.checked_add(1) {
            Some(seq) => {
                self.seq = seq;
                true
            }
            None => match self.incarnation.checked_add(1) {
                Some(incarnation) => {
                    self.incarnation = incarnation;
                    self.seq = 1;
                    if let Some(life) = self.lives.get_mut(self.me) {
                        *life = incarnation;
                    }
                    true
                }
                None => false,
            },
        };
        if advanced && let Some(at) = self.sent.get_mut((self.seq % SENT_RING) as usize) {
            *at = now;
        }
        if let Some(own) = self.heard.get_mut(self.me) {
            *own = (self.incarnation, self.seq);
        }
        let uptime = now.since(self.started);
        self.uptime_ms = saturating_ms(uptime);
        self.age_ms = saturating_ms(uptime.max(self.cluster_age(now)));
        self.next_send = self.next_send.plus(self.config.interval).max(now);
    }

    fn sent_at(&self, echo: Echo) -> Option<Nanos> {
        let back = self.seq.checked_sub(echo.seq)?;
        if echo.incarnation != self.incarnation || echo.seq == 0 || back >= SENT_RING {
            return None;
        }
        self.sent.get((echo.seq % SENT_RING) as usize).copied()
    }

    fn refresh_hears(&mut self) {
        let now = self.now;
        let timeout = self.config.timeout;
        let window = self.lock_window;
        let len = mask_len(self.ids.len());
        self.hears.clear();
        self.hears.resize(len, 0);
        self.lock_hears.clear();
        self.lock_hears.resize(len, 0);
        for (index, peer) in self.peers.iter_mut().enumerate() {
            let silence = match peer.heard {
                _ if index == self.me => Duration::ZERO,
                Some(at) => now.since(at),
                None => Duration::MAX,
            };
            let mask = 1 << (index % 8);
            if silence > timeout {
                peer.unheard = now;
            } else if let Some(byte) = self.hears.get_mut(index / 8) {
                *byte |= mask;
            }
            if silence <= window
                && let Some(byte) = self.lock_hears.get_mut(index / 8)
            {
                *byte |= mask;
            }
        }
    }

    fn heard_every_live_member(&self) -> bool {
        let others = || {
            self.peers
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != self.me)
        };
        let mut heard_one = false;
        for (member, peer) in others() {
            if peer.heard.is_some() {
                heard_one = true;
            } else if others()
                .any(|(_, reporter)| reporter.heard.is_some() && bit(&reporter.hears, member))
            {
                return false;
            }
        }
        heard_one
    }

    fn renew(&mut self, now: Nanos) {
        let sent = (self.incarnation, self.seq);
        let Some(incarnation) = self
            .peers
            .iter()
            .filter_map(|peer| peer.echo)
            .filter(|echo| *echo > sent)
            .map(|(incarnation, _)| incarnation)
            .max()
            .and_then(|newest| newest.checked_add(1))
        else {
            return;
        };
        self.incarnation = incarnation;
        self.seq = 0;
        if self.status == MemberStatus::Active {
            self.announced = (incarnation, 1);
        }
        self.started = now;
        if self.listen.is_none() {
            self.next_send = now;
        }
        if let Some(life) = self.lives.get_mut(self.me) {
            *life = incarnation;
        }
        if let Some(own) = self.heard.get_mut(self.me) {
            *own = (incarnation, 0);
        }
    }

    fn foreign(&mut self, beat: &Heartbeat<'_>, now: Nanos) {
        if self.ids.get(self.me) != Some(&beat.receiver) {
            return;
        }
        let life = (beat.incarnation, beat.seq);
        if self
            .index_of(beat.sender)
            .and_then(|index| self.heard.get(index))
            .is_some_and(|heard| *heard >= life)
        {
            return;
        }
        let position = self
            .foreign
            .iter()
            .position(|entry| entry.node == beat.sender);
        let known = position.and_then(|at| self.foreign.get(at)).copied();
        if known.is_some_and(|entry| life <= entry.life) {
            return;
        }
        let far = match known {
            Some(entry) if (entry.members, entry.digest) == (beat.members, beat.member_list) => {
                entry.far
            }
            _ => self.far_from(beat.sender, beat.members, beat.member_list),
        };
        let entry = Foreign {
            node: beat.sender,
            members: beat.members,
            digest: beat.member_list,
            far,
            life,
            heard: now,
        };
        if let Some(slot) = position.and_then(|at| self.foreign.get_mut(at)) {
            *slot = entry;
        } else if self.foreign.len() < MAX_MEMBERS {
            self.foreign.push(entry);
        } else if let Some(stale) = self.foreign.iter_mut().min_by_key(|entry| entry.heard) {
            *stale = entry;
        }
        if beat.far {
            self.flag(beat.sender, now);
        } else if far {
            self.drop_credits();
        }
    }

    fn far_from(&self, sender: NodeId, members: u16, digest: u64) -> bool {
        let mine = self.ids.len();
        let theirs = usize::from(members);
        if theirs + 1 == mine {
            return !self
                .ids
                .iter()
                .any(|id| self.member_list.wrapping_sub(self.key.member(*id)) == digest);
        }
        if theirs == mine + 1 {
            return self.index_of(sender).is_none()
                && self.member_list.wrapping_add(self.key.member(sender)) != digest;
        }
        true
    }

    fn hears_far(&self, now: Nanos) -> bool {
        let timeout = self.config.timeout;
        self.foreign
            .iter()
            .any(|entry| entry.far && now.since(entry.heard) <= timeout)
    }

    fn flagged(&self, now: Nanos) -> bool {
        self.flagged
            .is_some_and(|(at, _)| now.since(at) <= self.config.timeout)
    }

    fn flag(&mut self, sender: NodeId, now: Nanos) {
        if self.flagged.is_none_or(|(at, _)| at <= now) {
            self.flagged = Some((now, sender));
        }
        self.drop_credits();
    }

    fn drop_credits(&mut self) {
        for peer in self.peers.iter_mut() {
            peer.confirmed = None;
        }
    }

    fn own_status(&self) -> MemberStatus {
        match self.status {
            MemberStatus::Active if !self.acknowledged => MemberStatus::Joining,
            status => status,
        }
    }

    pub fn clock_lost(&mut self, now: Nanos) {
        self.lose_clock();
        self.next_check = now;
    }

    fn lose_clock(&mut self) {
        self.clock_losses += 1;
        for peer in self.peers.iter_mut() {
            peer.confirmed = None;
            peer.reported = None;
            peer.mark = None;
        }
    }

    fn acknowledged(&self) -> bool {
        self.status == MemberStatus::Active && self.awaiting().next().is_none()
    }

    fn agree(&mut self, now: Nanos) {
        let majority = self.ids.len() / 2 + 1;
        let lock_break = self.limits.lock_break();
        let mut votes = mem::take(&mut self.votes);
        let mut lock_votes = mem::take(&mut self.lock_votes);
        let mut seen = mem::take(&mut self.seen);
        let mut lock_seen = mem::take(&mut self.lock_seen);
        self.tally(&mut votes, &mut seen, now, Window::Routing);
        self.tally(&mut lock_votes, &mut lock_seen, now, Window::Lock);
        for (index, (((peer, node), lock_dead), (votes, lock_votes))) in self
            .peers
            .iter_mut()
            .zip(self.ids.iter())
            .zip(self.lock_dead.iter_mut())
            .zip(votes.iter().zip(&lock_votes))
            .enumerate()
        {
            if index == self.me {
                continue;
            }
            let liveness = if usize::from(*votes) >= majority
                || (peer.liveness == Liveness::Dead && !bit(&seen, index))
            {
                Liveness::Dead
            } else {
                Liveness::Alive
            };
            let lock_majority = usize::from(*lock_votes) >= majority;
            let lock_heard = bit(&lock_seen, index);
            peer.excused = lock_majority || (peer.excused && !lock_heard);
            let since = match *lock_dead {
                None if lock_majority => Some(now),
                Some(since)
                    if lock_majority
                        || (!lock_heard && now.since(peer.certified) <= lock_break) =>
                {
                    Some(since)
                }
                _ => None,
            };
            if lock_majority {
                peer.certified = now;
            }
            if peer.liveness != liveness || *lock_dead != since {
                peer.liveness = liveness;
                *lock_dead = since;
                self.events.push(MemberEvent {
                    index,
                    node: *node,
                    liveness,
                    lock_dead_since: since,
                    status: peer.status,
                    at: now,
                });
            }
        }
        self.votes = votes;
        self.lock_votes = lock_votes;
        self.seen = seen;
        self.lock_seen = lock_seen;
        let acknowledged = self.acknowledged();
        if acknowledged != self.acknowledged {
            self.acknowledged = acknowledged;
            if let Some(node) = self.ids.get(self.me) {
                self.events.push(MemberEvent {
                    index: self.me,
                    node: *node,
                    liveness: Liveness::Alive,
                    lock_dead_since: None,
                    status: self.own_status(),
                    at: now,
                });
            }
        }
    }

    fn tally(&self, votes: &mut Vec<u16>, seen: &mut Vec<u8>, now: Nanos, window: Window) {
        let (span, own) = match window {
            Window::Routing => (self.config.timeout, &self.hears),
            Window::Lock => (self.lock_window, &self.lock_hears),
        };
        let fresh = now.minus(self.config.timeout);
        votes.clear();
        votes.resize(self.ids.len(), 0);
        seen.clear();
        seen.extend_from_slice(own);
        if now.since(self.started) >= span {
            add_unheard(votes, own);
        }
        for peer in self
            .peers
            .iter()
            .filter(|peer| peer.reported.is_some_and(|at| at >= fresh))
        {
            let report = peer.report(window);
            for (seen, heard) in seen.iter_mut().zip(report) {
                *seen |= *heard;
            }
            if peer.uptime >= span {
                add_unheard(votes, report);
            }
        }
    }
}

fn add_unheard(votes: &mut [u16], hears: &[u8]) {
    for (byte, votes) in hears.iter().zip(votes.chunks_mut(8)) {
        let mut unheard = !byte;
        while unheard != 0 {
            let index = unheard.trailing_zeros() as usize;
            unheard &= unheard - 1;
            if let Some(vote) = votes.get_mut(index) {
                *vote += 1;
            }
        }
    }
}

fn saturating_ms(span: Duration) -> u32 {
    u32::try_from(span.as_millis()).unwrap_or(u32::MAX)
}
