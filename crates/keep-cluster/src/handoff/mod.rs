/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

mod frames;
mod peer;

use std::mem;
use std::time::Duration;
use std::vec::Drain;

use keep_proto::{Liveness, Member, MemberStatus, Placement, SLOT_COUNT, Slot};

use crate::slots::SlotSet;
use crate::time::Nanos;
use crate::view::View;

pub use frames::{PeerFrame, PeerFrameError, PeerHello};
pub use peer::{Handshake, PeerError, PeerLink};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct HandoffLimits {
    pub leave_wait: Duration,
    pub hold: Duration,
    pub retry: Duration,
    pub reply: Duration,
}

impl HandoffLimits {
    pub const DEFAULT: HandoffLimits = HandoffLimits {
        leave_wait: Duration::from_secs(10),
        hold: Duration::from_secs(30),
        retry: Duration::from_millis(200),
        reply: Duration::from_secs(5),
    };
}

impl Default for HandoffLimits {
    fn default() -> HandoffLimits {
        HandoffLimits::DEFAULT
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(crate) enum Inflow {
    #[default]
    None,
    Lost {
        after: (u32, u32),
    },
    Open {
        from: u16,
    },
    Join {
        from: u16,
        life: u32,
        after: (u32, u32),
    },
    Leave {
        from: u16,
        life: u32,
        after: (u32, u32),
    },
    Orphan {
        from: u16,
        since: Nanos,
    },
    Handed {
        from: u16,
        base: Nanos,
    },
    Regain {
        from: u16,
        life: u32,
        after: (u32, u32),
    },
}

const _: () = assert!(size_of::<Inflow>() <= 16);

impl Inflow {
    pub(crate) fn source(self) -> Option<u16> {
        match self {
            Inflow::None | Inflow::Lost { .. } => None,
            Inflow::Open { from }
            | Inflow::Join { from, .. }
            | Inflow::Leave { from, .. }
            | Inflow::Orphan { from, .. }
            | Inflow::Handed { from, .. }
            | Inflow::Regain { from, .. } => Some(from),
        }
    }

    fn awaited_from(self) -> Option<(u16, u32)> {
        match self {
            Inflow::Join { from, life, .. }
            | Inflow::Leave { from, life, .. }
            | Inflow::Regain { from, life, .. } => Some((from, life)),
            _ => None,
        }
    }

    fn after(self) -> (u32, u32) {
        match self {
            Inflow::Join { after, .. }
            | Inflow::Leave { after, .. }
            | Inflow::Regain { after, .. } => after,
            _ => (0, 0),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum HandoffAction {
    Pull {
        to: usize,
        batch: u32,
        slots: SlotSet,
        since: (u32, u32),
    },
    Ack {
        to: usize,
        batch: u32,
    },
    Purge {
        slots: SlotSet,
    },
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Answer {
    pub data: SlotSet,
    pub empty: SlotSet,
    pub quarantine_ms: u32,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Pulling {
    Due {
        at: Nanos,
    },
    AfterPurge,
    Sent {
        batch: u32,
        at: Nanos,
    },
    Importing {
        batch: u32,
        base: Nanos,
        entries: u32,
        seen: u32,
        walks: u32,
        check: Nanos,
    },
}

#[derive(Clone, Debug)]
struct Source {
    from: u16,
    life: u32,
    state: Pulling,
    data: SlotSet,
    since: (u32, u32),
}

#[derive(Clone, Debug)]
struct Hold {
    until: Nanos,
    quarantine: Nanos,
    to: u16,
    heard: (u32, u32),
    granted: bool,
    slots: SlotSet,
}

#[derive(Clone, Debug)]
struct Served {
    batch: u32,
    life: u32,
    data: SlotSet,
}

#[derive(Clone, Copy, Debug)]
struct Unacked {
    to: u16,
    life: u32,
    batch: u32,
    until: Nanos,
    resend: Option<Nanos>,
}

#[derive(Debug)]
pub struct Handoffs {
    me: usize,
    limits: HandoffLimits,
    inflow: Box<[Inflow]>,
    owned: SlotSet,
    awaiting: SlotSet,
    gated: SlotSet,
    held: SlotSet,
    intact: SlotSet,
    kept: SlotSet,
    holds: Vec<Hold>,
    doomed: SlotSet,
    walking: Option<SlotSet>,
    walks: u32,
    leaving_since: Vec<Option<Nanos>>,
    leave_due: Vec<Option<Nanos>>,
    regain_due: Option<Nanos>,
    regain_floor: (u32, u32),
    reopened: bool,
    sources: Vec<Source>,
    served: Vec<Option<Served>>,
    unacked: Vec<Unacked>,
    next_batch: u32,
    actions: Vec<HandoffAction>,
    fenced: bool,
    announced: (u32, u32),
    quarantine: Duration,
    run_outs: u64,
}

impl Handoffs {
    pub fn new(me: usize, limits: HandoffLimits) -> Handoffs {
        Handoffs {
            me,
            limits,
            inflow: vec![Inflow::None; SLOT_COUNT].into_boxed_slice(),
            owned: SlotSet::default(),
            awaiting: SlotSet::default(),
            gated: SlotSet::default(),
            held: SlotSet::default(),
            intact: SlotSet::default(),
            kept: SlotSet::default(),
            holds: Vec::new(),
            doomed: SlotSet::default(),
            walking: None,
            walks: 0,
            leaving_since: Vec::new(),
            leave_due: Vec::new(),
            regain_due: None,
            regain_floor: (0, 0),
            reopened: false,
            sources: Vec::new(),
            served: Vec::new(),
            unacked: Vec::new(),
            next_batch: 1,
            actions: Vec::new(),
            fenced: false,
            announced: (0, 0),
            quarantine: Duration::ZERO,
            run_outs: 0,
        }
    }

    pub fn gated(&self) -> &SlotSet {
        &self.gated
    }

    pub fn held(&self) -> &SlotSet {
        &self.held
    }

    pub fn settled(&self) -> bool {
        self.gated.is_empty() && self.held.is_empty()
    }

    pub fn run_outs(&self) -> u64 {
        self.run_outs
    }

    pub fn dirty(&self) -> SlotSet {
        let mut dirty = self.held.clone();
        dirty.union_with(&self.doomed);
        if let Some(walking) = &self.walking {
            dirty.union_with(walking);
        }
        dirty
    }

    pub(crate) fn inflow(&self, slot: Slot) -> Inflow {
        self.inflow.get(slot.index()).copied().unwrap_or_default()
    }

    pub fn drain_actions(&mut self) -> Drain<'_, HandoffAction> {
        self.actions.drain(..)
    }

    pub fn next_deadline(&self) -> Nanos {
        let reply = self.limits.reply;
        let leave = self.leave_due.iter().flatten().copied();
        let holds = self.holds.iter().map(|hold| hold.until);
        let acks = self.unacked.iter().filter_map(|ack| ack.resend);
        let pulls = self.sources.iter().filter_map(|source| match source.state {
            Pulling::Due { at } => Some(at),
            Pulling::AfterPurge => None,
            Pulling::Sent { at, .. } => Some(at.plus(reply)),
            Pulling::Importing { check, .. } => Some(check),
        });
        leave
            .chain(self.regain_due)
            .chain(holds)
            .chain(acks)
            .chain(pulls)
            .fold(Nanos::MAX, Nanos::min)
    }

    pub(crate) fn plan(&mut self, change: Change<'_>, now: Nanos) {
        let Change {
            previous,
            next,
            lock_deaths,
            lives,
            heard,
            announced,
            ends,
            fenced,
            regain,
            quarantine,
            max_hold,
        } = change;
        let lapsed = fenced || regain.is_some() || announced != self.announced;
        if lapsed {
            self.intact.clear();
        }
        self.fenced = fenced;
        self.announced = announced;
        self.quarantine = quarantine;
        let members = next.topology().members();
        let me = members.get(self.me).map(|member| member.id);
        let wait = self.limits.leave_wait;
        self.leaving_since.resize(members.len(), None);
        self.leave_due.resize(members.len(), None);
        self.served.resize(members.len(), None);
        for ((since, due), member) in self
            .leaving_since
            .iter_mut()
            .zip(self.leave_due.iter_mut())
            .zip(members)
        {
            if member.status != MemberStatus::Leaving {
                *since = None;
                *due = None;
            } else if since.is_none() {
                *since = Some(now);
                *due = Some(now.plus(wait));
            }
        }
        let life_of = |index: u16| lives.get(usize::from(index)).copied().unwrap_or(0);
        self.unacked.retain(|ack| life_of(ack.to) == ack.life);
        let lost_after = successor(heard.get(self.me).copied().unwrap_or_default());
        if regain.is_some() {
            self.regain_due = Some(now.plus(wait));
        }
        let regain_floor = self.regain_floor;
        let mut reopened = false;
        let until = now.plus(self.limits.hold.min(max_hold));
        let mut moved: Vec<Hold> = Vec::new();
        let mut lost = SlotSet::default();
        let mut returned = SlotSet::default();
        let mut kept = SlotSet::default();
        let mut gained_from: Vec<(u16, u32)> = Vec::new();
        for (slot, inflow) in Slot::all().zip(self.inflow.iter_mut()) {
            let mine = next
                .owner(slot)
                .is_some_and(|owner| usize::from(owner.index) == self.me);
            let had = previous.filter(|view| view.owns(slot));
            *inflow = match (had, mine) {
                (None, true) => {
                    self.owned.insert(slot);
                    if self.intact.contains(slot) {
                        kept.insert(slot);
                    } else if self.held.contains(slot) {
                        returned.insert(slot);
                    }
                    let after = match *inflow {
                        Inflow::Lost { after } => after.max(announced),
                        _ => announced,
                    };
                    previous
                        .and_then(|view| origin(view.placement(), slot, members, self.me))
                        .map_or(Inflow::None, |origin| match origin {
                            Origin::Member(from) => gain(from, members, life_of(from), after),
                            Origin::Lost(from) => Inflow::Open { from },
                        })
                }
                (Some(_), false) => {
                    self.owned.remove(slot);
                    lost.insert(slot);
                    let end = ends.get(slot.index()).copied().unwrap_or(Nanos::MAX);
                    let granted = end <= now;
                    match next.owner(slot) {
                        Some(owner)
                            if end != Nanos::MAX
                                && (!self.gated.contains(slot) || self.kept.contains(slot)) =>
                        {
                            match moved
                                .iter_mut()
                                .find(|hold| hold.to == owner.index && hold.granted == granted)
                            {
                                Some(hold) => {
                                    hold.slots.insert(slot);
                                    hold.quarantine = hold.quarantine.max(end);
                                }
                                None => moved.push(Hold {
                                    until,
                                    quarantine: end,
                                    to: owner.index,
                                    heard: heard
                                        .get(usize::from(owner.index))
                                        .copied()
                                        .unwrap_or_default(),
                                    granted,
                                    slots: [slot].into_iter().collect(),
                                }),
                            }
                        }
                        _ => self.doomed.insert(slot),
                    }
                    Inflow::Lost { after: lost_after }
                }
                (None, false) => *inflow,
                (Some(_), true) => match *inflow {
                    Inflow::Join { from, life, .. } if life_of(from) != life => {
                        Inflow::Open { from }
                    }
                    Inflow::Join { from, .. } => {
                        match lock_deaths.get(usize::from(from)).copied().flatten() {
                            Some(since) => Inflow::Orphan { from, since },
                            None => *inflow,
                        }
                    }
                    Inflow::Leave { from, life, .. }
                        if life_of(from) != life
                            || members.get(usize::from(from)).is_none_or(|member| {
                                member.liveness == Liveness::Dead
                                    || member.status != MemberStatus::Leaving
                            }) =>
                    {
                        self.run_outs += 1;
                        Inflow::Open { from }
                    }
                    Inflow::Regain { from, life, .. }
                        if life_of(from) != life
                            || lock_deaths
                                .get(usize::from(from))
                                .copied()
                                .flatten()
                                .is_some() =>
                    {
                        reopened = true;
                        Inflow::None
                    }
                    other if regain.is_some() && other.awaited_from().is_none() => next
                        .rendezvous_where(u64::from(slot.get()), |member| Some(member.id) != me)
                        .map_or(other, |owner| Inflow::Regain {
                            from: owner.index,
                            life: life_of(owner.index),
                            after: regain_floor,
                        }),
                    other => other,
                },
            };
            match inflow.awaited_from() {
                Some(source) => {
                    self.awaiting.insert(slot);
                    if (had.is_none() || matches!(*inflow, Inflow::Regain { .. }))
                        && !gained_from.contains(&source)
                    {
                        gained_from.push(source);
                    }
                }
                None => self.awaiting.remove(slot),
            }
        }
        self.reopened |= reopened;
        for source in &mut self.sources {
            source.data.subtract(&lost);
            source.data.intersect_with(&self.awaiting);
        }
        for (from, life) in gained_from {
            self.register(from, life, now);
        }
        if !kept.is_empty() {
            self.unhold(&kept);
            self.kept.union_with(&kept);
        }
        self.release(&returned);
        for hold in &moved {
            self.held.union_with(&hold.slots);
            if !lapsed {
                self.intact.union_with(&hold.slots);
            }
        }
        self.holds.append(&mut moved);
        if let Some(regain) = regain {
            for hold in &mut self.holds {
                hold.quarantine = hold.quarantine.max(regain);
            }
        }
        self.advance(now);
        if !fenced && mem::take(&mut self.reopened) {
            self.regain_floor = lost_after;
        }
    }

    pub fn tick(&mut self, now: Nanos) -> bool {
        now >= self.next_deadline() && self.advance(now)
    }

    fn advance(&mut self, now: Nanos) -> bool {
        let mut changed = false;
        for (from, due) in (0u16..).zip(self.leave_due.iter_mut()) {
            if due.is_none_or(|due| now < due) {
                continue;
            }
            *due = None;
            for (slot, inflow) in Slot::all().zip(self.inflow.iter_mut()) {
                if matches!(*inflow, Inflow::Leave { from: f, .. } if f == from) {
                    *inflow = Inflow::Open { from };
                    self.awaiting.remove(slot);
                    self.run_outs += 1;
                    changed = true;
                }
            }
        }
        if self.regain_due.is_some_and(|due| now >= due) {
            self.regain_due = None;
            for (slot, inflow) in Slot::all().zip(self.inflow.iter_mut()) {
                if matches!(*inflow, Inflow::Regain { .. }) {
                    *inflow = Inflow::None;
                    self.awaiting.remove(slot);
                    self.run_outs += 1;
                    self.reopened = true;
                    changed = true;
                }
            }
        }
        if changed {
            for source in &mut self.sources {
                source.data.intersect_with(&self.awaiting);
            }
        }
        let mut expired = SlotSet::default();
        let run_outs = &mut self.run_outs;
        self.holds.retain(|hold| {
            let keep = now < hold.until;
            if !keep {
                *run_outs += hold.slots.len() as u64;
                expired.union_with(&hold.slots);
            }
            keep
        });
        self.release(&expired);
        self.unacked.retain(|ack| now < ack.until);
        self.pull(now);
        let before = mem::take(&mut self.gated);
        self.settle();
        changed || before != self.gated
    }

    pub fn answer(
        &mut self,
        peer: PeerLink,
        batch: u32,
        slots: &SlotSet,
        since: (u32, u32),
        now: Nanos,
    ) -> Answer {
        let mut covered = slots.clone();
        covered.subtract(&self.owned);
        let mut data = SlotSet::default();
        let mut quarantine = Nanos::ZERO;
        if self.fenced {
            covered.subtract(&self.held);
        } else {
            let mut fresh = SlotSet::default();
            let mut fresh_quarantine = now.plus(self.quarantine);
            for hold in self.holds.iter().filter(|hold| {
                usize::from(hold.to) == peer.index
                    && now < hold.until
                    && hold.heard >= since
                    && hold.slots.intersects(&covered)
            }) {
                if hold.granted {
                    data.union_with(&hold.slots);
                    quarantine = quarantine.max(hold.quarantine);
                } else {
                    fresh.union_with(&hold.slots);
                    fresh_quarantine = fresh_quarantine.max(hold.quarantine);
                }
            }
            data.intersect_with(&covered);
            fresh.intersect_with(&covered);
            if data.is_empty() && !fresh.is_empty() {
                data = fresh;
                quarantine = fresh_quarantine;
            } else {
                covered.subtract(&fresh);
            }
        }
        self.intact.subtract(&covered);
        let mut empty = covered;
        empty.subtract(&data);
        let quarantine_ms =
            u32::try_from(quarantine.since(now).as_millis().saturating_add(1)).unwrap_or(u32::MAX);
        if let Some(served) = self.served.get_mut(peer.index) {
            *served = (!data.is_empty()).then(|| Served {
                batch,
                life: peer.incarnation,
                data: data.clone(),
            });
        }
        Answer {
            data,
            empty,
            quarantine_ms,
        }
    }

    pub fn acked(&mut self, peer: PeerLink, batch: u32) {
        let Some(served) = self.served.get_mut(peer.index).and_then(|served| {
            served.take_if(|served| served.batch == batch && served.life == peer.incarnation)
        }) else {
            return;
        };
        self.release(&served.data);
        self.settle();
    }

    pub fn disconnected(&mut self, member: usize, now: Nanos) {
        let at = now.plus(self.limits.retry);
        for source in self
            .sources
            .iter_mut()
            .filter(|source| usize::from(source.from) == member)
        {
            if source.state != Pulling::AfterPurge {
                source.state = Pulling::Due { at };
            }
        }
        for ack in self
            .unacked
            .iter_mut()
            .filter(|ack| usize::from(ack.to) == member)
        {
            ack.resend = Some(at);
        }
    }

    pub fn ready(&mut self, from: usize, batch: u32, answer: &Answer, now: Nanos) -> bool {
        let retry = now.plus(self.limits.retry);
        let check = now.plus(self.limits.reply);
        let walks = self.walks;
        let Some(source) = self.sources.iter_mut().find(|source| {
            usize::from(source.from) == from
                && matches!(source.state, Pulling::Sent { batch: sent, .. } if sent == batch)
        }) else {
            return false;
        };
        let (from, life) = (source.from, source.life);
        let awaited = Some((from, life));
        let mut regained = SlotSet::default();
        let mut reopened = false;
        source.data.clear();
        for slot in answer.data.iter() {
            let Some(inflow) = self.inflow.get_mut(slot.index()) else {
                continue;
            };
            if inflow.awaited_from() != awaited {
                continue;
            }
            match *inflow {
                Inflow::Regain { after, .. } => {
                    *inflow = Inflow::Join { from, life, after };
                    regained.insert(slot);
                    reopened = true;
                }
                _ if self.kept.contains(slot) => regained.insert(slot),
                _ => source.data.insert(slot),
            }
        }
        if !regained.is_empty() {
            source.data.clear();
        }
        source.state = if source.data.is_empty() {
            Pulling::Due { at: retry }
        } else {
            Pulling::Importing {
                batch,
                base: now.plus(Duration::from_millis(u64::from(answer.quarantine_ms))),
                entries: 0,
                seen: 0,
                walks,
                check,
            }
        };
        let mut opened = false;
        for slot in answer.empty.iter() {
            let Some(inflow) = self.inflow.get_mut(slot.index()) else {
                continue;
            };
            *inflow = match *inflow {
                current if current.awaited_from() != awaited => continue,
                Inflow::Regain { .. } => {
                    reopened = true;
                    Inflow::None
                }
                _ => Inflow::Open { from },
            };
            self.awaiting.remove(slot);
            opened = true;
        }
        self.reopened |= reopened;
        self.kept.subtract(&regained);
        let purge = !regained.is_empty();
        if purge {
            self.doomed.union_with(&regained);
        }
        if opened || purge {
            self.settle();
        }
        opened
    }

    #[inline]
    pub fn entry(&mut self, from: usize, batch: u32, slot: Slot) -> bool {
        self.sources
            .iter_mut()
            .find(|source| usize::from(source.from) == from)
            .is_some_and(|source| match &mut source.state {
                Pulling::Importing {
                    batch: importing,
                    entries,
                    ..
                } if *importing == batch => {
                    *entries = entries.wrapping_add(1);
                    source.data.contains(slot)
                }
                _ => false,
            })
    }

    pub fn end(&mut self, from: usize, batch: u32, count: u32, now: Nanos) -> bool {
        let retry = now.plus(self.limits.retry);
        let Some(source) = self
            .sources
            .iter_mut()
            .find(|source| usize::from(source.from) == from)
        else {
            return false;
        };
        let Pulling::Importing {
            batch: importing,
            base,
            entries,
            ..
        } = source.state
        else {
            return false;
        };
        if importing != batch {
            return false;
        }
        source.state = Pulling::Due { at: retry };
        let data = mem::take(&mut source.data);
        if entries != count {
            return false;
        }
        let (to, life) = (source.from, source.life);
        let awaited = Some((to, life));
        for slot in data.iter() {
            if let Some(inflow) = self.inflow.get_mut(slot.index())
                && inflow.awaited_from() == awaited
            {
                *inflow = Inflow::Handed { from: to, base };
                self.awaiting.remove(slot);
                self.reopened = true;
            }
        }
        self.unacked.retain(|ack| ack.to != to);
        self.unacked.push(Unacked {
            to,
            life,
            batch,
            until: now.plus(self.limits.hold),
            resend: None,
        });
        self.actions.push(HandoffAction::Ack {
            to: usize::from(to),
            batch,
        });
        self.settle();
        true
    }

    pub fn purged(&mut self) -> bool {
        self.walking = None;
        self.walks = self.walks.wrapping_add(1);
        for source in &mut self.sources {
            if source.state == Pulling::AfterPurge {
                source.state = Pulling::Due { at: Nanos::ZERO };
            }
        }
        let before = mem::take(&mut self.gated);
        self.settle();
        before != self.gated
    }

    fn pull(&mut self, now: Nanos) {
        let reply = self.limits.reply;
        let retry = self.limits.retry;
        let announced = self.announced;
        let dirty = self.dirty();
        let walking = self.walking.is_some();
        let walked = self.walks;
        let Handoffs {
            awaiting,
            inflow,
            sources,
            unacked,
            next_batch,
            actions,
            ..
        } = self;
        for ack in unacked.iter_mut() {
            if ack.resend.is_some_and(|at| now >= at) {
                ack.resend = None;
                actions.push(HandoffAction::Ack {
                    to: usize::from(ack.to),
                    batch: ack.batch,
                });
            }
        }
        let from_source = |slot: &Slot, source: &Source| {
            inflow
                .get(slot.index())
                .and_then(|inflow| inflow.awaited_from())
                == Some((source.from, source.life))
        };
        sources.retain_mut(|source| {
            let due = match &mut source.state {
                Pulling::Due { at } => now >= *at,
                Pulling::AfterPurge => false,
                Pulling::Sent { at, .. } => now >= at.plus(reply),
                Pulling::Importing {
                    entries,
                    seen,
                    walks,
                    check,
                    ..
                } => {
                    if now < *check {
                        false
                    } else if *entries == *seen && *walks == walked && !walking {
                        true
                    } else {
                        *seen = *entries;
                        *walks = walked;
                        *check = now.plus(reply);
                        false
                    }
                }
            };
            if !due {
                return matches!(
                    source.state,
                    Pulling::Importing { .. } | Pulling::AfterPurge
                ) || awaiting.iter().any(|slot| from_source(&slot, source));
            }
            if let Pulling::Importing { .. } = source.state {
                source.data.clear();
                source.state = Pulling::Due {
                    at: now.plus(retry),
                };
                return true;
            }
            let mut wanted: SlotSet = awaiting
                .iter()
                .filter(|slot| from_source(slot, source))
                .collect();
            if wanted.is_empty() {
                return false;
            }
            wanted.subtract(&dirty);
            if wanted.is_empty() {
                source.state = Pulling::AfterPurge;
                return true;
            }
            let since_of = |slot: Slot| {
                inflow
                    .get(slot.index())
                    .map_or(announced, |inflow| inflow.after().max(announced))
            };
            let since = wanted
                .iter()
                .map(since_of)
                .filter(|since| *since > source.since)
                .min()
                .or_else(|| wanted.iter().map(since_of).min())
                .unwrap_or(announced);
            let wanted: SlotSet = wanted
                .iter()
                .filter(|slot| since_of(*slot) == since)
                .collect();
            source.since = since;
            let batch = *next_batch;
            *next_batch = next_batch.wrapping_add(1).max(1);
            source.state = Pulling::Sent { batch, at: now };
            if let Some(ack) = unacked
                .iter_mut()
                .find(|ack| ack.to == source.from && ack.resend.is_some())
            {
                ack.resend = None;
                actions.push(HandoffAction::Ack {
                    to: usize::from(ack.to),
                    batch: ack.batch,
                });
            }
            actions.push(HandoffAction::Pull {
                to: usize::from(source.from),
                batch,
                slots: wanted,
                since,
            });
            true
        });
    }

    fn register(&mut self, from: u16, life: u32, now: Nanos) {
        match self.sources.iter_mut().find(|source| source.from == from) {
            Some(source) if source.life == life => {}
            Some(source) => {
                source.life = life;
                source.state = Pulling::Due { at: now };
                source.data.clear();
            }
            None => self.sources.push(Source {
                from,
                life,
                state: Pulling::Due { at: now },
                data: SlotSet::default(),
                since: (0, 0),
            }),
        }
    }

    fn release(&mut self, slots: &SlotSet) {
        if slots.is_empty() {
            return;
        }
        let released = self.unhold(slots);
        self.doomed.union_with(&released);
    }

    fn unhold(&mut self, slots: &SlotSet) -> SlotSet {
        let mut released = slots.clone();
        released.intersect_with(&self.held);
        self.held.subtract(&released);
        self.intact.subtract(&released);
        for hold in &mut self.holds {
            hold.slots.subtract(&released);
        }
        self.holds.retain(|hold| !hold.slots.is_empty());
        for served in &mut self.served {
            served.take_if(|served| {
                served.data.subtract(&released);
                served.data.is_empty()
            });
        }
        released
    }

    fn settle(&mut self) {
        if self.walking.is_none() && !self.doomed.is_empty() {
            let ready = mem::take(&mut self.doomed);
            self.actions.push(HandoffAction::Purge {
                slots: ready.clone(),
            });
            self.walking = Some(ready);
        }
        self.kept.intersect_with(&self.awaiting);
        let mut gated = self.dirty();
        gated.intersect_with(&self.owned);
        gated.union_with(&self.awaiting);
        self.gated = gated;
    }
}

fn gain(from: u16, members: &[Member], life: u32, after: (u32, u32)) -> Inflow {
    match members.get(usize::from(from)) {
        Some(member) if member.liveness == Liveness::Dead => Inflow::Open { from },
        Some(member) if member.status == MemberStatus::Leaving => {
            Inflow::Leave { from, life, after }
        }
        Some(member) if member.places() => Inflow::Join { from, life, after },
        _ => Inflow::Open { from },
    }
}

enum Origin {
    Member(u16),
    Lost(u16),
}

fn origin(previous: &Placement, slot: Slot, members: &[Member], me: usize) -> Option<Origin> {
    let owner = previous.owner(slot)?;
    if members
        .get(usize::from(owner.index))
        .is_none_or(|member| member.liveness != Liveness::Dead)
    {
        return Some(Origin::Member(owner.index));
    }
    let alive = |candidate: &Member| {
        !members
            .iter()
            .any(|member| member.id == candidate.id && member.liveness == Liveness::Dead)
    };
    Some(
        match previous.rendezvous_where(u64::from(slot.get()), alive) {
            Some(taker) if usize::from(taker.index) != me => Origin::Member(taker.index),
            _ => Origin::Lost(owner.index),
        },
    )
}

fn successor((incarnation, seq): (u32, u32)) -> (u32, u32) {
    match seq.checked_add(1) {
        Some(seq) => (incarnation, seq),
        None => (incarnation.saturating_add(1), 1),
    }
}

pub(crate) struct Change<'a> {
    pub(crate) previous: Option<&'a View>,
    pub(crate) next: &'a Placement,
    pub(crate) lock_deaths: &'a [Option<Nanos>],
    pub(crate) lives: &'a [u32],
    pub(crate) heard: &'a [(u32, u32)],
    pub(crate) announced: (u32, u32),
    pub(crate) ends: &'a [Nanos],
    pub(crate) fenced: bool,
    pub(crate) regain: Option<Nanos>,
    pub(crate) quarantine: Duration,
    pub(crate) max_hold: Duration,
}
