/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use keep_proto::{
    Liveness, Member, MemberStatus, NodeId, NodeSalt, Placement, PlacementError, SLOT_COUNT, Slot,
    Topology, TopologyError,
};

use crate::fence::{FenceState, SelfFence};
use crate::handoff::{Change, Handoffs, Inflow};
use crate::locks::LockLimits;
use crate::slots::SlotSet;
use crate::time::Nanos;
use crate::view::View;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Start {
    Bootstrap,
    Rejoin,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ViewError {
    Topology(TopologyError),
    Placement(PlacementError),
}

impl fmt::Display for ViewError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ViewError::Topology(error) => error.fmt(f),
            ViewError::Placement(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ViewError {}

impl From<TopologyError> for ViewError {
    fn from(error: TopologyError) -> ViewError {
        ViewError::Topology(error)
    }
}

impl From<PlacementError> for ViewError {
    fn from(error: PlacementError) -> ViewError {
        ViewError::Placement(error)
    }
}

#[derive(Clone, Debug)]
pub struct Roster<'a> {
    pub members: Vec<Member>,
    pub lock_deaths: &'a [Option<Nanos>],
    pub lives: &'a [u32],
    pub heard: &'a [(u32, u32)],
    pub announced: (u32, u32),
}

#[derive(Debug)]
pub struct Ownership {
    me: NodeId,
    salt: NodeSalt,
    start: Start,
    quarantine: Duration,
    max_hold: Duration,
    epoch: u64,
    floor: u64,
    current: Option<Arc<View>>,
    placed: bool,
    server_quarantine: Nanos,
    base: Box<[Nanos]>,
    carried: Box<[Nanos]>,
    ranks: Vec<(NodeSalt, NodeId, Nanos)>,
    above: Vec<usize>,
}

impl Ownership {
    pub fn new(me: NodeId, start: Start, limits: &LockLimits) -> Ownership {
        Ownership {
            me,
            salt: NodeSalt::of(me),
            start,
            quarantine: limits.quarantine(),
            max_hold: limits.max_hold(),
            epoch: 0,
            floor: 0,
            current: None,
            placed: false,
            server_quarantine: Nanos::ZERO,
            base: vec![Nanos::ZERO; SLOT_COUNT].into_boxed_slice(),
            carried: vec![Nanos::ZERO; SLOT_COUNT].into_boxed_slice(),
            ranks: Vec::new(),
            above: Vec::new(),
        }
    }

    pub fn current(&self) -> Option<&Arc<View>> {
        self.current.as_ref()
    }

    pub fn floor(&self) -> u64 {
        self.floor
    }

    pub fn lease(&self, fence: &SelfFence) -> (Nanos, u64) {
        let state = fence.state();
        let accounted = self
            .current
            .as_deref()
            .is_some_and(|view| view.accounted == Some(state.lapses));
        let until = if accounted {
            fence.lease_end()
        } else {
            Nanos::ZERO
        };
        (until, self.floor)
    }

    pub fn next(
        &mut self,
        roster: Roster<'_>,
        now: Nanos,
        fence: FenceState,
        handoffs: &mut Handoffs,
    ) -> Result<Option<Arc<View>>, ViewError> {
        let Roster {
            members,
            lock_deaths,
            lives,
            heard,
            announced,
        } = roster;
        let epoch = self.epoch + 1;
        let quarantine_length = self.quarantine;
        self.ranks.clear();
        self.above.clear();
        for (index, member) in members.iter().enumerate() {
            let lock_end = lock_deaths
                .get(index)
                .copied()
                .flatten()
                .map_or(Nanos::MAX, |since| since.plus(quarantine_length));
            self.ranks
                .push((NodeSalt::of(member.id), member.id, lock_end));
            if member.liveness == Liveness::Dead && member.status != MemberStatus::Leaving {
                self.above.push(index);
            }
        }
        let placement = Placement::new(Topology::new(epoch, members)?)?;
        let previous = self.current.as_deref();
        let dependencies = Dependencies {
            me: (self.salt, self.me),
            ranks: &self.ranks,
            above: &self.above,
            quarantine: quarantine_length,
        };
        for slot in previous.iter().flat_map(|view| view.owned().iter()) {
            if placement
                .owner(slot)
                .is_some_and(|owner| owner.member.id == self.me)
            {
                continue;
            }
            if let (Some(carried), Some(base)) = (
                self.carried.get_mut(slot.index()),
                self.base.get(slot.index()),
            ) {
                *carried = dependencies.end(slot, *base, handoffs.inflow(slot));
            }
        }
        let fenced = fence.fenced();
        let regain = !fenced && previous.is_some_and(|view| view.accounted != Some(fence.lapses));
        let gained = now.plus(self.quarantine);
        handoffs.plan(
            Change {
                previous,
                next: &placement,
                lock_deaths,
                lives,
                heard,
                announced,
                ends: &self.carried,
                fenced,
                regain: regain.then_some(gained),
                quarantine: quarantine_length,
                max_hold: self.max_hold,
            },
            now,
        );
        let accounted = match previous {
            Some(view) if fenced => view.accounted,
            None if fenced && self.start == Start::Rejoin => None,
            _ => Some(fence.lapses),
        };
        let everyone = placement.topology().members().iter().all(Member::places);
        let first = match self.start {
            Start::Bootstrap if everyone => Nanos::ZERO,
            Start::Bootstrap | Start::Rejoin => gained,
        };
        let placing = |placement: &Placement| {
            placement
                .topology()
                .members()
                .iter()
                .any(|member| member.id == self.me && member.places())
        };
        let places = placing(&placement);
        if regain {
            self.server_quarantine = self.server_quarantine.max(gained);
        }
        if places && !previous.is_some_and(|view| placing(view.placement())) {
            let floor = if self.placed { gained } else { first };
            self.server_quarantine = self.server_quarantine.max(floor);
        }
        let mut owned = SlotSet::default();
        let mut quarantine = vec![Nanos::ZERO; SLOT_COUNT].into_boxed_slice();
        for ((slot, end), base) in Slot::all()
            .zip(quarantine.iter_mut())
            .zip(self.base.iter_mut())
        {
            if !placement
                .owner(slot)
                .is_some_and(|owner| owner.member.id == self.me)
            {
                *base = Nanos::ZERO;
                continue;
            }
            owned.insert(slot);
            let inflow = handoffs.inflow(slot);
            *base = if regain {
                gained
            } else if let Inflow::Handed { base: sent, .. } = inflow {
                sent.max(self.server_quarantine)
            } else if previous.is_some_and(|view| view.owns(slot)) {
                *base
            } else if !self.placed || previous.is_none() {
                first
            } else {
                gained
            };
            *end = dependencies.end(slot, *base, inflow);
        }
        let mut serving = if fenced {
            SlotSet::default()
        } else {
            owned.clone()
        };
        serving.subtract(handoffs.gated());
        let view = View {
            epoch,
            me: self.me,
            placement,
            owned,
            serving,
            gated: handoffs.gated().clone(),
            quarantine,
            fenced,
            accounted,
        };
        if previous.is_some_and(|current| current.same_state(&view)) {
            return Ok(None);
        }
        if previous
            .is_some_and(|current| current.accounted != view.accounted || view.restricts(current))
        {
            self.floor = epoch;
        }
        self.epoch = epoch;
        self.placed |= !view.owned.is_empty();
        let view = Arc::new(view);
        self.current = Some(Arc::clone(&view));
        Ok(Some(view))
    }
}

struct Dependencies<'a> {
    me: (NodeSalt, NodeId),
    ranks: &'a [(NodeSalt, NodeId, Nanos)],
    above: &'a [usize],
    quarantine: Duration,
}

impl Dependencies<'_> {
    fn end(&self, slot: Slot, base: Nanos, inflow: Inflow) -> Nanos {
        let mut end = base;
        if !self.above.is_empty() {
            let point = u64::from(slot.get());
            let (salt, me) = self.me;
            let mut threshold = (salt.score(point), me);
            if let Some((salt, id, _)) = inflow
                .source()
                .and_then(|from| self.ranks.get(usize::from(from)))
            {
                threshold = threshold.min((salt.score(point), *id));
            }
            end = self
                .above
                .iter()
                .filter_map(|index| self.ranks.get(*index))
                .filter(|(salt, id, _)| {
                    let score = salt.score(point);
                    score > threshold.0 || (score == threshold.0 && *id < threshold.1)
                })
                .fold(base, |end, (_, _, lock_end)| end.max(*lock_end));
        }
        if let Inflow::Orphan { since, .. } = inflow {
            end = end.max(since.plus(self.quarantine));
        }
        end
    }
}
