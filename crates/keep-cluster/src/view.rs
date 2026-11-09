/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use keep_proto::{NodeId, Placement, Slot, Topology};

use crate::slots::SlotSet;
use crate::time::Nanos;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub enum Gate {
    #[default]
    Open,
    Awaiting,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Route {
    Serve,
    Fenced,
    Busy,
    Moved { owner: u16 },
}

#[derive(Clone, Debug)]
pub struct View {
    pub(crate) epoch: u64,
    pub(crate) me: NodeId,
    pub(crate) placement: Placement,
    pub(crate) owned: SlotSet,
    pub(crate) serving: SlotSet,
    pub(crate) gated: SlotSet,
    pub(crate) quarantine: Box<[Nanos]>,
    pub(crate) fenced: bool,
    pub(crate) accounted: Option<u64>,
}

impl View {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn me(&self) -> NodeId {
        self.me
    }

    pub fn fenced(&self) -> bool {
        self.fenced
    }

    pub fn topology(&self) -> &Topology {
        self.placement.topology()
    }

    pub fn placement(&self) -> &Placement {
        &self.placement
    }

    #[inline]
    pub fn serves(&self, slot: Slot) -> bool {
        self.serving.contains(slot)
    }

    pub fn route(&self, slot: Slot) -> Route {
        if self.serving.contains(slot) {
            return Route::Serve;
        }
        if self.fenced {
            return Route::Fenced;
        }
        if self.owned.contains(slot) {
            return Route::Busy;
        }
        self.placement
            .owner(slot)
            .map_or(Route::Busy, |owner| Route::Moved { owner: owner.index })
    }

    #[inline]
    pub fn lock_quarantined(&self, slot: Slot, now: Nanos) -> bool {
        self.quarantine
            .get(slot.index())
            .is_some_and(|end| now < *end)
    }

    pub fn quarantine_end(&self, slot: Slot) -> Nanos {
        self.quarantine
            .get(slot.index())
            .copied()
            .unwrap_or_default()
    }

    #[inline]
    pub fn owns(&self, slot: Slot) -> bool {
        self.owned.contains(slot)
    }

    pub fn owned(&self) -> &SlotSet {
        &self.owned
    }

    pub fn gate(&self, slot: Slot) -> Gate {
        if self.gated.contains(slot) {
            Gate::Awaiting
        } else {
            Gate::Open
        }
    }

    pub(crate) fn restricts(&self, before: &View) -> bool {
        !before.serving.is_subset(&self.serving)
            || self
                .quarantine
                .iter()
                .zip(before.quarantine.iter())
                .zip(Slot::all())
                .any(|((now, then), slot)| now > then && before.serving.contains(slot))
    }

    pub(crate) fn same_state(&self, other: &View) -> bool {
        self.topology().members() == other.topology().members()
            && self.fenced == other.fenced
            && self.accounted == other.accounted
            && self.gated == other.gated
            && self.quarantine == other.quarantine
    }
}
