/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

mod hasher;
mod slots;

use std::fmt;

pub use hasher::{ClusterSecret, FINGERPRINT_LABEL, KeyHash, KeyHasher};
pub use slots::{NodeSalt, SLOT_BITS, SLOT_COUNT, Slot, fmix64, hrw_pick};

use crate::limits::MAX_MEMBERS;
use crate::topology::{Member, NodeId, Topology};
use crate::types::Topic;

/// Key to server and shard routing over a topology's placing members: slots by rendezvous hashing,
/// the shard from the key hash bits. With no placing member every slot is unowned.
#[derive(Clone, Debug)]
pub struct Placement {
    topology: Topology,
    owners: Box<[u16; SLOT_COUNT]>,
    salts: Box<[NodeSalt]>,
    placing: Box<[u16]>,
}

/// A slot owner and its index in the topology's member list, the `arg` of
/// `Moved`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Owner<'a> {
    /// The owner's index in the member list.
    pub index: u16,
    /// The owner.
    pub member: &'a Member,
}

/// A topology that cannot be placed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PlacementError {
    /// Two members share this node id.
    DuplicateNode(NodeId),
}

impl fmt::Display for PlacementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlacementError::DuplicateNode(node) => write!(f, "node id {} listed twice", node.0),
        }
    }
}

impl std::error::Error for PlacementError {}

const UNOWNED: u16 = u16::MAX;

const _: () = assert!(MAX_MEMBERS < UNOWNED as usize);

impl Placement {
    /// Precomputes placement over a topology.
    pub fn new(topology: Topology) -> Result<Placement, PlacementError> {
        let mut placing: Vec<(NodeId, u16)> = topology
            .members()
            .iter()
            .zip(0u16..)
            .filter(|(member, _)| member.places())
            .map(|(member, index)| (member.id, index))
            .collect();
        placing.sort_unstable();
        if let Some(node) = placing.windows(2).find_map(|pair| match pair {
            [(a, _), (b, _)] if a == b => Some(*a),
            _ => None,
        }) {
            return Err(PlacementError::DuplicateNode(node));
        }
        let salts: Box<[NodeSalt]> = placing.iter().map(|(id, _)| NodeSalt::of(*id)).collect();
        let placing: Box<[u16]> = placing.into_iter().map(|(_, index)| index).collect();
        let mut owners = Box::new([UNOWNED; SLOT_COUNT]);
        for (slot, owner) in Slot::all().zip(owners.iter_mut()) {
            if let Some(index) =
                hrw_pick(u64::from(slot.get()), &salts).and_then(|winner| placing.get(winner))
            {
                *owner = *index;
            }
        }
        Ok(Placement {
            topology,
            owners,
            salts,
            placing,
        })
    }

    /// The topology placed over.
    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    /// The slot's owner; `None` only when no member places.
    #[inline]
    pub fn owner(&self, slot: Slot) -> Option<Owner<'_>> {
        let index = *self.owners.get(slot.index())?;
        let member = self.topology.members().get(usize::from(index))?;
        Some(Owner { index, member })
    }

    /// The home server of a stream topic: rendezvous over the placing members with the topic id as
    /// the point.
    pub fn home(&self, topic: Topic) -> Option<Owner<'_>> {
        self.rendezvous(u64::from(topic.get()))
    }

    /// The placing member with the highest rendezvous score for `point`.
    pub fn rendezvous(&self, point: u64) -> Option<Owner<'_>> {
        let index = *self.placing.get(hrw_pick(point, &self.salts)?)?;
        let member = self.topology.members().get(usize::from(index))?;
        Some(Owner { index, member })
    }

    /// The placing member with the highest rendezvous score for `point`
    /// among those `accept` takes: the next choice when better ones are
    /// unusable. Ties go the same way as in `rendezvous`.
    pub fn rendezvous_where(
        &self,
        point: u64,
        mut accept: impl FnMut(&Member) -> bool,
    ) -> Option<Owner<'_>> {
        let members = self.topology.members();
        let mut best: Option<(u64, Owner<'_>)> = None;
        for (salt, index) in self.salts.iter().zip(self.placing.iter()) {
            let Some(member) = members.get(usize::from(*index)) else {
                continue;
            };
            let score = salt.score(point);
            if best.is_none_or(|(top, _)| score > top) && accept(member) {
                best = Some((
                    score,
                    Owner {
                        index: *index,
                        member,
                    },
                ));
            }
        }
        best.map(|(_, owner)| owner)
    }

    /// The owner of the key with `hash` and the shard within it.
    #[inline]
    pub fn route(&self, hash: KeyHash) -> Option<(Owner<'_>, u16)> {
        let owner = self.owner(hash.slot())?;
        let shard = hash.shard(owner.member.shards);
        Some((owner, shard))
    }
}
