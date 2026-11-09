/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::num::NonZeroU16;
use std::sync::Arc;

use keep_cluster::{Gate as SlotGate, Nanos as ClusterNanos, Route};
use keep_engine::Gate;
use keep_proto::{Command, KeyHash, KeyHasher, Response, Slot, Status, Value};
use keep_rt::clock::Nanos;

use crate::cluster::{Published, Snapshot};

#[derive(Clone, Debug)]
pub(super) struct Router {
    hasher: KeyHasher,
    shards: NonZeroU16,
    me: u16,
}

impl Router {
    pub(super) fn new(hasher: KeyHasher, shards: NonZeroU16, me: u16) -> Router {
        Router { hasher, shards, me }
    }

    #[inline]
    pub(super) fn hash(&self, key: &[u8]) -> KeyHash {
        self.hasher.hash(key)
    }

    #[inline]
    pub(super) fn shard_of(&self, hash: KeyHash) -> u16 {
        if self.shards.get() == 1 {
            0
        } else {
            hash.shard(self.shards)
        }
    }

    #[inline]
    pub(super) fn me(&self) -> u16 {
        self.me
    }

    pub(super) fn others(&self) -> impl Iterator<Item = usize> + use<> {
        let me = self.me;
        (0..self.shards.get())
            .filter(move |&shard| shard != me)
            .map(usize::from)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Admit {
    Serve(Gate),
    Fenced,
    Busy,
    Moved { owner: u16, epoch: u64 },
}

pub(super) struct Placement {
    published: Arc<Published>,
    snapshot: Arc<Snapshot>,
    epoch: u64,
    announced: u64,
}

impl Placement {
    pub(super) fn new(published: Arc<Published>) -> Placement {
        let snapshot = published.current();
        let epoch = snapshot.epoch();
        Placement {
            published,
            snapshot,
            epoch,
            announced: epoch,
        }
    }

    #[inline]
    pub(super) fn refresh(&mut self) {
        if self.published.epoch() != self.epoch {
            self.adopt();
        }
    }

    #[cold]
    #[inline(never)]
    fn adopt(&mut self) {
        self.snapshot = self.published.current();
        self.epoch = self.snapshot.epoch();
    }

    #[inline]
    pub(super) fn epoch(&self) -> u64 {
        self.epoch
    }

    #[inline]
    pub(super) fn serving(&self, now: ClusterNanos) -> bool {
        self.valid(now, self.epoch)
    }

    #[inline]
    pub(super) fn valid(&self, now: ClusterNanos, epoch: u64) -> bool {
        self.published.lease().valid(now, epoch)
    }

    #[inline]
    pub(super) fn sendable(&self, now: Nanos, boot: Option<u64>, epoch: u64) -> bool {
        self.published.sendable(now, boot, epoch)
    }

    pub(super) fn announce(&mut self) -> Option<u64> {
        let epoch = self.epoch;
        (epoch != self.announced).then(|| {
            self.announced = epoch;
            epoch
        })
    }

    pub(super) fn importable(&self, slot: Slot) -> bool {
        let view = &self.snapshot.view;
        view.owns(slot) && view.gate(slot) == SlotGate::Awaiting
    }

    pub(super) fn topology(&self) -> &[u8] {
        &self.snapshot.topology
    }

    #[inline]
    pub(super) fn admit(&self, slot: Slot, command: &Command<'_>, now: ClusterNanos) -> Admit {
        let view = &self.snapshot.view;
        match view.route(slot) {
            Route::Serve => {
                let quarantined =
                    matches!(command, Command::TryLock { .. }) && view.lock_quarantined(slot, now);
                Admit::Serve(if quarantined {
                    Gate::Quarantined
                } else {
                    Gate::Open
                })
            }
            Route::Fenced => Admit::Fenced,
            Route::Busy => Admit::Busy,
            Route::Moved { owner } => Admit::Moved {
                owner,
                epoch: view.epoch(),
            },
        }
    }
}

#[cold]
#[inline(never)]
pub(super) fn moved(out: &mut Vec<u8>, id: u32, owner: u16, epoch: u64) {
    let epoch = epoch.to_le_bytes();
    Response {
        id,
        status: Status::Moved,
        arg: u32::from(owner),
        value: Value::new(&epoch).unwrap_or(Value::EMPTY),
    }
    .encode(out);
}
