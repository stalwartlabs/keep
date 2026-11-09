/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::io;
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use keep_cluster::{
    FenceState, HandoffLimits, Handoffs, Lease, LockLimits, Nanos as ClusterNanos, Ownership,
    Roster, Start, View,
};
use keep_proto::Member;
use keep_rt::clock::{Nanos, SuspendMark};

#[inline]
pub fn cluster_time(now: Nanos) -> ClusterNanos {
    ClusterNanos(now.raw())
}

#[inline]
pub fn rt_time(at: ClusterNanos) -> Nanos {
    Nanos::from_raw(at.0)
}

#[derive(Debug)]
pub struct Snapshot {
    pub view: Arc<View>,
    pub topology: Box<[u8]>,
}

impl Snapshot {
    pub fn new(view: Arc<View>) -> Snapshot {
        let mut topology = Vec::new();
        view.topology().encode(&mut topology);
        Snapshot {
            view,
            topology: topology.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn epoch(&self) -> u64 {
        self.view.epoch()
    }
}

#[derive(Debug)]
pub struct Published {
    epoch: AtomicU64,
    current: Mutex<Arc<Snapshot>>,
    lease: Lease,
    suspend: SuspendMark,
}

impl Published {
    pub fn new(first: Snapshot) -> Published {
        Published {
            epoch: AtomicU64::new(first.epoch()),
            current: Mutex::new(Arc::new(first)),
            lease: Lease::new(),
            suspend: SuspendMark::default(),
        }
    }

    pub fn alone(member: Member, limits: &LockLimits, now: Nanos) -> io::Result<Published> {
        Published::single(member, limits, Start::Bootstrap, now)
    }

    pub fn single(
        member: Member,
        limits: &LockLimits,
        start: Start,
        now: Nanos,
    ) -> io::Result<Published> {
        let mut ownership = Ownership::new(member.id, start, limits);
        let mut handoffs = Handoffs::new(0, HandoffLimits::DEFAULT);
        let view = ownership
            .next(
                Roster {
                    members: vec![member],
                    lock_deaths: &[None],
                    lives: &[1],
                    heard: &[(0, 0)],
                    announced: (0, 0),
                },
                cluster_time(now),
                FenceState::default(),
                &mut handoffs,
            )
            .map_err(io::Error::other)?
            .ok_or_else(|| io::Error::other("the first view is empty"))?;
        let published = Published::new(Snapshot::new(view));
        published
            .lease
            .publish(ClusterNanos::MAX, ownership.floor());
        Ok(published)
    }

    pub fn publish(&self, snapshot: Snapshot) {
        let epoch = snapshot.epoch();
        let snapshot = Arc::new(snapshot);
        let old = mem::replace(
            &mut *self.current.lock().unwrap_or_else(PoisonError::into_inner),
            snapshot,
        );
        self.epoch.store(epoch, Ordering::Release);
        drop(old);
    }

    #[inline]
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub fn current(&self) -> Arc<Snapshot> {
        self.current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    #[inline]
    pub fn lease(&self) -> &Lease {
        &self.lease
    }

    #[inline]
    pub fn suspend(&self) -> &SuspendMark {
        &self.suspend
    }

    pub fn publish_turn(&self, until: ClusterNanos, floor: u64, raw: Nanos, boot: u64) {
        self.lease.publish(until, floor);
        self.suspend.publish(raw, boot);
    }

    #[inline]
    pub fn sendable(&self, raw: Nanos, boot: Option<u64>, epoch: u64) -> bool {
        !boot.is_some_and(|boot| self.suspend.passed(raw, boot))
            && self.lease.valid(cluster_time(raw), epoch)
    }
}
