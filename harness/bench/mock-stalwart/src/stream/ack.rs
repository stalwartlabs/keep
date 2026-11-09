/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::{
    StreamStats, Topic,
    backend::{AckPublisher, StreamBackend},
    frame::{AckEntry, Message, begin_acks, write_ack},
    subscriber::{STABLE_SUBSCRIPTION, resubscribe_delay},
};
use crate::{
    clock::{Clock, Window},
    stats::{add, bump},
};
use rand::{SeedableRng, rngs::SmallRng};
use std::{
    sync::{Mutex, PoisonError},
    time::Instant,
};
use tokio::sync::{mpsc, watch};

const RING: usize = 1 << 16;
const ACK_BATCH: usize = 1024;

#[derive(Clone, Copy, Debug)]
pub struct AckItem {
    pub origin: u16,
    pub entry: AckEntry,
}

#[derive(Clone, Copy)]
struct Slot {
    seq: u64,
    sent: u64,
    oldest: u64,
    acks: u32,
    in_window: bool,
}

const EMPTY: Slot = Slot {
    seq: u64::MAX,
    sent: 0,
    oldest: 0,
    acks: 0,
    in_window: false,
};

pub struct Tracker {
    ring: Mutex<Box<[Slot]>>,
    expected: u32,
    every: u64,
}

impl Tracker {
    pub fn new(expected: u32, every: u64) -> Self {
        Tracker {
            ring: Mutex::new(vec![EMPTY; RING].into_boxed_slice()),
            expected,
            every: every.max(1),
        }
    }

    pub fn sent(&self, seq: u64, sent: u64, oldest: u64, in_window: bool, stats: &StreamStats) {
        if !seq.is_multiple_of(self.every) || self.expected == 0 {
            return;
        }
        let mut ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
        let slot = &mut ring[(seq / self.every) as usize % RING];
        if slot.seq == seq {
            slot.sent = sent;
            slot.oldest = oldest;
            slot.in_window = in_window;
            return;
        }
        if slot.seq != u64::MAX && slot.in_window && slot.acks < self.expected {
            bump(&stats.counters.deliveries_incomplete);
        }
        *slot = Slot {
            seq,
            sent,
            oldest,
            acks: 0,
            in_window,
        };
    }

    pub fn ack(&self, seq: u64, now: u64, stats: &StreamStats) {
        let mut ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
        let slot = &mut ring[(seq / self.every) as usize % RING];
        if slot.seq != seq {
            bump(&stats.counters.acks_unmatched);
            return;
        }
        slot.acks += 1;
        if slot.acks == self.expected && slot.in_window {
            stats.echo_last.record(now.saturating_sub(slot.sent));
            stats.event_age_last.record(now.saturating_sub(slot.oldest));
            bump(&stats.counters.deliveries_complete);
        }
    }

    pub fn finish(&self, stats: &StreamStats) {
        let ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
        let incomplete = ring
            .iter()
            .filter(|slot| slot.seq != u64::MAX && slot.in_window && slot.acks < self.expected)
            .count();
        add(&stats.counters.deliveries_incomplete, incomplete as u64);
    }
}

pub async fn run_acker(
    mut publisher: AckPublisher,
    mut rx: mpsc::Receiver<AckItem>,
    node: u16,
    stats: &StreamStats,
) {
    let mut items = Vec::with_capacity(ACK_BATCH);
    let mut payload = Vec::with_capacity(ACK_BATCH * 26);
    while rx.recv_many(&mut items, ACK_BATCH).await > 0 {
        items.sort_unstable_by_key(|item| item.origin);
        for group in items.chunk_by(|left, right| left.origin == right.origin) {
            let Some(first) = group.first() else {
                continue;
            };
            begin_acks(&mut payload, node, first.origin, group.len() as u32);
            for item in group {
                write_ack(&mut payload, &item.entry);
            }
            match publisher.publish(first.origin, &mut payload).await {
                Ok(()) => bump(&stats.counters.ack_messages_sent),
                Err(_) => bump(&stats.counters.ack_publish_errors),
            }
        }
        items.clear();
    }
}

pub struct AckReceiver<'a> {
    pub node: u16,
    pub clock: Clock,
    pub window: Window,
    pub stats: &'a StreamStats,
    pub tracker: &'a Tracker,
}

pub async fn run_receiver(
    backend: &StreamBackend,
    receiver: AckReceiver<'_>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut rng = SmallRng::seed_from_u64(u64::from(receiver.node));
    let mut retry = 0u32;
    loop {
        if let Ok(mut subscription) = backend.subscribe(Topic::Ack(receiver.node)).await {
            let subscribed_at = Instant::now();
            let mut received = false;
            loop {
                tokio::select! {
                    message = subscription.next(|payload| receiver.receive(payload)) => match message {
                        Some(()) => received = true,
                        None => break,
                    },
                    _ = shutdown.changed() => return,
                }
            }
            if received || subscribed_at.elapsed() >= STABLE_SUBSCRIPTION {
                retry = 0;
                continue;
            }
        }
        match tokio::time::timeout(resubscribe_delay(retry, &mut rng), shutdown.changed()).await {
            Ok(_) => return,
            Err(_) => retry += 1,
        }
    }
}

impl AckReceiver<'_> {
    fn receive(&self, payload: &[u8]) {
        let now = self.clock.now();
        let Ok(Message::Acks { origin, entries }) = Message::decode(payload) else {
            bump(&self.stats.counters.invalid_messages);
            return;
        };
        if origin != self.node {
            return;
        }
        for entry in entries {
            let Ok(entry) = entry else {
                bump(&self.stats.counters.invalid_messages);
                return;
            };
            bump(&self.stats.counters.acks_received);
            if self.window.contains(entry.sent_ns) {
                self.stats.echo.record(now.saturating_sub(entry.sent_ns));
            }
            self.tracker.ack(entry.msg_seq, now, self.stats);
        }
    }
}
