/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::{
    StreamStats, TAG_CACHE_INVALIDATE_ALL, Topic, Verify,
    ack::AckItem,
    backend::StreamBackend,
    frame::{AckEntry, MAX_NODE, Message},
};
use crate::{
    clock::{Clock, Window, duration_ns},
    stats::{add, bump},
};
use rand::{RngExt, SeedableRng, rngs::SmallRng};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

pub const STABLE_SUBSCRIPTION: Duration = Duration::from_secs(1);
const RETRY_INITIAL: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(5);

pub struct ReceiveContext<'a> {
    pub node: u16,
    pub clock: Clock,
    pub window: Window,
    pub stats: &'a StreamStats,
    pub verify: Verify,
    pub apply: Duration,
    pub catch_up: Duration,
    pub acks: Option<(mpsc::Sender<AckItem>, u64)>,
}

#[derive(Clone, Copy, Default)]
struct Peer {
    incarnation: Option<u16>,
    expected: u64,
}

#[derive(Clone, Copy)]
enum CatchUp {
    Resubscribe,
    Gap,
}

pub fn resubscribe_delay(retry: u32, rng: &mut SmallRng) -> Duration {
    (RETRY_INITIAL * 4 * (1 << retry.min(6)))
        .min(RETRY_MAX)
        .mul_f64(rng.random_range(0.75..=1.0))
}

pub async fn run(
    backend: &StreamBackend,
    ctx: ReceiveContext<'_>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut peers = vec![Peer::default(); usize::from(MAX_NODE) + 1].into_boxed_slice();
    let mut rng = SmallRng::seed_from_u64(u64::from(ctx.node) << 1);
    let mut retry = 0u32;
    let mut resubscribed = false;
    'subscribe: loop {
        match backend.subscribe(Topic::Events).await {
            Ok(mut subscription) => {
                if std::mem::replace(&mut resubscribed, true) {
                    ctx.catch_up(CatchUp::Resubscribe);
                }
                let subscribed_at = Instant::now();
                let mut received = false;
                loop {
                    tokio::select! {
                        message = subscription.next(|payload| ctx.receive(&mut peers, payload)) => match message {
                            Some(()) => received = true,
                            None => {
                                bump(&ctx.stats.counters.disconnects);
                                break;
                            }
                        },
                        _ = shutdown.changed() => break 'subscribe,
                    }
                }
                if received || subscribed_at.elapsed() >= STABLE_SUBSCRIPTION {
                    retry = 0;
                    continue;
                }
            }
            Err(err) => {
                bump(&ctx.stats.counters.subscribe_errors);
                if retry == 0 {
                    eprintln!("node {} subscribe: {err}", ctx.node);
                }
            }
        }
        match tokio::time::timeout(resubscribe_delay(retry, &mut rng), shutdown.changed()).await {
            Ok(_) => break,
            Err(_) => retry += 1,
        }
    }
}

impl ReceiveContext<'_> {
    fn catch_up(&self, reason: CatchUp) {
        match reason {
            CatchUp::Resubscribe => bump(&self.stats.counters.catch_up_resubscribe),
            CatchUp::Gap => bump(&self.stats.counters.catch_up_gap),
        }
        spin(self.catch_up);
    }

    fn receive(&self, peers: &mut [Peer], payload: &[u8]) {
        let now = self.clock.now();
        let counters = &self.stats.counters;
        let Ok(Message::Events { header, events }) = Message::decode(payload) else {
            bump(&counters.invalid_messages);
            return;
        };
        let origin = header.origin;
        if origin.node == self.node {
            bump(&counters.own_skipped);
            return;
        }
        let Some(peer) = peers.get_mut(usize::from(origin.node)) else {
            bump(&counters.invalid_messages);
            return;
        };
        match peer.incarnation {
            Some(incarnation) if incarnation == origin.incarnation => {
                if header.msg_seq == peer.expected {
                    peer.expected += 1;
                } else if header.msg_seq > peer.expected {
                    bump(&counters.gaps);
                    add(&counters.gap_messages, header.msg_seq - peer.expected);
                    peer.expected = header.msg_seq + 1;
                    self.catch_up(CatchUp::Gap);
                } else {
                    bump(&counters.duplicates);
                }
            }
            known => {
                if known.is_some() {
                    bump(&counters.publisher_restarts);
                }
                peer.incarnation = Some(origin.incarnation);
                peer.expected = header.msg_seq + 1;
            }
        }
        if header.count == 0 {
            bump(&counters.end_markers_received);
            return;
        }
        let in_window = self.window.contains(header.sent_ns);
        if in_window {
            match now.checked_sub(header.sent_ns) {
                Some(latency) => self.stats.oneway.record(latency),
                None => bump(&counters.oneway_negative),
            }
            bump(&counters.messages_received);
            add(&counters.bytes_received, payload.len() as u64);
        }
        let mut received = 0u64;
        for event in events {
            let Ok(event) = event else {
                bump(&counters.invalid_messages);
                break;
            };
            received += 1;
            if self.verify == Verify::Full && !event.verify(origin) {
                bump(&counters.verify_failures);
            }
            if event.tag == TAG_CACHE_INVALIDATE_ALL {
                bump(&counters.invalidate_all_received);
            }
            spin(self.apply);
        }
        if in_window {
            add(&counters.events_received, received);
        }
        if let Some((acks, every)) = &self.acks
            && header.msg_seq.is_multiple_of(*every)
        {
            let item = AckItem {
                origin: origin.node,
                entry: AckEntry {
                    msg_seq: header.msg_seq,
                    sent_ns: header.sent_ns,
                    recv_ns: now,
                },
            };
            match acks.try_send(item) {
                Ok(()) => bump(&counters.acks_queued),
                Err(_) => bump(&counters.acks_dropped),
            }
        }
    }
}

#[inline]
pub fn spin(duration: Duration) {
    if duration.is_zero() {
        return;
    }
    let target = duration_ns(duration);
    let start = Instant::now();
    while duration_ns(start.elapsed()) < target {
        std::hint::spin_loop();
    }
}
