/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::{
    Event, MessageSpec, StreamError, StreamStats, TAG_STATE_CHANGE,
    ack::Tracker,
    backend::Publisher,
    backlog::{Backlog, MAX_BATCH_SIZE, refresh_slot},
    frame::{EventsHeader, Origin, begin_events, write_event},
};
use crate::{
    clock::{Clock, Window},
    stats::{add, bump},
};
use std::{collections::VecDeque, future::Future, sync::atomic::Ordering, time::Duration};
use tokio::sync::mpsc;

pub const IPC_CHANNEL_BUFFER: usize = 1024;
const STALWART_BATCH_HEADER: u32 = 2;
pub const MAX_PAYLOAD_BYTES: usize = 512 * 1024;
const RETRY_INITIAL: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(5);
const LOGGED_ERRORS: u64 = 10;

pub struct PublishContext<'a> {
    pub origin: Origin,
    pub clock: Clock,
    pub window: Window,
    pub stats: &'a StreamStats,
    pub tracker: Option<&'a Tracker>,
}

struct Output {
    payload: Vec<u8>,
    msg_seq: u64,
}

struct Intake<'x> {
    rx: &'x mut mpsc::Receiver<Event>,
    received: Vec<Event>,
    queued: VecDeque<Event>,
    backlog: Backlog,
    is_open: bool,
}

pub async fn run(mut publisher: Publisher, mut rx: mpsc::Receiver<Event>, ctx: PublishContext<'_>) {
    let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
    let mut output = Output {
        payload: Vec::with_capacity(4096),
        msg_seq: 1,
    };
    let mut intake = Intake {
        rx: &mut rx,
        received: Vec::with_capacity(MAX_BATCH_SIZE),
        queued: VecDeque::with_capacity(IPC_CHANNEL_BUFFER),
        backlog: Backlog::default(),
        is_open: true,
    };
    let mut backoff = RETRY_INITIAL;
    while intake.next_batch(&mut batch, ctx.stats).await {
        match intake
            .absorb(output.publish(&mut publisher, &mut batch, &ctx))
            .await
        {
            Ok(()) => {
                intake.backlog.release();
                batch.clear();
                backoff = RETRY_INITIAL;
            }
            Err(err) => {
                bump(&ctx.stats.counters.publish_errors);
                log_error(&ctx, &err);
                intake.absorb(tokio::time::sleep(backoff)).await;
                backoff = (backoff * 2).min(RETRY_MAX);
            }
        }
        add(
            &ctx.stats.counters.backlog_dropped,
            intake.backlog.take_dropped(),
        );
        add(
            &ctx.stats.counters.backlog_coalesced,
            intake.backlog.take_coalesced(),
        );
    }
    output.finish(&mut publisher, &ctx).await;
}

pub async fn run_messages(
    mut publisher: Publisher,
    mut rx: mpsc::Receiver<MessageSpec>,
    ctx: PublishContext<'_>,
) {
    let mut output = Output {
        payload: Vec::with_capacity(4096),
        msg_seq: 1,
    };
    let mut event_seq = 1u64;
    while let Some(spec) = rx.recv().await {
        let mut backoff = RETRY_INITIAL;
        loop {
            let sent_ns = ctx.clock.now();
            output.encode_spec(&spec, event_seq, &ctx, sent_ns);
            let bytes = output.payload.len();
            let in_window = ctx.window.contains(sent_ns);
            if let Some(tracker) = ctx.tracker {
                tracker.sent(output.msg_seq, sent_ns, spec.created, in_window, ctx.stats);
            }
            match publisher.publish(&mut output.payload).await {
                Ok(()) => {
                    let done = ctx.clock.now();
                    if in_window {
                        let stats = ctx.stats;
                        stats.publish.record(done - sent_ns);
                        stats.batch_events.record(u64::from(spec.count));
                        stats.batch_bytes.record(bytes as u64);
                        stats
                            .event_delay
                            .record(sent_ns.saturating_sub(spec.created));
                        bump(&stats.counters.messages_published);
                        add(&stats.counters.events_published, u64::from(spec.count));
                        add(&stats.counters.bytes_published, bytes as u64);
                    }
                    output.msg_seq += 1;
                    event_seq += u64::from(spec.count);
                    break;
                }
                Err(err) => {
                    bump(&ctx.stats.counters.publish_errors);
                    log_error(&ctx, &err);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(RETRY_MAX);
                }
            }
        }
    }
    output.finish(&mut publisher, &ctx).await;
}

impl Intake<'_> {
    async fn next_batch(&mut self, batch: &mut Vec<Event>, stats: &StreamStats) -> bool {
        if !batch.is_empty() {
            return true;
        }
        if !self.queued.is_empty() {
            let count = self.queued.len().min(MAX_BATCH_SIZE);
            batch.extend(self.queued.drain(..count));
            dedup_refreshes(batch, stats);
            return true;
        }
        if self.backlog.fill(batch) {
            return true;
        }
        if self.is_open && self.rx.recv_many(batch, MAX_BATCH_SIZE).await > 0 {
            dedup_refreshes(batch, stats);
            return true;
        }
        self.is_open = false;
        false
    }

    async fn absorb<T>(&mut self, until: impl Future<Output = T>) -> T {
        tokio::pin!(until);
        loop {
            tokio::select! {
                biased;
                result = &mut until => return result,
                count = self.rx.recv_many(&mut self.received, MAX_BATCH_SIZE), if self.is_open => {
                    if count == 0 {
                        self.is_open = false;
                    }
                    for event in self.received.drain(..) {
                        if self.queued.len() < IPC_CHANNEL_BUFFER && self.backlog.is_empty() {
                            self.queued.push_back(event);
                        } else {
                            self.backlog.push(event);
                        }
                    }
                }
            }
        }
    }
}

fn dedup_refreshes(batch: &mut Vec<Event>, stats: &StreamStats) {
    let mut seen = 0u8;
    let before = batch.len();
    batch.retain(|event| match refresh_slot(event.tag) {
        Some(slot) => {
            let bit = 1 << slot;
            let first = seen & bit == 0;
            seen |= bit;
            first
        }
        None => true,
    });
    add(
        &stats.counters.refreshes_deduplicated,
        (before - batch.len()) as u64,
    );
}

impl Output {
    async fn finish(&mut self, publisher: &mut Publisher, ctx: &PublishContext<'_>) {
        self.encode(&[], ctx, ctx.clock.now());
        match publisher.publish(&mut self.payload).await {
            Ok(()) => bump(&ctx.stats.counters.end_markers_sent),
            Err(err) => {
                bump(&ctx.stats.counters.publish_errors);
                log_error(ctx, &err);
            }
        }
    }

    async fn publish(
        &mut self,
        publisher: &mut Publisher,
        batch: &mut Vec<Event>,
        ctx: &PublishContext<'_>,
    ) -> Result<(), StreamError> {
        while !batch.is_empty() {
            let sent_ns = ctx.clock.now();
            let Some(count) = self.serialize(batch, ctx, sent_ns) else {
                continue;
            };
            let bytes = self.payload.len();
            let oldest = batch[..count]
                .iter()
                .filter(|event| event.created > 0)
                .map(|event| event.created)
                .min()
                .unwrap_or(sent_ns);
            let in_window = ctx.window.contains(sent_ns);
            if let Some(tracker) = ctx.tracker {
                tracker.sent(self.msg_seq, sent_ns, oldest, in_window, ctx.stats);
            }
            publisher.publish(&mut self.payload).await?;
            let done = ctx.clock.now();
            if in_window {
                let stats = ctx.stats;
                stats.publish.record(done - sent_ns);
                stats.batch_events.record(count as u64);
                stats.batch_bytes.record(bytes as u64);
                for event in batch[..count].iter().filter(|event| event.created > 0) {
                    stats
                        .event_delay
                        .record(sent_ns.saturating_sub(event.created));
                }
                bump(&stats.counters.messages_published);
                add(&stats.counters.events_published, count as u64);
                add(&stats.counters.bytes_published, bytes as u64);
            }
            self.msg_seq += 1;
            batch.drain(..count);
        }
        Ok(())
    }

    fn serialize(
        &mut self,
        batch: &mut Vec<Event>,
        ctx: &PublishContext<'_>,
        sent_ns: u64,
    ) -> Option<usize> {
        let mut count = batch.len();
        loop {
            self.encode(&batch[..count], ctx, sent_ns);
            if self.payload.len() <= MAX_PAYLOAD_BYTES {
                return Some(count);
            }
            if count == 1 {
                batch.remove(0);
                bump(&ctx.stats.counters.oversize_dropped);
                return None;
            }
            count /= 2;
        }
    }

    fn encode_spec(
        &mut self,
        spec: &MessageSpec,
        first_seq: u64,
        ctx: &PublishContext<'_>,
        sent_ns: u64,
    ) {
        begin_events(
            &mut self.payload,
            &EventsHeader {
                origin: ctx.origin,
                msg_seq: self.msg_seq,
                sent_ns,
                count: spec.count,
            },
        );
        let total = spec.bytes.saturating_sub(STALWART_BATCH_HEADER);
        let share = total / spec.count;
        for (offset, seq) in (first_seq..first_seq + u64::from(spec.count)).enumerate() {
            let size = match offset {
                0 => share + total % spec.count,
                _ => share,
            };
            write_event(
                &mut self.payload,
                ctx.origin,
                TAG_STATE_CHANGE,
                seq,
                size.max(1),
            );
        }
    }

    fn encode(&mut self, events: &[Event], ctx: &PublishContext<'_>, sent_ns: u64) {
        begin_events(
            &mut self.payload,
            &EventsHeader {
                origin: ctx.origin,
                msg_seq: self.msg_seq,
                sent_ns,
                count: events.len() as u32,
            },
        );
        for event in events {
            write_event(
                &mut self.payload,
                ctx.origin,
                event.tag,
                event.seq,
                event.size,
            );
        }
    }
}

#[cold]
#[inline(never)]
fn log_error(ctx: &PublishContext<'_>, err: &StreamError) {
    if ctx
        .stats
        .counters
        .errors_logged
        .fetch_add(1, Ordering::Relaxed)
        < LOGGED_ERRORS
    {
        eprintln!("node {} publish: {err}", ctx.origin.node);
    }
}
