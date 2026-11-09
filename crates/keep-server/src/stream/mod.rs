/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

mod hub;
mod lanes;
mod records;
mod subs;

use std::mem;
use std::sync::Arc;
use std::time::Duration;

use keep_proto::{MAX_SUBSCRIPTIONS, Mark, Status, Topic, Value};
use keep_rt::clock::Nanos;
use keep_rt::{ConnId, Conns, Ctx};

pub use hub::Hub;
pub use records::Records;

use crate::shard::{Msg as ShardMsg, OUTPUT_CAP};
use lanes::Lanes;
use records::Buckets;
use subs::Subscriptions;

pub const SUBSCRIBER_CAP: usize = OUTPUT_CAP;

pub const WINDOW: usize = 16 << 20;

const RETRY: Duration = Duration::from_millis(10);

const COALESCE_BYTES: usize = 16 << 10;

const BUCKETS: usize = 256;

#[derive(Debug)]
pub enum Msg {
    Events(Arc<Records>),
    Skipped(Box<[(Topic, u32)]>),
    Marker,
}

#[derive(Debug, Default)]
pub struct Topics(Vec<Topic>);

impl Topics {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Counts {
    pub published: u64,
    pub delivered: u64,
    pub skipped: u64,
    pub slow: u64,
    pub skip_runs: u64,
    pub windows: u64,
    pub held_ns: u64,
}

#[derive(Debug)]
pub struct Stream {
    me: usize,
    hub: Arc<Hub>,
    subs: Subscriptions,
    turn: Records,
    lanes: Lanes,
    wanted: Box<[u32; BUCKETS]>,
    interval: Duration,
    slack: Duration,
    next_beat: Option<Nanos>,
    slow: u64,
    markers: usize,
    sealed: bool,
    ended: bool,
    counts: Counts,
    coalesce: Duration,
    fanout: u32,
    window: Vec<ConnId>,
    window_open: Option<Nanos>,
}

impl Stream {
    pub fn new(
        me: u16,
        hub: Arc<Hub>,
        interval: Duration,
        coalesce: Duration,
        fanout: u32,
    ) -> Stream {
        let me = usize::from(me);
        Stream {
            me,
            lanes: Lanes::new(me, hub.shards()),
            hub,
            subs: Subscriptions::default(),
            turn: Records::default(),
            wanted: Box::new([0; BUCKETS]),
            interval,
            slack: interval / 4,
            next_beat: None,
            slow: 0,
            markers: 0,
            sealed: false,
            ended: false,
            counts: Counts::default(),
            coalesce,
            fanout,
            window: Vec::new(),
            window_open: None,
        }
    }

    #[inline]
    pub fn publish(&mut self, topic: Topic, payload: Value<'_>, max_value: usize) -> Status {
        if payload.as_bytes().len() > max_value {
            return Status::TooLarge;
        }
        self.turn.push(topic, payload);
        self.counts.published += 1;
        Status::Ok
    }

    pub fn subscribe(
        &mut self,
        cx: &mut Ctx<'_, ShardMsg>,
        conn: ConnId,
        topics: &mut Topics,
        topic: Topic,
    ) -> Status {
        let Err(at) = topics.0.binary_search(&topic) else {
            return Status::Conflict;
        };
        if topics.0.len() >= MAX_SUBSCRIPTIONS {
            return Status::Full;
        }
        if topics.is_empty() {
            self.hub.subscriber_joined();
        }
        topics.0.insert(at, topic);
        let now = cx.now();
        self.subs.insert(topic, conn, now);
        self.want(topic, true);
        let due = now.after(self.interval);
        if self.next_beat.is_none_or(|next| due < next) {
            self.next_beat = Some(due);
            cx.wake_at(due);
        }
        Status::Ok
    }

    pub fn unsubscribe(&mut self, conn: ConnId, topics: &mut Topics, topic: Topic) -> Option<Mark> {
        let at = topics.0.binary_search(&topic).ok()?;
        topics.0.remove(at);
        if topics.is_empty() {
            self.hub.subscriber_left();
        }
        self.want(topic, false);
        self.subs.remove(topic, conn)
    }

    pub fn closed(&mut self, topics: Topics) {
        if !topics.is_empty() {
            self.hub.subscriber_left();
        }
        for topic in topics.0 {
            self.want(topic, false);
        }
    }

    pub fn message<C>(&mut self, from: usize, msg: Msg, conns: &mut Conns<'_, C>, now: Nanos) {
        match msg {
            Msg::Events(records) => {
                let span = self.hold_span();
                for run in records.runs() {
                    self.subs
                        .deliver(&run, conns, now, &mut self.counts, &mut self.window, span);
                }
                self.hub.consume(self.me, from, records.len());
            }
            Msg::Skipped(lost) => {
                for &(topic, count) in &lost {
                    self.subs.skip(topic, count, conns, now, &mut self.counts);
                }
            }
            Msg::Marker => self.markers += 1,
        }
    }

    pub fn end_turn<C>(&mut self, cx: &mut Ctx<'_, ShardMsg>, conns: &mut Conns<'_, C>) {
        let now = cx.now();
        if !self.turn.is_empty() {
            let span = self.hold_span();
            for run in self.turn.runs() {
                self.subs
                    .deliver(&run, conns, now, &mut self.counts, &mut self.window, span);
            }
        }
        if self.lanes.flush(&mut self.turn, &self.hub, cx.mesh()) {
            cx.wake_at(now.after(RETRY));
        }
        self.close_window(cx, conns, now);
        if self.sealed && !self.ended && self.markers + 1 >= self.hub.shards() {
            self.subs.end_all(conns);
            self.ended = true;
            cx.wake_at(now);
        }
    }

    pub fn ended(&self) -> bool {
        self.ended
    }

    pub fn beat<C>(&mut self, conns: &mut Conns<'_, C>, now: Nanos) -> Option<Nanos> {
        if self.next_beat.is_some_and(|next| next <= now) {
            let mut slow = 0;
            self.next_beat = self
                .subs
                .beat(conns, now, self.interval, self.slack, &mut slow);
            self.counts.slow = self.counts.slow.wrapping_add(slow.wrapping_sub(self.slow));
            self.slow = slow;
        }
        self.next_beat
    }

    pub fn seal(&mut self) {
        if !self.sealed {
            self.sealed = true;
            self.lanes.seal();
        }
    }

    pub fn markers_in(&self) -> bool {
        self.markers + 1 >= self.hub.shards()
    }

    pub fn settled(&self) -> bool {
        self.sealed && !self.lanes.owing()
    }

    pub fn take_counts(&mut self) -> Counts {
        mem::take(&mut self.counts)
    }

    #[inline]
    fn hold_span(&self) -> Duration {
        if self.sealed || self.hub.subscribers() < self.fanout {
            Duration::ZERO
        } else {
            self.coalesce
        }
    }

    fn close_window<C>(
        &mut self,
        cx: &mut Ctx<'_, ShardMsg>,
        conns: &mut Conns<'_, C>,
        now: Nanos,
    ) {
        if self.window.is_empty() {
            self.window_open = None;
            return;
        }
        let open = *self.window_open.get_or_insert(now);
        let end = open.after(self.coalesce);
        if now < end && !self.sealed {
            cx.wake_at(end);
            return;
        }
        for conn in self.window.drain(..) {
            conns.get(conn);
        }
        self.window_open = None;
        self.counts.windows += 1;
        self.counts.held_ns += now.raw().saturating_sub(open.raw());
    }

    fn want(&mut self, topic: Topic, wanted: bool) {
        let bucket = Buckets::bucket(topic);
        let Some(count) = self.wanted.get_mut(bucket) else {
            return;
        };
        let before = *count;
        *count = if wanted {
            before.saturating_add(1)
        } else {
            before.saturating_sub(1)
        };
        if (before == 0) != (*count == 0) {
            self.hub.show(self.me, bucket, wanted);
        }
    }
}
