/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    clock::{Clock, Window, duration_ns},
    cpu::{ProcessUsage, thread_cpu_ns, tighten_timer_slack},
    kv::worker::{FollowUp, KvContext, KvJob, KvTask, Worker},
    stats::bump,
    stream::{Event, MessageSpec, StreamStats},
    workload::{
        EventSource, Workload,
        dist::exponential,
        trace::{EventTrace, KvTrace},
    },
};
use rand::{SeedableRng, rngs::SmallRng};
use std::{
    cmp::Ordering as CmpOrdering,
    collections::{BTreeMap, BinaryHeap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, RecvTimeoutError},
    },
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::mpsc::{Sender, error::TrySendError},
};

const OVERFLOW_RETRY_NS: u64 = 100_000;
const TRACE_LOOP_GAP_NS: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub enum Arrival {
    Poisson { rate: f64 },
    Uniform { rate: f64 },
    Trace { speed: f64, looped: Looping },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum Looping {
    Once,
    Repeat,
}

pub struct KvFeed {
    pub arrival: Arrival,
    pub ctx: Arc<KvContext>,
    pub jobs: JobSink,
    pub follow_ups: mpsc::Receiver<FollowUp>,
}

pub enum JobSink {
    Pool(async_channel::Sender<KvJob>),
    Spawn {
        handle: Handle,
        follow: mpsc::Sender<FollowUp>,
    },
}

pub struct EventFeed {
    pub arrival: Arrival,
    pub source: Arc<EventSource>,
    pub stats: Arc<StreamStats>,
    pub sink: EventSink,
}

pub enum EventSink {
    Events(Sender<Event>),
    Messages(Sender<MessageSpec>),
}

#[derive(Clone, Copy, Debug)]
enum Produced {
    Event(Event),
    Message(MessageSpec),
}

enum SinkError {
    Full(Produced),
    Closed,
}

impl EventSink {
    fn try_send(&self, item: Produced) -> Result<(), SinkError> {
        match (self, item) {
            (EventSink::Events(tx), Produced::Event(event)) => {
                tx.try_send(event).map_err(|err| match err {
                    TrySendError::Full(event) => SinkError::Full(Produced::Event(event)),
                    TrySendError::Closed(_) => SinkError::Closed,
                })
            }
            (EventSink::Messages(tx), Produced::Message(message)) => {
                tx.try_send(message).map_err(|err| match err {
                    TrySendError::Full(message) => SinkError::Full(Produced::Message(message)),
                    TrySendError::Closed(_) => SinkError::Closed,
                })
            }
            _ => Err(SinkError::Closed),
        }
    }
}

pub struct Dispatcher {
    pub clock: Clock,
    pub begin: u64,
    pub window: Window,
    pub spin: Duration,
    pub seed: u64,
    pub kv: Option<KvFeed>,
    pub events: Option<EventFeed>,
    pub burn: Arc<AtomicU64>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Sample {
    pub usage: ProcessUsage,
    pub thread_ns: u64,
    pub burn_ns: u64,
    pub at: u64,
}

#[derive(Debug, Default)]
pub struct Marks {
    pub start: Option<Sample>,
    pub end: Option<Sample>,
    pub client_start: Vec<(&'static str, u64)>,
    pub client_end: Vec<(&'static str, u64)>,
}

impl Marks {
    pub fn client_deltas(&self) -> Option<BTreeMap<&'static str, u64>> {
        (!self.client_end.is_empty() && self.client_start.len() == self.client_end.len()).then(
            || {
                self.client_start
                    .iter()
                    .zip(&self.client_end)
                    .map(|(&(_, start), &(name, end))| (name, end.saturating_sub(start)))
                    .collect()
            },
        )
    }
}

struct Pending {
    follow_up: FollowUp,
    order: u64,
}

impl PartialEq for Pending {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == CmpOrdering::Equal
    }
}

impl Eq for Pending {}

impl PartialOrd for Pending {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for Pending {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        (other.follow_up.due, other.order).cmp(&(self.follow_up.due, self.order))
    }
}

struct Cursor {
    next: Option<u64>,
    index: usize,
    base: u64,
}

struct KvState {
    feed: KvFeed,
    cursor: Cursor,
    heap: BinaryHeap<Pending>,
    order: u64,
}

struct EventState {
    feed: EventFeed,
    cursor: Cursor,
    overflow: VecDeque<Produced>,
    seq: u64,
}

impl Cursor {
    fn start(arrival: Arrival, begin: u64, rng: &mut SmallRng, first: Option<u64>) -> Cursor {
        let mut cursor = Cursor {
            next: None,
            index: 0,
            base: begin,
        };
        cursor.next = match arrival {
            Arrival::Trace { speed, .. } => first.map(|at| begin + scaled(at, speed)),
            arrival => Some(begin + gap(arrival, rng)),
        };
        cursor
    }

    fn advance(&mut self, arrival: Arrival, rng: &mut SmallRng, times: TraceTimes<'_>) {
        self.next = match (arrival, self.next) {
            (_, None) => None,
            (Arrival::Trace { speed, looped }, Some(_)) => {
                self.index += 1;
                match (times.at(self.index), looped) {
                    (Some(at), _) => Some(self.base + scaled(at, speed)),
                    (None, Looping::Repeat) => {
                        self.index = 0;
                        self.base += scaled(times.span(), speed) + TRACE_LOOP_GAP_NS;
                        times.at(0).map(|at| self.base + scaled(at, speed))
                    }
                    (None, Looping::Once) => None,
                }
            }
            (arrival, Some(at)) => Some(at + gap(arrival, rng)),
        };
    }
}

#[derive(Clone, Copy)]
enum TraceTimes<'a> {
    Rate,
    Kv(&'a KvTrace),
    Events(&'a EventTrace),
    Messages(&'a EventTrace),
}

impl TraceTimes<'_> {
    fn of_workload(workload: &Workload) -> TraceTimes<'_> {
        match workload {
            Workload::Trace(trace) => TraceTimes::Kv(trace),
            Workload::Mix(_) => TraceTimes::Rate,
        }
    }

    fn of_events<'a>(source: &'a EventSource, sink: &EventSink) -> TraceTimes<'a> {
        match (source, sink) {
            (EventSource::Trace(trace), EventSink::Events(_)) => TraceTimes::Events(trace),
            (EventSource::Trace(trace), EventSink::Messages(_)) => TraceTimes::Messages(trace),
            (EventSource::Mix(_), _) => TraceTimes::Rate,
        }
    }

    fn at(&self, index: usize) -> Option<u64> {
        match self {
            TraceTimes::Rate => None,
            TraceTimes::Kv(trace) => trace.at_ns(index),
            TraceTimes::Events(trace) => trace.events.get(index).map(|event| event.at_ns),
            TraceTimes::Messages(trace) => trace.messages.get(index).map(|message| message.at_ns),
        }
    }

    fn span(&self) -> u64 {
        match self {
            TraceTimes::Rate => 0,
            TraceTimes::Kv(trace) => trace.span_ns,
            TraceTimes::Events(trace) => trace.span_ns,
            TraceTimes::Messages(trace) => trace.message_span_ns,
        }
    }
}

fn scaled(at: u64, speed: f64) -> u64 {
    (at as f64 / speed) as u64
}

fn gap(arrival: Arrival, rng: &mut SmallRng) -> u64 {
    match arrival {
        Arrival::Poisson { rate } => exponential(rng, 1e9 / rate).max(1.0) as u64,
        Arrival::Uniform { rate } => (1e9 / rate).max(1.0) as u64,
        Arrival::Trace { .. } => u64::MAX,
    }
}

impl Dispatcher {
    pub fn run(self) -> Marks {
        tighten_timer_slack();
        let Dispatcher {
            clock,
            begin,
            window,
            spin,
            seed,
            kv,
            events,
            burn,
        } = self;
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut kv = kv.map(|feed| {
            let first = match &feed.ctx.workload {
                Workload::Trace(trace) => trace.at_ns(0),
                Workload::Mix(_) => None,
            };
            KvState {
                cursor: Cursor::start(feed.arrival, begin, &mut rng, first),
                feed,
                heap: BinaryHeap::new(),
                order: 0,
            }
        });
        let mut events = events.map(|feed| {
            let first = TraceTimes::of_events(&feed.source, &feed.sink).at(0);
            EventState {
                cursor: Cursor::start(feed.arrival, begin, &mut rng, first),
                feed,
                overflow: VecDeque::new(),
                seq: 1,
            }
        });
        let spin_ns = duration_ns(spin);
        let mut marks = Marks::default();
        loop {
            let now = clock.now();
            if marks.start.is_none() && now >= window.start {
                marks.start = Some(sample(&burn, now));
                if let Some(state) = kv.as_ref() {
                    state
                        .feed
                        .ctx
                        .backend
                        .client_counters(&mut marks.client_start);
                }
            }
            if now >= window.end {
                marks.end = Some(sample(&burn, now));
                if let Some(state) = kv.as_ref() {
                    state
                        .feed
                        .ctx
                        .backend
                        .client_counters(&mut marks.client_end);
                }
                break;
            }
            if let Some(state) = kv.as_mut() {
                state.issue(now, window, &mut rng);
            }
            if let Some(state) = events.as_mut() {
                state.issue(now, window, &mut rng);
            }
            let mut due = window.end;
            if marks.start.is_none() {
                due = due.min(window.start);
            }
            if let Some(state) = kv.as_ref() {
                due = state.next_due().map_or(due, |at| at.min(due));
            }
            if let Some(state) = events.as_ref() {
                due = state.cursor.next.map_or(due, |at| at.min(due));
                if !state.overflow.is_empty() {
                    due = due.min(now + OVERFLOW_RETRY_NS);
                }
            }
            wait_until(clock, due, spin_ns, kv.as_mut());
        }
        marks
    }
}

fn sample(burn: &AtomicU64, at: u64) -> Sample {
    Sample {
        usage: ProcessUsage::now(),
        thread_ns: thread_cpu_ns(),
        burn_ns: burn.load(Ordering::Relaxed),
        at,
    }
}

fn wait_until(clock: Clock, due: u64, spin_ns: u64, kv: Option<&mut KvState>) {
    let now = clock.now();
    if due <= now {
        return;
    }
    let sleep_ns = (due - now).saturating_sub(spin_ns);
    if sleep_ns > 0 {
        let timeout = Duration::from_nanos(sleep_ns);
        match kv {
            Some(state) => match state.feed.follow_ups.recv_timeout(timeout) {
                Ok(follow_up) => {
                    state.push(follow_up);
                    return;
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => std::thread::sleep(timeout),
            },
            None => std::thread::sleep(timeout),
        }
    }
    while clock.now() < due {
        std::hint::spin_loop();
    }
}

impl KvState {
    fn push(&mut self, follow_up: FollowUp) {
        self.order += 1;
        self.heap.push(Pending {
            follow_up,
            order: self.order,
        });
    }

    fn next_due(&self) -> Option<u64> {
        let follow = self.heap.peek().map(|pending| pending.follow_up.due);
        match (self.cursor.next, follow) {
            (Some(at), Some(due)) => Some(at.min(due)),
            (at, due) => at.or(due),
        }
    }

    fn issue(&mut self, now: u64, window: Window, rng: &mut SmallRng) {
        while let Ok(follow_up) = self.feed.follow_ups.try_recv() {
            self.push(follow_up);
        }
        while self
            .heap
            .peek()
            .is_some_and(|pending| pending.follow_up.due <= now)
        {
            if let Some(pending) = self.heap.pop() {
                self.send(pending.follow_up.due, now, pending.follow_up.task, window);
            }
        }
        while let Some(at) = self.cursor.next.filter(|at| *at <= now) {
            let task = match &self.feed.ctx.workload {
                Workload::Mix(mix) => KvTask::Action(mix.pick(rng)),
                Workload::Trace(_) => KvTask::Trace(self.cursor.index as u32),
            };
            self.send(at, now, task, window);
            let times = TraceTimes::of_workload(&self.feed.ctx.workload);
            self.cursor.advance(self.feed.arrival, rng, times);
        }
    }

    fn send(&self, intended: u64, now: u64, task: KvTask, window: Window) {
        let stats = &self.feed.ctx.stats;
        bump(&stats.counters.dispatched);
        if window.contains(intended) {
            bump(&stats.counters.dispatched_in_window);
            stats.lag.record(now - intended);
        }
        let job = KvJob {
            intended,
            dispatched: now,
            task,
        };
        match &self.feed.jobs {
            JobSink::Pool(jobs) => {
                let _ = jobs.try_send(job);
            }
            JobSink::Spawn { handle, follow } => {
                let ctx = self.feed.ctx.clone();
                let follow = follow.clone();
                handle.spawn(async move { Worker::once(&ctx, job, follow).await });
            }
        }
    }
}

impl EventState {
    fn issue(&mut self, now: u64, window: Window, rng: &mut SmallRng) {
        self.flush_overflow();
        while let Some(at) = self.cursor.next.filter(|at| *at <= now) {
            let event = |tag, size, account| Event {
                seq: 0,
                created: at,
                size,
                account,
                tag,
            };
            let produced = match (self.feed.source.as_ref(), &self.feed.sink) {
                (EventSource::Mix(mix), _) => {
                    let sampled = mix.sample(rng);
                    Produced::Event(event(sampled.tag, sampled.size, sampled.account))
                }
                (EventSource::Trace(trace), EventSink::Events(_)) => {
                    match trace.events.get(self.cursor.index) {
                        Some(record) => {
                            Produced::Event(event(record.tag, record.bytes.max(1), record.account))
                        }
                        None => break,
                    }
                }
                (EventSource::Trace(trace), EventSink::Messages(_)) => {
                    match trace.messages.get(self.cursor.index) {
                        Some(record) => Produced::Message(MessageSpec {
                            created: at,
                            bytes: record.bytes,
                            count: record.count,
                        }),
                        None => break,
                    }
                }
            };
            let (produced, events) = match produced {
                Produced::Event(event) => {
                    self.seq += 1;
                    (
                        Produced::Event(Event {
                            seq: self.seq,
                            ..event
                        }),
                        1,
                    )
                }
                message @ Produced::Message(spec) => (message, u64::from(spec.count)),
            };
            let stats = &self.feed.stats;
            if window.contains(at) {
                stats
                    .counters
                    .events_produced
                    .fetch_add(events, Ordering::Relaxed);
                stats.lag.record(now - at);
            }
            match self.overflow.is_empty() {
                true => match self.feed.sink.try_send(produced) {
                    Ok(()) | Err(SinkError::Closed) => {}
                    Err(SinkError::Full(produced)) => {
                        bump(&stats.counters.producer_stalls);
                        self.overflow.push_back(produced);
                    }
                },
                false => self.overflow.push_back(produced),
            }
            let times = TraceTimes::of_events(&self.feed.source, &self.feed.sink);
            self.cursor.advance(self.feed.arrival, rng, times);
        }
    }

    fn flush_overflow(&mut self) {
        while let Some(produced) = self.overflow.pop_front() {
            match self.feed.sink.try_send(produced) {
                Ok(()) => {}
                Err(SinkError::Full(produced)) => {
                    self.overflow.push_front(produced);
                    return;
                }
                Err(SinkError::Closed) => {
                    self.overflow.clear();
                    return;
                }
            }
        }
    }
}
