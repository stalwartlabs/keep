/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::time::Duration;

use serde::Serialize;

use crate::arrival::{Arrival, Schedule};
use crate::clock::Clock;
use crate::conn::{Conn, ConnError};
use crate::io::{Driver, IoKind};
use crate::op::{OpKind, Request};
use crate::protocol::cluster::SlotMap;
use crate::protocol::shard::ShardMap;
use crate::protocol::{Matching, Protocol};
use crate::stats::{CpuWindow, NO_PHASE, PhaseStats, Recorder};
use crate::sys;
use crate::tls::Stream;
use crate::workload::trace::TraceShard;
use crate::workload::{Generator, VirtualTime};

pub const PRELOAD_PHASE: u8 = NO_PHASE - 1;
const MAX_BATCH: usize = 64;
const NS_PER_MS: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PreloadMode {
    Write,
    Check,
}

impl PreloadMode {
    pub fn is_write(&self) -> bool {
        *self == PreloadMode::Write
    }
}

pub enum Source {
    Synthetic(Generator),
    Trace {
        shard: TraceShard,
        values: Arc<[u8]>,
        span_ns: u64,
        repeat: bool,
    },
}

pub enum Pacing {
    Arrival(Arrival),
    Recorded { speed: f64 },
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Idle {
    Spin,
    Sleep { threshold_ns: u64 },
}

#[derive(Clone)]
pub enum NodeMap {
    Slots(Arc<SlotMap>),
    Shards(ShardMap),
}

impl NodeMap {
    #[inline]
    fn node_of(&self, key: &[u8]) -> usize {
        match self {
            NodeMap::Slots(map) => map.node_of(key),
            NodeMap::Shards(map) => map.node_of(key),
        }
    }
}

pub enum Routing {
    RoundRobin {
        next: usize,
    },
    Least {
        next: usize,
    },
    Nodes {
        map: NodeMap,
        per_node: usize,
        next: Vec<usize>,
    },
}

impl Routing {
    #[inline]
    fn pick(&mut self, key: &[u8], affinity: Option<u32>, conns: &[Conn]) -> usize {
        let count = conns.len();
        match self {
            Routing::RoundRobin { next } => match affinity {
                Some(a) => a as usize % count,
                None => {
                    let i = *next % count;
                    *next = i + 1;
                    i
                }
            },
            Routing::Least { next } => {
                let start = *next % count.max(1);
                *next = start + 1;
                let (tail, head) = conns.split_at(start);
                head.iter()
                    .chain(tail)
                    .enumerate()
                    .min_by_key(|(_, conn)| conn.in_flight())
                    .map_or(0, |(i, _)| (start + i) % count)
            }
            Routing::Nodes {
                map,
                per_node,
                next,
            } => {
                let node = map.node_of(key);
                let k = match affinity {
                    Some(a) => a as usize % *per_node,
                    None => {
                        let k = next[node] % *per_node;
                        next[node] = k + 1;
                        k
                    }
                };
                node * *per_node + k
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Segment {
    pub rate: f64,
    pub warm_start_ns: u64,
    pub meas_start_ns: u64,
    pub meas_end_ns: u64,
}

pub enum Msg {
    Phase {
        worker: usize,
        tid: i64,
        point: usize,
        stats: Box<PhaseStats>,
    },
    Preloaded {
        worker: usize,
        stats: Box<PhaseStats>,
    },
    Failed {
        worker: usize,
        error: String,
    },
}

pub struct Worker {
    pub id: usize,
    tid: i64,
    threads: usize,
    driver: Driver,
    conns: Vec<Conn>,
    protocol: Protocol,
    routing: Routing,
    source: Source,
    preloader: Option<Generator>,
    rec: Recorder,
    dirty: Vec<usize>,
    seed: u64,
}

struct Pace {
    schedule: Schedule,
    speed: Option<f64>,
    segment: usize,
}

pub struct WorkerSetup {
    pub id: usize,
    pub threads: usize,
    pub cpu: Option<usize>,
    pub io: IoKind,
    pub streams: Vec<Stream>,
    pub protocol: Protocol,
    pub routing: Routing,
    pub source: Source,
    pub preloader: Option<Generator>,
    pub rec: Recorder,
    pub seed: u64,
}

impl WorkerSetup {
    pub fn pin(&self) -> Option<String> {
        let cpu = self.cpu?;
        sys::pin_current_thread(cpu)
            .err()
            .map(|e| format!("worker {} cannot pin to cpu {cpu}: {e}", self.id))
    }
}

impl Worker {
    pub fn new(setup: WorkerSetup) -> std::io::Result<Self> {
        let matching: Matching = setup.protocol.matching();
        let conns: Vec<Conn> = (0..setup.streams.len())
            .map(|id| Conn::new(matching, u32::try_from(id).unwrap_or(u32::MAX)))
            .collect();
        let driver = Driver::new(setup.io, setup.streams)?;
        Ok(Worker {
            id: setup.id,
            tid: sys::thread_id(),
            threads: setup.threads.max(1),
            driver,
            dirty: Vec::with_capacity(conns.len()),
            conns,
            protocol: setup.protocol,
            routing: setup.routing,
            source: setup.source,
            preloader: setup.preloader,
            rec: setup.rec,
            seed: setup.seed,
        })
    }

    pub fn preload(&mut self, clock: &Clock, depth: usize, mode: PreloadMode, tx: &Sender<Msg>) {
        self.rec.open(PRELOAD_PHASE);
        let cpu_start = sys::thread_cpu_ns();
        let wall_start = clock.now_ns();
        if let Err(e) = self.preload_inner(clock, depth, mode) {
            self.abort(&e, tx);
            return;
        }
        if let Some(mut stats) = self.rec.take(PRELOAD_PHASE) {
            stats.cpu = CpuWindow {
                wall_ns: clock.now_ns() - wall_start,
                thread_cpu_ns: sys::thread_cpu_ns() - cpu_start,
                idle_ns: 0,
            };
            let _ = tx.send(Msg::Preloaded {
                worker: self.id,
                stats,
            });
        }
    }

    fn preload_inner(
        &mut self,
        clock: &Clock,
        depth: usize,
        mode: PreloadMode,
    ) -> Result<(), ConnError> {
        let cap = (depth * self.conns.len()) as u64;
        let counts = match (&self.source, &self.preloader) {
            (Source::Synthetic(generator), _) | (_, Some(generator)) => generator.preload_counts(),
            _ => Vec::new(),
        };
        let mut cursor = PreloadCursor::new(counts, self.id as u64, self.threads as u64);
        loop {
            let now = clock.now_ns();
            while self.rec.total_outstanding() < cap {
                let Some((ks, index)) = cursor.next() else {
                    break;
                };
                let Worker {
                    conns,
                    protocol,
                    routing,
                    source,
                    preloader,
                    rec,
                    dirty,
                    ..
                } = self;
                let generator = match (source, preloader) {
                    (Source::Synthetic(generator), _) | (_, Some(generator)) => generator,
                    _ => break,
                };
                let Some(mut req) = generator.preload(ks, index, 0) else {
                    continue;
                };
                if mode == PreloadMode::Check {
                    req.kind = OpKind::Exists;
                    req.value = &[];
                }
                let ci = routing.pick(req.key, None, conns);
                let Some(conn) = conns.get_mut(ci) else {
                    break;
                };
                conn.push(protocol, &req, now, PRELOAD_PHASE)?;
                rec.issued(PRELOAD_PHASE);
                if conn.mark_dirty() {
                    dirty.push(ci);
                }
            }
            self.flush_dirty(clock)?;
            for i in 0..self.poll_ready(Duration::ZERO)? {
                self.handle_ready(i, clock)?;
            }
            if cursor.done() && self.rec.total_outstanding() == 0 {
                return Ok(());
            }
        }
    }

    pub fn run(
        &mut self,
        clock: &Clock,
        segments: &[Segment],
        pacing: &Pacing,
        idle: Idle,
        drain_ns: u64,
        tx: &Sender<Msg>,
    ) {
        if let Err(e) = self.run_inner(clock, segments, pacing, idle, drain_ns, tx) {
            self.abort(&e, tx);
        }
    }

    fn run_inner(
        &mut self,
        clock: &Clock,
        segments: &[Segment],
        pacing: &Pacing,
        idle: Idle,
        drain_ns: u64,
        tx: &Sender<Msg>,
    ) -> Result<(), ConnError> {
        let phase_offset = (self.id as f64 + 0.5) / self.threads as f64;
        let mut pace = Pace {
            schedule: Schedule::new(
                match pacing {
                    Pacing::Arrival(arrival) => *arrival,
                    Pacing::Recorded { .. } => Arrival::Constant,
                },
                self.seed ^ 0x9e37_79b9_7f4a_7c15,
                phase_offset,
            ),
            speed: match pacing {
                Pacing::Recorded { speed } => Some(*speed),
                Pacing::Arrival(_) => None,
            },
            segment: 0,
        };
        let mut start_cpu = CpuWindow::default();
        let mut measuring = false;
        let mut closing: VecDeque<(u8, u64)> = VecDeque::new();
        let mut idle_ns = 0u64;
        let mut last_now = clock.now_ns();
        let mut last_idle = false;
        if let Some(first) = segments.first() {
            self.rec.open(0);
            pace.schedule.start(first.warm_start_ns, first.rate);
        }
        loop {
            let now = clock.now_ns();
            if last_idle {
                idle_ns += now - last_now;
            }
            if measuring {
                self.rec.loop_gap(
                    u8::try_from(pace.segment).unwrap_or(NO_PHASE),
                    now - last_now,
                );
            }
            last_now = now;
            if !measuring
                && let Some(seg) = segments.get(pace.segment)
                && now >= seg.meas_start_ns
            {
                measuring = true;
                self.rec
                    .set_current(u8::try_from(pace.segment).unwrap_or(NO_PHASE));
                start_cpu = CpuWindow {
                    wall_ns: now,
                    thread_cpu_ns: sys::thread_cpu_ns(),
                    idle_ns,
                };
            }
            let mut issued = self.issue_and_flush(now, segments, &mut pace, clock)?;
            if let Some(seg) = segments.get(pace.segment)
                && now >= seg.meas_end_ns
                && self.next_due(&pace, seg) >= seg.meas_end_ns
            {
                let point = u8::try_from(pace.segment).unwrap_or(NO_PHASE);
                if let Some(stats) = self.rec.phase_mut(point) {
                    stats.cpu = CpuWindow {
                        wall_ns: now - start_cpu.wall_ns,
                        thread_cpu_ns: sys::thread_cpu_ns() - start_cpu.thread_cpu_ns,
                        idle_ns: idle_ns - start_cpu.idle_ns,
                    };
                }
                measuring = false;
                self.rec.set_current(NO_PHASE);
                closing.push_back((point, now + drain_ns));
                pace.segment += 1;
                if let Some(next) = segments.get(pace.segment) {
                    self.rec.open(point + 1);
                    if pace.speed.is_none() {
                        pace.schedule.start(next.warm_start_ns, next.rate);
                    }
                }
            }
            let timeout = match idle {
                Idle::Spin => Duration::ZERO,
                Idle::Sleep { threshold_ns } => {
                    let due = segments
                        .get(pace.segment)
                        .map_or(u64::MAX, |seg| self.next_due(&pace, seg));
                    let gap = due.saturating_sub(now).saturating_sub(threshold_ns);
                    Duration::from_millis(gap / NS_PER_MS)
                }
            };
            let events = if timeout.is_zero() {
                self.poll_ready(timeout)?
            } else {
                let before = clock.now_ns();
                let events = self.poll_ready(timeout)?;
                idle_ns += clock.now_ns() - before;
                events
            };
            for i in 0..events {
                self.handle_ready(i, clock)?;
                issued += self.issue_and_flush(clock.now_ns(), segments, &mut pace, clock)?;
            }
            while let Some(&(point, deadline)) = closing.front() {
                if self.rec.outstanding(point) > 0 && now < deadline {
                    break;
                }
                closing.pop_front();
                if let Some(stats) = self.rec.take(point) {
                    let _ = tx.send(Msg::Phase {
                        worker: self.id,
                        tid: self.tid,
                        point: point as usize,
                        stats,
                    });
                }
            }
            if pace.segment >= segments.len() && closing.is_empty() {
                return Ok(());
            }
            last_idle = issued == 0 && events == 0 && timeout.is_zero();
        }
    }

    #[inline]
    fn issue_and_flush(
        &mut self,
        now: u64,
        segments: &[Segment],
        pace: &mut Pace,
        clock: &Clock,
    ) -> Result<usize, ConnError> {
        let Some(seg) = segments.get(pace.segment) else {
            return Ok(0);
        };
        if self.next_due(pace, seg) > now {
            return Ok(0);
        }
        let point = u8::try_from(pace.segment).unwrap_or(NO_PHASE);
        let issued = self.issue_due(now, seg, point, pace)?;
        self.flush_dirty(clock)?;
        Ok(issued)
    }

    #[inline]
    fn next_due(&self, pace: &Pace, seg: &Segment) -> u64 {
        match (pace.speed, &self.source) {
            (
                Some(speed),
                Source::Trace {
                    shard,
                    span_ns,
                    repeat,
                    ..
                },
            ) => shard
                .peek_t_ns(*span_ns, *repeat)
                .map_or(u64::MAX, |t| seg.warm_start_ns + (t as f64 / speed) as u64),
            _ => pace.schedule.due_ns(),
        }
    }

    #[inline]
    fn issue_due(
        &mut self,
        now: u64,
        seg: &Segment,
        point: u8,
        pace: &mut Pace,
    ) -> Result<usize, ConnError> {
        let mut issued = 0;
        while issued < MAX_BATCH {
            let due = self.next_due(pace, seg);
            if due > now || due >= seg.meas_end_ns {
                break;
            }
            let phase = if due >= seg.meas_start_ns {
                point
            } else {
                NO_PHASE
            };
            let Worker {
                conns,
                protocol,
                routing,
                source,
                rec,
                dirty,
                ..
            } = self;
            let (req, affinity): (Request<'_>, Option<u32>) = match source {
                Source::Synthetic(generator) => (generator.next(due), None),
                Source::Trace {
                    shard,
                    values,
                    span_ns,
                    repeat,
                } => match shard.next(values, *span_ns, *repeat) {
                    Some(op) => (op.request, Some(op.affinity)),
                    None => break,
                },
            };
            let ci = routing.pick(req.key, affinity, conns);
            let Some(conn) = conns.get_mut(ci) else {
                break;
            };
            conn.push(protocol, &req, due, phase)?;
            rec.issued(phase);
            if conn.mark_dirty() {
                dirty.push(ci);
            }
            if pace.speed.is_none() {
                pace.schedule.advance();
            }
            issued += 1;
        }
        Ok(issued)
    }

    #[inline]
    fn flush_dirty(&mut self, clock: &Clock) -> Result<(), ConnError> {
        let Worker {
            driver,
            conns,
            rec,
            dirty,
            ..
        } = self;
        for ci in dirty.drain(..) {
            if let Some(conn) = conns.get_mut(ci) {
                driver.flush(ci, conn, clock, rec)?;
            }
        }
        Ok(())
    }

    #[inline]
    fn poll_ready(&mut self, timeout: Duration) -> Result<usize, ConnError> {
        self.driver.poll(timeout, &mut self.rec)
    }

    #[inline]
    fn handle_ready(&mut self, index: usize, clock: &Clock) -> Result<(), ConnError> {
        let Worker {
            driver,
            conns,
            protocol,
            rec,
            ..
        } = self;
        driver.handle(index, conns, protocol, clock, rec)
    }

    fn abort(&mut self, error: &ConnError, tx: &Sender<Msg>) {
        let Worker { conns, rec, .. } = self;
        conns.iter_mut().for_each(|conn| conn.fail_all(rec));
        for point in 0..=PRELOAD_PHASE {
            if let Some(stats) = rec.take(point) {
                let _ = tx.send(Msg::Phase {
                    worker: self.id,
                    tid: self.tid,
                    point: point as usize,
                    stats,
                });
            }
        }
        let _ = tx.send(Msg::Failed {
            worker: self.id,
            error: error.to_string(),
        });
    }

    pub fn conn_count(&self) -> usize {
        self.conns.len()
    }

    pub fn set_time(&mut self, time: VirtualTime) {
        if let Source::Synthetic(generator) = &mut self.source {
            generator.set_time(time);
        }
        if let Some(generator) = &mut self.preloader {
            generator.set_time(time);
        }
    }
}

struct PreloadCursor {
    counts: Vec<u64>,
    ks: usize,
    next: u64,
    step: u64,
}

impl PreloadCursor {
    fn new(counts: Vec<u64>, start: u64, step: u64) -> Self {
        PreloadCursor {
            counts,
            ks: 0,
            next: start,
            step,
        }
    }

    fn next(&mut self) -> Option<(usize, u64)> {
        loop {
            let count = *self.counts.get(self.ks)?;
            if self.next < count {
                let index = self.next;
                self.next += self.step;
                return Some((self.ks, index));
            }
            self.ks += 1;
            self.next %= self.step.max(1);
        }
    }

    fn done(&self) -> bool {
        self.ks >= self.counts.len()
    }
}
