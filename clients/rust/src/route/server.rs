/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::future::Future;
use std::hash::BuildHasher;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::num::{NonZeroU8, NonZeroU16};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::{Duration, Instant};

use foldhash::fast::RandomState;
use keep_proto::{Fingerprint, KeyHash};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::timeout_at;

use crate::config::{Backoff, FeedPlacement, Selection};
use crate::conn::{Close, Conn, Epochs, Gate, Handshake, Opener, Signals};
use crate::error::Error;
use crate::stats::Stats;

const RECONNECT_MIN: Duration = Duration::from_millis(50);
const RECONNECT_MAX: Duration = Duration::from_secs(2);
const WATCHDOG_TICK: Duration = Duration::from_millis(100);
const CONNECTS_PER_SHARD: usize = 8;
const PLACEMENT_CONNECTS_PER_SHARD: usize = 7;
const COLLECT_RETRY_MIN: Duration = Duration::from_secs(1);
const COLLECT_RETRY_MAX: Duration = Duration::from_secs(30);
const RETIRE_POLL: Duration = Duration::from_millis(5);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Layout {
    Unrouted(usize),
    Routed {
        shards: NonZeroU16,
        per_shard: NonZeroU8,
    },
    Placed(FeedTarget),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct FeedTarget {
    node: u64,
    width: Option<NonZeroU16>,
}

impl FeedTarget {
    pub fn new(placement: FeedPlacement, node: u64) -> Option<FeedTarget> {
        let width = match placement {
            FeedPlacement::Random => return None,
            FeedPlacement::Spread => None,
            FeedPlacement::Concentrate(width) => Some(width),
        };
        Some(FeedTarget { node, width })
    }

    fn shard(self, count: NonZeroU16) -> u16 {
        let width = self.width.map_or(count, |width| width.min(count));
        (self.node % u64::from(width.get())) as u16
    }
}

struct Misplaced {
    handshake: Handshake,
    shards: NonZeroU16,
}

pub(crate) struct Policy {
    pub selection: Selection,
    pub escape: Option<Duration>,
    pub attempt_timeout: Duration,
    pub keepalive: Option<Duration>,
    pub backoff: Backoff,
    pub fingerprint: Option<Fingerprint>,
    pub signals: Arc<Signals>,
}

pub(crate) struct Server {
    addr: SocketAddr,
    layout: Layout,
    slots: Box<[Arc<Conn>]>,
    policy: Arc<Policy>,
    epochs: Arc<Epochs>,
    gate: Arc<Gate>,
    routable: AtomicBool,
    ticket: AtomicUsize,
    workers: RandomState,
    down: Arc<Notify>,
    closed: AtomicBool,
    exhausted: AtomicBool,
    escapes: AtomicU64,
    connect_failures: AtomicU64,
    misplaced: AtomicU64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Refill {
    Complete,
    Failed,
    TimedOut,
    Exhausted,
    Gone,
}

impl Server {
    pub fn start(
        addr: SocketAddr,
        layout: Layout,
        policy: Arc<Policy>,
        opener: Opener,
        first: Option<Handshake>,
    ) -> Arc<Server> {
        let down = Arc::new(Notify::new());
        let count = match layout {
            Layout::Unrouted(count) => count,
            Layout::Routed { shards, per_shard } => {
                usize::from(shards.get()) * usize::from(per_shard.get())
            }
            Layout::Placed(_) => 1,
        };
        let epochs = Arc::new(Epochs::default());
        let gate = Arc::new(Gate::default());
        let slots = (0..count)
            .map(|_| {
                Conn::new(
                    addr,
                    Arc::clone(&down),
                    Arc::clone(&policy.signals),
                    Arc::clone(&epochs),
                    Arc::clone(&gate),
                )
            })
            .collect();
        let server = Arc::new(Server {
            addr,
            layout,
            slots,
            epochs,
            gate,
            routable: AtomicBool::new(matches!(layout, Layout::Routed { .. })),
            policy,
            ticket: AtomicUsize::new(0),
            workers: RandomState::default(),
            down,
            closed: AtomicBool::new(false),
            exhausted: AtomicBool::new(false),
            escapes: AtomicU64::new(0),
            connect_failures: AtomicU64::new(0),
            misplaced: AtomicU64::new(0),
        });
        if let Some(first) = first
            && let Some(misplaced) = server.place(first)
        {
            server.keep(misplaced);
        }
        tokio::spawn(maintain(Arc::downgrade(&server), opener));
        server
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn layout(&self) -> Layout {
        self.layout
    }

    pub fn epochs(&self) -> &Epochs {
        &self.epochs
    }

    pub fn gate(&self) -> &Gate {
        &self.gate
    }

    pub fn is_routable(&self) -> bool {
        self.routable.load(Ordering::Relaxed)
    }

    pub fn feed(&self) -> Option<&Arc<Conn>> {
        self.slots.first()
    }

    pub fn healthy(&self) -> usize {
        self.slots.iter().filter(|conn| conn.is_healthy()).count()
    }

    #[inline]
    pub fn pick(&self, hash: Option<KeyHash>) -> Option<&Arc<Conn>> {
        let (preferred, shard): (Option<&Arc<Conn>>, &[Arc<Conn>]) = match (self.layout, hash) {
            (Layout::Routed { shards, per_shard }, Some(hash)) if self.is_routable() => {
                match self.shard_slots(hash.shard(shards), per_shard) {
                    Some([conn]) => (Some(conn), &[]),
                    Some(conns) => (
                        conns
                            .iter()
                            .filter(|conn| conn.is_healthy())
                            .min_by_key(|conn| conn.outstanding()),
                        conns,
                    ),
                    None => (None, &[]),
                }
            }
            (Layout::Unrouted(count), _) if self.policy.selection == Selection::Worker => {
                let worker = self.workers.hash_one(thread::current().id()) as usize;
                (self.slots.get(worker % count.max(1)), &[])
            }
            _ => (None, &[]),
        };
        match preferred {
            Some(conn) if conn.is_healthy() => match self.policy.escape {
                Some(escape) if conn.stalled(escape) => {
                    let twin = shard
                        .iter()
                        .filter(|other| other.is_healthy() && !other.stalled(escape))
                        .min_by_key(|other| other.outstanding());
                    match twin.or_else(|| self.least_where(|other| !other.stalled(escape))) {
                        Some(other) => {
                            self.escapes.fetch_add(1, Ordering::Relaxed);
                            Some(other)
                        }
                        None => Some(conn),
                    }
                }
                _ => Some(conn),
            },
            _ => self.least(),
        }
    }

    #[inline]
    fn shard_slots(&self, shard: u16, per_shard: NonZeroU8) -> Option<&[Arc<Conn>]> {
        let per = usize::from(per_shard.get());
        let first = usize::from(shard) * per;
        self.slots.get(first..first + per)
    }

    fn least(&self) -> Option<&Arc<Conn>> {
        self.least_where(|_| true)
    }

    fn least_where(&self, usable: impl Fn(&Conn) -> bool) -> Option<&Arc<Conn>> {
        let count = self.slots.len();
        let ticket = self.ticket.fetch_add(1, Ordering::Relaxed);
        let first = ticket % count.max(1);
        let second = match count {
            0 | 1 => first,
            _ => (first + 1 + (ticket.wrapping_mul(0x9E37_79B9) >> 7) % (count - 1)) % count,
        };
        let qualifies = |conn: &&Arc<Conn>| conn.is_healthy() && usable(conn);
        let candidate = |index: usize| self.slots.get(index).filter(qualifies);
        match (candidate(first), candidate(second)) {
            (Some(a), Some(b)) => Some(if b.outstanding() < a.outstanding() {
                b
            } else {
                a
            }),
            (Some(conn), None) | (None, Some(conn)) => Some(conn),
            (None, None) => self.slots.iter().find(qualifies),
        }
    }

    pub fn stats(&self) -> Stats {
        let mut total = Stats::default();
        for conn in &self.slots {
            total += conn.stats();
        }
        total += self.gate.stats();
        total.escapes += self.escapes.load(Ordering::Relaxed);
        total.connect_failures += self.connect_failures.load(Ordering::Relaxed);
        total.feed_misplaced += self.misplaced.load(Ordering::Relaxed);
        total
    }

    pub async fn settle(&self, deadline: Instant) {
        let failures = self.connect_failures.load(Ordering::Relaxed);
        while self.healthy() < self.slots.len()
            && !self.exhausted.load(Ordering::Relaxed)
            && self.connect_failures.load(Ordering::Relaxed) == failures
            && Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub fn shutdown(&self, close: Close) {
        self.closed.store(true, Ordering::Relaxed);
        self.slots.iter().for_each(|conn| conn.shutdown(close));
        self.down.notify_one();
    }

    pub fn retire(self: Arc<Self>) {
        self.closed.store(true, Ordering::Relaxed);
        self.down.notify_one();
        self.slots.iter().for_each(|conn| conn.retire());
        tokio::spawn(async move {
            let limit = self.policy.attempt_timeout;
            let until = Instant::now() + limit;
            loop {
                let ended = Instant::now() >= until;
                let mut open = false;
                for conn in self.slots.iter() {
                    match ended || conn.outstanding() == 0 || conn.stalled(limit) {
                        true => conn.close_retired(),
                        false => open = true,
                    }
                }
                if !open {
                    break;
                }
                tokio::time::sleep(RETIRE_POLL).await;
            }
        });
    }

    fn missing(&self) -> usize {
        self.slots.iter().filter(|conn| conn.is_down()).count()
    }

    fn place(&self, handshake: Handshake) -> Option<Misplaced> {
        let shard_slots = match self.layout {
            Layout::Routed { shards, per_shard } => {
                let matches = handshake.shard().is_some_and(|info| info.count() == shards)
                    && handshake.info.placement == self.policy.fingerprint;
                if !matches {
                    self.routable.store(false, Ordering::Relaxed);
                }
                handshake
                    .shard()
                    .map(|info| info.shard())
                    .filter(|_| matches)
                    .and_then(|shard| self.shard_slots(shard, per_shard))
            }
            Layout::Placed(target) => match handshake.shard() {
                Some(info) if info.shard() != target.shard(info.count()) => {
                    return Some(Misplaced {
                        handshake,
                        shards: info.count(),
                    });
                }
                Some(_) => None,
                None => {
                    self.misplaced.fetch_add(1, Ordering::Relaxed);
                    None
                }
            },
            Layout::Unrouted(_) => None,
        };
        self.install(handshake, shard_slots);
        None
    }

    fn keep(&self, misplaced: Misplaced) {
        self.misplaced.fetch_add(1, Ordering::Relaxed);
        self.install(misplaced.handshake, None);
    }

    fn install(&self, handshake: Handshake, shard_slots: Option<&[Arc<Conn>]>) {
        let mut handshake = Some(handshake);
        let placed = match shard_slots {
            Some(conns) => conns.iter().any(|conn| conn.install(&mut handshake)),
            None => {
                !self.is_routable()
                    && self
                        .slots
                        .iter()
                        .filter(|conn| conn.is_down())
                        .any(|conn| conn.install(&mut handshake))
            }
        };
        if placed && self.epochs.forget() {
            self.policy.signals.nudge();
        }
    }

    fn tick(&self) {
        for conn in self.slots.iter() {
            conn.watchdog(self.policy.attempt_timeout);
            if let Some(idle) = self.policy.keepalive {
                conn.keepalive(idle);
            }
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.slots
            .iter()
            .for_each(|conn| conn.shutdown(Close::Graceful));
    }
}

async fn maintain(server: Weak<Server>, opener: Opener) {
    let Some((addr, down, backoff, layout, attempt_timeout)) = server.upgrade().map(|s| {
        (
            s.addr,
            Arc::clone(&s.down),
            s.policy.backoff,
            s.layout,
            s.policy.attempt_timeout,
        )
    }) else {
        return;
    };
    let mut delay = RECONNECT_MIN;
    let mut collect = COLLECT_RETRY_MIN;
    loop {
        let missing = match server.upgrade() {
            Some(server) if !server.closed.load(Ordering::Relaxed) => {
                server.tick();
                server.missing()
            }
            _ => return,
        };
        if missing == 0 {
            if idle(&server, &down, WATCHDOG_TICK).await == Woken::Gone {
                return;
            }
            continue;
        }
        let (budget, until) = match layout {
            Layout::Unrouted(_) => (missing, None),
            Layout::Placed(_) => (missing, Some(Instant::now() + attempt_timeout / 2)),
            Layout::Routed { .. } => (missing * CONNECTS_PER_SHARD, None),
        };
        let refill = refill(&server, &opener, addr, missing, budget, until);
        let outcome = match ticking(&server, refill).await {
            Some(outcome) => outcome,
            None => return,
        };
        let left = server.upgrade().map_or(0, |server| {
            server
                .exhausted
                .store(outcome == Refill::Exhausted, Ordering::Relaxed);
            server.missing()
        });
        let wait = match outcome {
            Refill::Gone => return,
            Refill::Complete => {
                delay = RECONNECT_MIN;
                collect = COLLECT_RETRY_MIN;
                continue;
            }
            Refill::Exhausted => {
                delay = RECONNECT_MIN;
                let wait = wait(backoff, collect);
                collect = (collect * 2).min(COLLECT_RETRY_MAX);
                wait
            }
            Refill::Failed => {
                let wait = wait(backoff, delay);
                delay = (delay * 2).min(RECONNECT_MAX);
                wait
            }
            Refill::TimedOut => {
                delay = RECONNECT_MIN;
                wait(backoff, delay)
            }
        };
        if outcome != Refill::Exhausted {
            if ticking(&server, tokio::time::sleep(wait)).await.is_none() {
                return;
            }
            continue;
        }
        let until = Instant::now() + wait;
        loop {
            let step = until
                .saturating_duration_since(Instant::now())
                .min(WATCHDOG_TICK);
            match idle(&server, &down, step).await {
                Woken::Gone => return,
                Woken::Down
                    if server
                        .upgrade()
                        .is_some_and(|server| server.missing() > left) =>
                {
                    break;
                }
                _ if Instant::now() >= until => break,
                _ => {}
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Woken {
    Down,
    Tick,
    Gone,
}

async fn idle(server: &Weak<Server>, down: &Notify, step: Duration) -> Woken {
    let woken = tokio::select! {
        () = down.notified() => Woken::Down,
        () = tokio::time::sleep(step) => Woken::Tick,
    };
    match server.upgrade() {
        Some(server) if !server.closed.load(Ordering::Relaxed) => {
            server.tick();
            woken
        }
        _ => Woken::Gone,
    }
}

async fn ticking<F: Future>(server: &Weak<Server>, work: F) -> Option<F::Output> {
    tokio::pin!(work);
    loop {
        tokio::select! {
            output = &mut work => return Some(output),
            () = tokio::time::sleep(WATCHDOG_TICK) => match server.upgrade() {
                Some(server) if !server.closed.load(Ordering::Relaxed) => server.tick(),
                _ => return None,
            },
        }
    }
}

async fn refill(
    server: &Weak<Server>,
    opener: &Opener,
    addr: SocketAddr,
    mut missing: usize,
    mut budget: usize,
    until: Option<Instant>,
) -> Refill {
    let mut spent = 0;
    let mut spare = None;
    while missing > 0 {
        let left = budget.saturating_sub(spent);
        let expired = spare.is_some() && until.is_some_and(|until| Instant::now() >= until);
        if left == 0 || expired {
            let Some(misplaced) = spare.take() else {
                return Refill::Exhausted;
            };
            let Some(server) = server.upgrade() else {
                return Refill::Gone;
            };
            server.keep(misplaced);
            missing = server.missing();
            continue;
        }
        let round = missing.min(left);
        spent += round;
        let mut opens = JoinSet::new();
        for _ in 0..round {
            let opener = opener.clone();
            opens.spawn(async move { opener.open(addr).await });
        }
        let mut failed = None;
        loop {
            let next = opens.join_next();
            let opened = match until.filter(|_| spare.is_some()) {
                Some(until) => match timeout_at(until.into(), next).await {
                    Ok(opened) => opened,
                    Err(_) => break,
                },
                None => next.await,
            };
            let Some(opened) = opened else {
                break;
            };
            let Some(server) = server.upgrade() else {
                return Refill::Gone;
            };
            let outcome = match opened {
                Ok(Ok(handshake)) => {
                    if let Some(misplaced) = server.place(handshake) {
                        budget = usize::from(misplaced.shards.get()) * PLACEMENT_CONNECTS_PER_SHARD;
                        spare = Some(misplaced);
                    }
                    continue;
                }
                Ok(Err(Error::Connect(ErrorKind::TimedOut))) => Refill::TimedOut,
                _ => Refill::Failed,
            };
            if let Some(misplaced) = spare.take() {
                server.keep(misplaced);
            }
            server.connect_failures.fetch_add(1, Ordering::Relaxed);
            if failed != Some(Refill::Failed) {
                failed = Some(outcome);
            }
        }
        if let Some(failed) = failed {
            return failed;
        }
        missing = match server.upgrade() {
            Some(server) if !server.closed.load(Ordering::Relaxed) => server.missing(),
            _ => return Refill::Gone,
        };
    }
    Refill::Complete
}

fn wait(backoff: Backoff, delay: Duration) -> Duration {
    let fraction = || {
        let mut bytes = [0u8; 8];
        let _ = getrandom::fill(&mut bytes);
        (u64::from_le_bytes(bytes) >> 11) as f64 / (1u64 << 53) as f64
    };
    match backoff {
        Backoff::Fixed => delay,
        Backoff::Jitter => delay.mul_f64(fraction()),
        Backoff::Equal => delay / 2 + (delay / 2).mul_f64(fraction()),
    }
}
