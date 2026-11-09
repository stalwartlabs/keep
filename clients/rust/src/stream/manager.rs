/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::HashMap;
use std::net::SocketAddr;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use keep_proto::Topic;

use super::inbox::Shared;
use crate::config::{ClientConfig, StreamMode};
use crate::conn::{Close, Conn, Opener, Settled, Signals, Want};
use crate::error::Error;
use crate::route::{Cluster, FeedTarget, Layout, Policy, Retired, Server, View};
use crate::stats::Stats;

const TICK: Duration = Duration::from_millis(100);

pub(crate) enum Source {
    Static { addr: SocketAddr, opener: Opener },
    Cluster(Arc<Cluster>),
}

struct Stamp {
    view: Weak<View>,
    generation: u64,
}

struct Carrier {
    stamp: Stamp,
    addr: SocketAddr,
    conn: Arc<Conn>,
}

pub(crate) struct Streams {
    mode: StreamMode,
    publisher: u64,
    layout: Layout,
    node_drawn: bool,
    source: Source,
    policy: Arc<Policy>,
    subscriptions: Mutex<Vec<Arc<Shared>>>,
    feeds: Mutex<HashMap<SocketAddr, Arc<Server>>>,
    generation: AtomicU64,
    carrier: ArcSwapOption<Carrier>,
    retired: Mutex<Retired>,
    running: AtomicBool,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Streams {
    pub fn new(
        config: &ClientConfig,
        source: Source,
        policy: Arc<Policy>,
    ) -> Result<Streams, Error> {
        let mut points = [[0u8; 8]; 2];
        getrandom::fill(points.as_flattened_mut())
            .map_err(|_| Error::Config("no randomness for the publisher point".into()))?;
        let [publisher, node] = points.map(u64::from_le_bytes);
        let target = FeedTarget::new(config.feeds, config.node.unwrap_or(node));
        Ok(Streams {
            mode: config.stream,
            publisher,
            layout: target.map_or(Layout::Unrouted(1), Layout::Placed),
            node_drawn: target.is_some() && config.node.is_none(),
            source,
            policy,
            subscriptions: Mutex::new(Vec::new()),
            feeds: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
            carrier: ArcSwapOption::empty(),
            retired: Mutex::default(),
            running: AtomicBool::new(false),
        })
    }

    pub fn signals(&self) -> &Arc<Signals> {
        &self.policy.signals
    }

    pub fn register(self: &Arc<Self>, topic: Topic) -> Result<Arc<Shared>, Error> {
        let shared = {
            let mut subscriptions = lock(&self.subscriptions);
            subscriptions.retain(|shared| shared.is_open());
            if subscriptions.iter().any(|shared| shared.topic == topic) {
                return Err(Error::AlreadySubscribed(topic.get()));
            }
            let shared = Arc::new(Shared::new(topic, Arc::clone(&self.policy.signals)));
            subscriptions.push(Arc::clone(&shared));
            shared
        };
        self.start();
        self.policy.signals.streams.notify_one();
        Ok(shared)
    }

    fn start(self: &Arc<Self>) {
        if !self.running.swap(true, Ordering::AcqRel) {
            tokio::spawn(run(Arc::downgrade(self)));
        }
    }

    pub async fn publisher(
        self: &Arc<Self>,
        topic: Topic,
        deadline: Instant,
    ) -> Result<(SocketAddr, Arc<Conn>), Error> {
        let (addr, stamp) = match (&self.source, self.mode) {
            (Source::Cluster(cluster), StreamMode::Owner) => {
                let home = cluster.home(u64::from(topic.get()));
                (home.ok_or(Error::Unavailable)?, None)
            }
            (Source::Cluster(cluster), StreamMode::Everywhere) => {
                if let Some(carried) = self.carried() {
                    return Ok(carried);
                }
                let generation = self.generation.load(Ordering::Acquire);
                let view = cluster.view();
                let home = view.home(self.publisher).ok_or(Error::Unavailable)?;
                let view = Arc::downgrade(&view);
                (home, Some(Stamp { view, generation }))
            }
            (Source::Static { addr, .. }, _) => {
                if let Some(carried) = self.carried() {
                    return Ok(carried);
                }
                let generation = self.generation.load(Ordering::Acquire);
                let view = Weak::new();
                (*addr, Some(Stamp { view, generation }))
            }
        };
        let (feed, fresh) = self.feed(addr)?;
        if fresh {
            feed.settle(deadline).await;
        }
        if let Some(conn) = feed.pick(None) {
            let conn = Arc::clone(conn);
            if let Some(stamp) = stamp {
                self.carry(stamp, addr, &conn);
            }
            return Ok((addr, conn));
        }
        match (&self.source, self.mode) {
            (Source::Cluster(cluster), StreamMode::Everywhere) => {
                Box::pin(self.fail_over(cluster, addr, deadline)).await
            }
            _ => Err(Error::Unavailable),
        }
    }

    fn is_current(&self, stamp: &Stamp) -> bool {
        stamp.generation == self.generation.load(Ordering::Acquire)
            && match &self.source {
                Source::Cluster(cluster) => cluster.is_current(&stamp.view),
                Source::Static { .. } => true,
            }
    }

    fn carried(&self) -> Option<(SocketAddr, Arc<Conn>)> {
        let carrier = self.carrier.load();
        let carrier = carrier.as_ref()?;
        (self.is_current(&carrier.stamp) && carrier.conn.is_healthy())
            .then(|| (carrier.addr, Arc::clone(&carrier.conn)))
    }

    fn carry(&self, stamp: Stamp, addr: SocketAddr, conn: &Arc<Conn>) {
        if self.is_current(&stamp) {
            self.carrier.store(Some(Arc::new(Carrier {
                stamp,
                addr,
                conn: Arc::clone(conn),
            })));
        }
    }

    async fn fail_over(
        self: &Arc<Self>,
        cluster: &Cluster,
        first: SocketAddr,
        deadline: Instant,
    ) -> Result<(SocketAddr, Arc<Conn>), Error> {
        let healthy = {
            let feeds = lock(&self.feeds);
            cluster
                .home_where(self.publisher, |addr| {
                    feeds.get(&addr).is_some_and(|feed| feed.healthy() > 0)
                })
                .and_then(|addr| Some((addr, Arc::clone(feeds.get(&addr)?.pick(None)?))))
        };
        if let Some(carrier) = healthy {
            return Ok(carrier);
        }
        let mut tried = vec![first];
        while Instant::now() < deadline {
            let Some(addr) = cluster.home_where(self.publisher, |addr| {
                !tried.contains(&addr) && cluster.reachable(addr)
            }) else {
                break;
            };
            tried.push(addr);
            let (feed, _) = self.feed(addr)?;
            feed.settle(deadline).await;
            if let Some(conn) = feed.pick(None) {
                return Ok((addr, Arc::clone(conn)));
            }
        }
        Err(Error::Unavailable)
    }

    fn feed(self: &Arc<Self>, addr: SocketAddr) -> Result<(Arc<Server>, bool), Error> {
        let mut feeds = lock(&self.feeds);
        if let Some(feed) = feeds.get(&addr) {
            return Ok((Arc::clone(feed), false));
        }
        let opener = match &self.source {
            Source::Static { opener, .. } => opener.clone(),
            Source::Cluster(cluster) => cluster.opener(addr)?,
        };
        let feed = Server::start(addr, self.layout, Arc::clone(&self.policy), opener, None);
        feeds.insert(addr, Arc::clone(&feed));
        self.generation.fetch_add(1, Ordering::Release);
        drop(feeds);
        self.start();
        Ok((feed, true))
    }

    pub fn servers(&self, topic: Topic) -> Vec<SocketAddr> {
        match (&self.source, self.mode) {
            (Source::Static { addr, .. }, _) => vec![*addr],
            (Source::Cluster(cluster), StreamMode::Everywhere) => cluster.placing(),
            (Source::Cluster(cluster), StreamMode::Owner) => {
                cluster.home(u64::from(topic.get())).into_iter().collect()
            }
        }
    }

    fn reconcile(self: &Arc<Self>) -> bool {
        let subscriptions: Vec<Arc<Shared>> = {
            let mut subscriptions = lock(&self.subscriptions);
            subscriptions.retain(|shared| shared.is_open());
            subscriptions.clone()
        };
        let mut wanted: HashMap<SocketAddr, Vec<Arc<Shared>>> = HashMap::new();
        for shared in subscriptions {
            for addr in self.servers(shared.topic) {
                wanted.entry(addr).or_default().push(Arc::clone(&shared));
            }
        }
        for (addr, here) in &mut wanted {
            here.sort_unstable_by_key(Arc::as_ptr);
            here.dedup_by(|a, b| Arc::ptr_eq(a, b));
            let _ = self.feed(*addr);
        }
        let feeds: Vec<(SocketAddr, Arc<Server>)> = lock(&self.feeds)
            .iter()
            .map(|(addr, feed)| (*addr, Arc::clone(feed)))
            .collect();
        let view = match &self.source {
            Source::Cluster(cluster) => Some((cluster, cluster.view())),
            Source::Static { .. } => None,
        };
        let now = Instant::now();
        let mut later = false;
        for (addr, feed) in feeds {
            let Some(conn) = feed.feed() else {
                continue;
            };
            let here = wanted.get(&addr).map_or(&[][..], Vec::as_slice);
            let settled = conn.reconcile(
                addr,
                here,
                |shared| match here.binary_search_by_key(&ptr::from_ref(shared), Arc::as_ptr) {
                    Ok(_) => Want::Keep,
                    Err(_) => Want::Linger,
                },
                now,
            );
            later |= settled == Settled::Later;
            if let Some((cluster, view)) = &view
                && conn.routeless()
                && !view.places(addr)
            {
                let mut feeds = lock(&self.feeds);
                feeds.remove(&addr);
                self.generation.fetch_add(1, Ordering::Release);
                lock(&self.retired).push(Arc::clone(&feed));
                drop(feeds);
                cluster.depart(view, feed);
            }
        }
        lock(&self.retired).fold() || later
    }

    pub fn stats(&self) -> Stats {
        let mut total = Stats::default();
        let feeds = lock(&self.feeds);
        total += lock(&self.retired).stats();
        for feed in feeds.values() {
            total += feed.stats();
        }
        drop(feeds);
        for shared in lock(&self.subscriptions).iter() {
            total.stream_drops += shared.dropped();
        }
        total.feed_node_drawn = u64::from(self.node_drawn);
        total
    }

    pub fn shutdown(&self) {
        for shared in lock(&self.subscriptions).drain(..) {
            shared.client_gone();
        }
        let mut feeds = lock(&self.feeds);
        for (_, feed) in feeds.drain() {
            feed.shutdown(Close::Graceful);
        }
        self.generation.fetch_add(1, Ordering::Release);
        drop(feeds);
        self.policy.signals.streams.notify_one();
    }
}

async fn run(streams: Weak<Streams>) {
    loop {
        let (later, signals) = match streams.upgrade() {
            Some(streams) => (streams.reconcile(), Arc::clone(streams.signals())),
            None => return,
        };
        if later {
            tokio::select! {
                () = signals.streams.notified() => {}
                () = tokio::time::sleep(TICK) => {}
            }
        } else {
            signals.streams.notified().await;
        }
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-client/stream/manager.rs"]
mod tests;
