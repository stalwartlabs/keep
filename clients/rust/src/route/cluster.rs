/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::mem;
use std::net::SocketAddr;
use std::num::NonZeroU8;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use keep_proto::{Command, KeyHash, Liveness, Member, Placement, Topology};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::timeout;

use super::retired::Retired;
use super::server::{Layout, Policy, Server};
use super::vote::{self, Choice, Placing};
use crate::answer::{AnswerValue, Expect};
use crate::config::Routing;
use crate::conn::{Close, Conn, Opener, Openers, Signals};
use crate::error::Error;
use crate::stats::Stats;

const MIN_REFRESH: Duration = Duration::from_millis(50);
const SEED_WAIT_STEP: Duration = Duration::from_millis(5);
const SLOW_REFRESH: Duration = Duration::from_secs(1);
const PUSH_ATTEMPTS: usize = 8;
const PROBES: usize = 64;
const PROBE_CONNECT: Duration = Duration::from_millis(250);

pub(crate) struct Cluster {
    seeds: Box<[SocketAddr]>,
    view: ArcSwap<View>,
    servers: Mutex<HashMap<SocketAddr, Arc<Server>>>,
    retiring: Mutex<Vec<Weak<Server>>>,
    retired: Mutex<Retired>,
    policy: Arc<Policy>,
    openers: Openers,
    routing: Routing,
    connections: usize,
    shard_connections: NonZeroU8,
    rounds: watch::Sender<u64>,
    closed: AtomicBool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Outcome {
    Settled,
    Unsettled,
}

#[derive(Default)]
pub(crate) struct View {
    placement: Option<Placement>,
    servers: Box<[Option<Arc<Server>>]>,
}

impl View {
    pub fn home(&self, point: u64) -> Option<SocketAddr> {
        let owner = self.placement.as_ref()?.rendezvous(point)?;
        Some(SocketAddr::new(owner.member.ip, owner.member.port))
    }

    pub fn places(&self, addr: SocketAddr) -> bool {
        self.servers
            .iter()
            .flatten()
            .any(|server| server.addr() == addr)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Cluster {
    pub async fn start(
        seeds: Box<[SocketAddr]>,
        policy: Arc<Policy>,
        openers: Openers,
        routing: Routing,
        connections: usize,
        shard_connections: NonZeroU8,
        deadline: Instant,
    ) -> Result<Arc<Cluster>, Error> {
        let mut seeds: Vec<SocketAddr> = seeds.iter().copied().map(vote::canonical).collect();
        seeds.sort_unstable();
        seeds.dedup();
        let cluster = Arc::new(Cluster {
            seeds: seeds.into_boxed_slice(),
            view: ArcSwap::from_pointee(View::default()),
            servers: Mutex::new(HashMap::new()),
            retiring: Mutex::new(Vec::new()),
            retired: Mutex::default(),
            policy,
            openers,
            routing,
            connections,
            shard_connections,
            rounds: watch::Sender::new(0),
            closed: AtomicBool::new(false),
        });
        cluster.seed()?;
        loop {
            cluster.refresh().await;
            if cluster.view.load().placement.is_some() {
                break;
            }
            if Instant::now() >= deadline {
                cluster.shutdown();
                return Err(Error::Unavailable);
            }
            tokio::time::sleep(SEED_WAIT_STEP.max(MIN_REFRESH)).await;
        }
        tokio::spawn(watch(Arc::downgrade(&cluster)));
        if cluster.policy.signals.wanted() {
            cluster.policy.signals.topology.notify_one();
        }
        Ok(cluster)
    }

    fn seed(&self) -> Result<(), Error> {
        let mut servers = lock(&self.servers);
        for &addr in self.seeds.iter() {
            if let Entry::Vacant(slot) = servers.entry(addr) {
                slot.insert(Server::start(
                    addr,
                    Layout::Unrouted(1),
                    Arc::clone(&self.policy),
                    self.openers.for_addr(addr)?,
                    None,
                ));
            }
        }
        Ok(())
    }

    #[inline]
    pub fn target(&self, hash: Option<KeyHash>) -> Option<Arc<Conn>> {
        let view = self.view.load();
        let server = match (hash, view.placement.as_ref()) {
            (Some(hash), Some(placement)) => {
                let (owner, _) = placement.route(hash)?;
                view.servers.get(usize::from(owner.index))?.as_ref()?
            }
            _ => view
                .servers
                .iter()
                .flatten()
                .find(|server| server.healthy() > 0)?,
        };
        server.pick(hash).cloned()
    }

    pub fn may_move(&self, hash: KeyHash) -> bool {
        let view = self.view.load();
        let owner = view
            .placement
            .as_ref()
            .and_then(|placement| placement.route(hash))
            .map(|(owner, _)| usize::from(owner.index));
        view.servers
            .iter()
            .enumerate()
            .filter(|(index, _)| Some(*index) != owner)
            .any(|(_, server)| server.as_ref().is_some_and(|server| server.healthy() > 0))
    }

    pub fn placing(&self) -> Vec<SocketAddr> {
        let view = self.view.load();
        view.servers
            .iter()
            .flatten()
            .map(|server| server.addr())
            .collect()
    }

    pub fn home(&self, point: u64) -> Option<SocketAddr> {
        self.view.load().home(point)
    }

    pub fn home_where(
        &self,
        point: u64,
        mut accept: impl FnMut(SocketAddr) -> bool,
    ) -> Option<SocketAddr> {
        let view = self.view.load();
        let owner = view.placement.as_ref()?.rendezvous_where(point, |member| {
            accept(SocketAddr::new(member.ip, member.port))
        })?;
        Some(SocketAddr::new(owner.member.ip, owner.member.port))
    }

    pub fn reachable(&self, addr: SocketAddr) -> bool {
        self.view
            .load()
            .servers
            .iter()
            .flatten()
            .any(|server| server.addr() == addr && server.healthy() > 0)
    }

    pub fn opener(&self, addr: SocketAddr) -> Result<Opener, Error> {
        self.openers.for_addr(addr)
    }

    pub fn servers(&self) -> Vec<Arc<Server>> {
        self.view.load().servers.iter().flatten().cloned().collect()
    }

    pub fn stats(&self) -> Stats {
        let servers = lock(&self.servers);
        let mut total = lock(&self.retired).stats();
        for server in servers.values() {
            total += server.stats();
        }
        drop(servers);
        total.refreshes += self.policy.signals.started();
        total
    }

    pub fn shutdown(&self) {
        self.closed.store(true, Ordering::Relaxed);
        for server in lock(&self.servers).values() {
            server.shutdown(Close::Graceful);
        }
        self.policy.signals.topology.notify_one();
    }

    pub async fn moved(&self, wait: Duration) -> bool {
        let ticket = self.policy.signals.ticket();
        if *self.rounds.borrow() >= ticket {
            return false;
        }
        let mut rounds = self.rounds.subscribe();
        let _ = timeout(wait, rounds.wait_for(|done| *done >= ticket)).await;
        true
    }

    pub fn nudge(&self) {
        self.policy.signals.nudge();
    }

    fn wants_refresh(&self) -> bool {
        self.policy.signals.wanted()
            || lock(&self.servers)
                .values()
                .any(|server| server.epochs().push_pending())
    }

    fn drop_pushes(&self) {
        for server in lock(&self.servers).values() {
            server.epochs().drop_push();
        }
    }

    async fn refresh(&self) {
        let round = self.policy.signals.start_round();
        if !self.closed.load(Ordering::Relaxed) && self.agree().await == Outcome::Unsettled {
            self.policy.signals.unsettled();
        }
        self.rounds.send_replace(round);
    }

    async fn agree(&self) -> Outcome {
        if lock(&self.servers).is_empty() && self.seed().is_err() {
            return Outcome::Settled;
        }
        let known: Vec<Arc<Server>> = lock(&self.servers).values().cloned().collect();
        let wait = self.policy.attempt_timeout;
        let installed = self
            .view
            .load()
            .placement
            .as_ref()
            .map(|placement| Placing::of(placement.topology()));
        let signals = &*self.policy.signals;
        let mut reports = fetch_all(&known, wait, signals, |reports, outstanding| {
            vote::decided(reports, installed.as_ref(), outstanding)
        })
        .await;
        let mut missed = reports.len() < known.len();
        let mut choice = vote::choose(&reports, installed.as_ref());
        if !matches!(choice, Choice::Agreed(_)) {
            let probes = self.probes(&known, &reports);
            if !probes.is_empty() {
                let settled = Instant::now() + wait.min(PROBE_CONNECT);
                let mut settling = JoinSet::new();
                for server in &probes {
                    let server = Arc::clone(server);
                    settling.spawn(async move { server.settle(settled).await });
                }
                while settling.join_next().await.is_some() {}
                let answered = fetch_all(&probes, wait, signals, |_, _| false).await;
                missed |= answered.len() < probes.len();
                reports.extend(answered);
                probes
                    .iter()
                    .for_each(|server| server.shutdown(Close::Graceful));
                choice = vote::choose(&reports, installed.as_ref());
            }
        }
        let unsettled = if missed {
            Outcome::Unsettled
        } else {
            Outcome::Settled
        };
        let (index, outcome) = match choice {
            Choice::Agreed(index) => (index, Outcome::Settled),
            Choice::Best(index) if installed.is_none() => (index, unsettled),
            Choice::Best(_) | Choice::Nothing => return unsettled,
        };
        let Some((_, topology)) = reports.into_iter().nth(index) else {
            return outcome;
        };
        self.reset_departed(&topology);
        if installed.is_some_and(|installed| installed == Placing::of(&topology)) {
            return outcome;
        }
        let Ok(placement) = Placement::new(topology) else {
            return outcome;
        };
        let (members, fresh) = self.members(&placement);
        let settled = Instant::now() + wait;
        let mut settling = JoinSet::new();
        for server in fresh {
            settling.spawn(async move { server.settle(settled).await });
        }
        while settling.join_next().await.is_some() {}
        self.install(placement, members);
        outcome
    }

    fn probes(
        &self,
        known: &[Arc<Server>],
        reports: &[(SocketAddr, Topology)],
    ) -> Vec<Arc<Server>> {
        let mut seen: Vec<SocketAddr> = known
            .iter()
            .map(|server| vote::canonical(server.addr()))
            .collect();
        seen.sort_unstable();
        let mut wanted: Vec<SocketAddr> = self
            .seeds
            .iter()
            .copied()
            .map(vote::canonical)
            .chain(
                reports
                    .iter()
                    .flat_map(|(_, topology)| topology.members().iter().map(vote::address)),
            )
            .filter(|addr| seen.binary_search(addr).is_err())
            .collect();
        wanted.sort_unstable();
        wanted.dedup();
        wanted
            .into_iter()
            .take(PROBES)
            .filter_map(|addr| {
                let opener = self.openers.for_addr(addr).ok()?;
                Some(Server::start(
                    addr,
                    Layout::Unrouted(1),
                    Arc::clone(&self.policy),
                    opener,
                    None,
                ))
            })
            .collect()
    }

    fn members(&self, placement: &Placement) -> (Vec<Option<Arc<Server>>>, Vec<Arc<Server>>) {
        let servers = lock(&self.servers);
        let mut fresh = Vec::new();
        let mut members = Vec::with_capacity(placement.topology().members().len());
        for member in placement.topology().members() {
            if !member.places() {
                members.push(None);
                continue;
            }
            let addr = SocketAddr::new(member.ip, member.port);
            let layout = match self.routing {
                Routing::Shard => Layout::Routed {
                    shards: member.shards,
                    per_shard: self.shard_connections,
                },
                Routing::None => Layout::Unrouted(self.connections),
            };
            let server = match servers.get(&addr) {
                Some(server) if server.layout() == layout => Arc::clone(server),
                _ => match self.openers.for_addr(addr) {
                    Ok(opener) => {
                        let server =
                            Server::start(addr, layout, Arc::clone(&self.policy), opener, None);
                        fresh.push(Arc::clone(&server));
                        server
                    }
                    Err(_) => {
                        members.push(None);
                        continue;
                    }
                },
            };
            members.push(Some(server));
        }
        (members, fresh)
    }

    fn reset_departed(&self, topology: &Topology) {
        lock(&self.retiring).retain(|server| match server.upgrade() {
            Some(server) if alive_at(topology, server.addr()).next().is_some() => true,
            Some(server) => {
                server.shutdown(Close::Reset);
                false
            }
            None => false,
        });
    }

    fn install(&self, placement: Placement, members: Vec<Option<Arc<Server>>>) {
        let kept: HashMap<SocketAddr, Arc<Server>> = members
            .iter()
            .flatten()
            .map(|server| (server.addr(), Arc::clone(server)))
            .collect();
        let mut servers = lock(&self.servers);
        let mut departed = mem::replace(&mut *servers, kept);
        departed.retain(|addr, server| {
            servers
                .get(addr)
                .is_none_or(|kept| !Arc::ptr_eq(kept, server))
        });
        let mut retired = lock(&self.retired);
        retired.fold();
        departed
            .values()
            .for_each(|server| retired.push(Arc::clone(server)));
        drop(retired);
        drop(servers);
        let view = Arc::new(View {
            placement: Some(placement),
            servers: members.into_boxed_slice(),
        });
        self.view.store(Arc::clone(&view));
        self.policy.signals.viewed();
        view.servers
            .iter()
            .flatten()
            .chain(departed.values())
            .for_each(|server| server.gate().wake());
        for server in departed.into_values() {
            self.depart(&view, server);
        }
        self.policy.signals.streams.notify_one();
    }

    pub fn view(&self) -> Arc<View> {
        self.view.load_full()
    }

    pub fn is_current(&self, view: &Weak<View>) -> bool {
        ptr::eq(Arc::as_ptr(&self.view.load()), view.as_ptr())
    }

    pub fn depart(&self, view: &View, server: Arc<Server>) {
        let drains = view
            .placement
            .as_ref()
            .is_some_and(|placement| drains(placement.topology(), server.addr()));
        match drains {
            true => {
                lock(&self.retiring).push(Arc::downgrade(&server));
                server.retire();
            }
            false => server.shutdown(Close::Reset),
        }
    }
}

fn alive_at(topology: &Topology, addr: SocketAddr) -> impl Iterator<Item = &Member> {
    topology.members().iter().filter(move |member| {
        SocketAddr::new(member.ip, member.port) == addr && member.liveness != Liveness::Dead
    })
}

fn drains(topology: &Topology, addr: SocketAddr) -> bool {
    alive_at(topology, addr).any(|member| !member.places())
}

async fn fetch_all(
    servers: &[Arc<Server>],
    wait: Duration,
    signals: &Signals,
    decided: impl Fn(&[(SocketAddr, Topology)], usize) -> bool,
) -> Vec<(SocketAddr, Topology)> {
    let mut fetches = JoinSet::new();
    for (index, server) in servers.iter().enumerate() {
        fetches.spawn(fetch_at(index, Arc::clone(server), wait));
    }
    let mut asking = vec![true; servers.len()];
    let mut voters: Vec<usize> = Vec::with_capacity(servers.len());
    let mut reports = Vec::with_capacity(servers.len());
    let mut woken = false;
    while !fetches.is_empty() {
        tokio::select! {
            Some(fetched) = fetches.join_next() => {
                if let Ok((index, report)) = fetched {
                    if let Some(asked) = asking.get_mut(index) {
                        *asked = false;
                    }
                    if let Some(report) = report {
                        voters.push(index);
                        reports.push(report);
                    }
                }
            }
            () = signals.topology.notified() => {
                woken = true;
                for ((index, server), asked) in servers.iter().enumerate().zip(asking.iter_mut()) {
                    if *asked || !server.epochs().push_pending() {
                        continue;
                    }
                    if let Some(at) = voters.iter().position(|voter| *voter == index) {
                        voters.swap_remove(at);
                        reports.swap_remove(at);
                        *asked = true;
                        fetches.spawn(fetch_at(index, Arc::clone(server), wait));
                    }
                }
            }
        }
        if !fetches.is_empty() && decided(&reports, fetches.len()) {
            fetches.detach_all();
            break;
        }
    }
    if woken {
        signals.topology.notify_one();
    }
    reports
}

async fn fetch_at(
    index: usize,
    server: Arc<Server>,
    wait: Duration,
) -> (usize, Option<(SocketAddr, Topology)>) {
    (index, fetch(server, wait).await)
}

async fn fetch(server: Arc<Server>, wait: Duration) -> Option<(SocketAddr, Topology)> {
    let life = server.epochs().life();
    let report = ask(&server, wait).await;
    match &report {
        Some((_, topology)) => server.epochs().fetched(life, topology.epoch()),
        None => server.epochs().missed(),
    }
    report
}

async fn ask(server: &Server, wait: Duration) -> Option<(SocketAddr, Topology)> {
    let conn = Arc::clone(server.pick(None)?);
    let answer = timeout(wait, conn.call(&Command::Topology, Expect::Bytes))
        .await
        .ok()?
        .ok()?;
    let topology = match answer.value {
        AnswerValue::Bytes(bytes) => Topology::decode(&bytes).ok()?,
        AnswerValue::None | AnswerValue::Counter(_) => return None,
    };
    Some((server.addr(), topology))
}

async fn watch(cluster: Weak<Cluster>) {
    let Some(signals) = cluster
        .upgrade()
        .map(|cluster| Arc::clone(&cluster.policy.signals))
    else {
        return;
    };
    let mut last: Option<Instant> = None;
    loop {
        signals.topology.notified().await;
        let mut attempts = 0;
        loop {
            let floor = {
                let Some(cluster) = cluster.upgrade() else {
                    return;
                };
                if cluster.closed.load(Ordering::Relaxed) {
                    return;
                }
                if !cluster.wants_refresh() {
                    break;
                }
                if attempts == PUSH_ATTEMPTS {
                    cluster.drop_pushes();
                }
                if attempts < PUSH_ATTEMPTS {
                    MIN_REFRESH
                } else {
                    SLOW_REFRESH
                }
            };
            if let Some(wait) = last.map(|at| floor.saturating_sub(at.elapsed()))
                && !wait.is_zero()
            {
                let woken = tokio::select! {
                    () = tokio::time::sleep(wait) => false,
                    () = signals.topology.notified() => true,
                };
                if woken {
                    attempts = 0;
                    continue;
                }
            }
            let Some(cluster) = cluster.upgrade() else {
                return;
            };
            attempts += 1;
            last = Some(Instant::now());
            cluster.refresh().await;
        }
    }
}
