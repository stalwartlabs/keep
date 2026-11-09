/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fs::File;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, UdpSocket};
use std::num::NonZeroU16;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use keep_cluster::{HandoffLimits, LockLimits, Start};
use keep_engine::{Cap, Eviction, MemoryUse, Pinned};
use keep_proto::{ClusterSecret, Liveness, Member, MemberStatus, NodeId, PeerSecret, Ttl};
use keep_rt::clock::Nanos;
use keep_rt::cpu::{CpuBudget, CpuList, pin_current_thread};
use keep_rt::listener::{Steering, shard_listeners};
use keep_rt::memory;
use keep_rt::mesh::{MeshConfig, Port, mesh};
use keep_rt::queues::RxQueues;
use keep_rt::shard::{LoopConfig, Shard};
use keep_rt::socket::{Applied, Tuning, grow_recv_buffer};
use keep_rt::stats::{LoopCounts, LoopStats};

use crate::cluster::{Published, cluster_time};
use crate::config::Settings;
use crate::control::{
    ClusterConfig, Control, ControlCounts, ControlService, ControlStats, Peer, secrets_differ,
};
use crate::metrics::{ShardCounts, ShardStats};
use crate::shard::{Limits, Msg, ShardService};
use crate::stream::Hub;
use crate::tls::ServerTls;

const CGROUP_V1_QUOTA: &str = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";

const COALESCE_TIMER_SLACK: Duration = Duration::from_micros(1);

const JOIN_POLL: Duration = Duration::from_millis(20);

const DEFAULT_MEMORY_PERCENT: usize = 75;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Pinning {
    Auto,
    Never,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Spread {
    Auto,
    Fixed(Steering),
}

#[derive(Clone, Debug)]
pub struct Config {
    pub listen: SocketAddr,
    pub advertise: Option<SocketAddr>,
    pub node: Option<NodeId>,
    pub shards: Option<usize>,
    pub cpus: Option<CpuList>,
    pub pinning: Pinning,
    pub spread: Spread,
    pub secret: ClusterSecret,
    pub memory: Option<usize>,
    pub max_lock_ttl: Ttl,
    pub max_value: usize,
    pub loop_config: LoopConfig,
    pub tuning: Tuning,
    pub start: Start,
    pub stream_coalesce: Duration,
    pub stream_coalesce_fanout: u32,
    pub cluster: Option<ClusterSetup>,
    pub tls: Option<ServerTls>,
    pub ready_file: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct ClusterSetup {
    pub members: Vec<Peer>,
    pub peer_listen: Option<SocketAddr>,
    pub peer_secret: PeerSecret,
    pub incarnation: u32,
    pub max_stop: Duration,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MemoryCap {
    Set { bytes: usize },
    Default { bytes: usize, budget: usize },
}

impl MemoryCap {
    fn resolve(set: Option<usize>) -> io::Result<MemoryCap> {
        match set {
            Some(bytes) => Ok(MemoryCap::Set { bytes }),
            None => memory::budget().map(MemoryCap::of_budget),
        }
    }

    fn of_budget(budget: usize) -> MemoryCap {
        MemoryCap::Default {
            bytes: budget / 100 * DEFAULT_MEMORY_PERCENT,
            budget,
        }
    }

    pub fn bytes(self) -> usize {
        match self {
            MemoryCap::Set { bytes } | MemoryCap::Default { bytes, .. } => bytes,
        }
    }

    pub fn default_percent() -> usize {
        DEFAULT_MEMORY_PERCENT
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Stopped {
    Shard(usize),
    Control,
}

struct ControlThread {
    thread: JoinHandle<io::Result<()>>,
    stats: Arc<ControlStats>,
}

pub type Counts = (ShardCounts, LoopCounts);

pub fn total(counts: impl IntoIterator<Item = Counts>) -> Counts {
    counts.into_iter().fold(
        Counts::default(),
        |(mut shard, mut loops), (counts, loop_counts)| {
            shard += counts;
            loops.accepted += loop_counts.accepted;
            loops.closed += loop_counts.closed;
            loops.turns += loop_counts.turns;
            loops.parks += loop_counts.parks;
            (shard, loops)
        },
    )
}

pub struct Running {
    pub addr: SocketAddr,
    pub settings: Settings,
    pub memory: MemoryCap,
    pub cpus: CpuList,
    pub pinned: bool,
    pub spin: Duration,
    pub hold: Option<Duration>,
    pub steering: Steering,
    pub unsteered: Option<String>,
    pub applied: Applied,
    pub heartbeat_buffer: Option<HeartbeatBuffer>,
    pub stats: Vec<Arc<ShardStats>>,
    pub loops: Vec<Arc<LoopStats>>,
    admin: Port<Msg>,
    threads: Vec<JoinHandle<io::Result<()>>>,
    control: Option<ControlThread>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct HeartbeatBuffer {
    pub wanted: usize,
    pub granted: usize,
}

impl HeartbeatBuffer {
    const ROUNDS: usize = 8;
    const DATAGRAM: usize = 2048;

    fn wanted(members: usize) -> usize {
        members.saturating_sub(1) * Self::ROUNDS * Self::DATAGRAM
    }
}

impl Running {
    pub fn drain(&mut self) -> io::Result<()> {
        let leaving = self
            .control
            .as_ref()
            .is_some_and(|control| !control.thread.is_finished());
        if leaving {
            let endpoint = usize::from(self.settings.shards.get());
            let _ = self.admin.parts().1.send(endpoint, Msg::Drain);
        } else {
            self.drain_shards();
        }
        self.admin.publish().map(drop)
    }

    fn drain_shards(&mut self) {
        for shard in 0..self.threads.len() {
            let _ = self.admin.parts().1.send(shard, Msg::Drain);
        }
    }

    pub fn stopped(&self) -> Option<Stopped> {
        if let Some(shard) = self.threads.iter().position(JoinHandle::is_finished) {
            return Some(Stopped::Shard(shard));
        }
        self.control
            .as_ref()
            .filter(|control| control.thread.is_finished())
            .map(|_| Stopped::Control)
    }

    pub fn join(mut self) -> io::Result<()> {
        let mut drained = false;
        while !self.threads.iter().all(JoinHandle::is_finished) {
            if !drained
                && self
                    .control
                    .as_ref()
                    .is_some_and(|control| control.thread.is_finished())
            {
                self.drain_shards();
                self.admin.publish()?;
                drained = true;
            }
            thread::sleep(JOIN_POLL);
        }
        let mut result = Ok(());
        for thread in self.threads.drain(..) {
            let outcome = thread
                .join()
                .unwrap_or_else(|_| Err(io::Error::other("shard thread panicked")));
            if let Err(err) = outcome {
                result = Err(err);
            }
        }
        if let Some(control) = self.control.take() {
            let endpoint = self.settings.shards.get().into();
            let _ = self.admin.parts().1.send(endpoint, Msg::Drain);
            self.admin.publish()?;
            let outcome = control
                .thread
                .join()
                .unwrap_or_else(|_| Err(io::Error::other("control thread panicked")));
            if let Err(err) = outcome {
                result = Err(err);
            }
        }
        result
    }

    pub fn control_counts(&self) -> Option<ControlCounts> {
        self.control.as_ref().map(|control| control.stats.counts())
    }

    pub fn counts(&self) -> Counts {
        total(self.shard_counts())
    }

    pub fn shard_counts(&self) -> impl Iterator<Item = Counts> + '_ {
        self.stats
            .iter()
            .zip(&self.loops)
            .map(|(shard, loops)| (shard.counts(), loops.counts()))
    }

    pub fn memory_use(&self) -> impl Iterator<Item = MemoryUse> + '_ {
        self.stats.iter().map(|shard| shard.memory())
    }

    pub fn cgroup_v1() -> bool {
        Path::new(CGROUP_V1_QUOTA).exists()
    }
}

fn own_entry(config: &Config) -> io::Result<Option<Peer>> {
    let Some(cluster) = &config.cluster else {
        return Ok(None);
    };
    let node = config
        .node
        .ok_or_else(|| io::Error::other("a cluster member needs --node"))?;
    let entry = cluster
        .members
        .iter()
        .find(|peer| peer.node == node)
        .copied()
        .ok_or_else(|| io::Error::other(format!("node {} is not in --members", node.0)))?;
    if config
        .shards
        .is_some_and(|shards| shards != usize::from(entry.shards.get()))
    {
        return Err(io::Error::other(format!(
            "--shards differs from the {} shards of this server's member entry",
            entry.shards
        )));
    }
    if config.advertise.is_some_and(|addr| addr != entry.client) {
        return Err(io::Error::other(
            "--advertise differs from the client address of this server's member entry",
        ));
    }
    if !secrets_differ(&config.secret, &cluster.peer_secret) {
        return Err(io::Error::other(
            "the peer secret must differ from the cluster secret",
        ));
    }
    Ok(Some(entry))
}

pub fn start(config: &Config) -> io::Result<Running> {
    let budget = CpuBudget::detect()?;
    let cpus = config.cpus.clone().unwrap_or_else(|| budget.cpus.clone());
    let entry = own_entry(config)?;
    let shards = match entry {
        Some(entry) if usize::from(entry.shards.get()) > cpus.len() => {
            return Err(io::Error::other(format!(
                "this server's member entry lists {} shards for {} CPUs",
                entry.shards,
                cpus.len()
            )));
        }
        Some(entry) => usize::from(entry.shards.get()),
        None => config
            .shards
            .unwrap_or_else(|| budget.shards())
            .clamp(1, cpus.len().max(1)),
    };
    let shard_count = NonZeroU16::new(u16::try_from(shards).map_err(io::Error::other)?)
        .ok_or_else(|| io::Error::other("no shards"))?;
    let pinned = config.pinning == Pinning::Auto && budget.exclusive() && cpus.len() >= shards;
    let (steering, unsteered) = match config.spread {
        Spread::Fixed(steering) => (steering, None),
        Spread::Auto if pinned => {
            let shard_cpus = cpus.as_slice().get(..shards).unwrap_or_default();
            match RxQueues::detect().map(|queues| queues.mismatch(shard_cpus)) {
                Some(None) => (Steering::Cpu, None),
                Some(Some((cpu, queues))) => (
                    Steering::Hash,
                    Some(format!("CPU {cpu} takes {queues} receive queues, not one")),
                ),
                None => (Steering::Hash, Some("no receive queue map".to_owned())),
            }
        }
        Spread::Auto => (Steering::Hash, Some("shards are not pinned".to_owned())),
    };
    let group = shard_listeners(
        config.listen,
        shards,
        cpus.as_slice(),
        steering,
        &config.tuning,
    )?;
    let addr = group
        .listeners
        .first()
        .ok_or_else(|| io::Error::other("no listener"))?
        .local_addr()?;
    let node = match config.node {
        Some(node) => node,
        None => NodeId(getrandom::u64().map_err(io::Error::other)?),
    };
    let memory = MemoryCap::resolve(config.memory)?;
    let settings = Settings {
        node,
        secret: config.secret,
        shards: shard_count,
        advertise: entry.map_or(config.advertise.unwrap_or(addr), |entry| entry.client),
        limits: Limits {
            max_lock_ttl: config.max_lock_ttl,
            max_value: config.max_value,
        },
        cap: Some(Cap {
            bytes: memory.bytes() / shards,
            eviction: Eviction::Count,
            pinned: Pinned::STALWART,
        }),
        start: Nanos::now(),
        stream_coalesce: config.stream_coalesce,
        stream_coalesce_fanout: config.stream_coalesce_fanout,
    };
    let limits = LockLimits::new(
        config.max_lock_ttl.secs(),
        LockLimits::DEFAULT.self_fence(),
        config
            .cluster
            .as_ref()
            .map_or(Duration::ZERO, |cluster| cluster.max_stop),
        None,
    )
    .map_err(io::Error::other)?;
    let hold = match &config.cluster {
        Some(_) => Some(handoff_hold(&limits, &HandoffLimits::DEFAULT)?),
        None => None,
    };
    let me = Member {
        id: settings.node,
        ip: settings.advertise.ip(),
        port: settings.advertise.port(),
        shards: settings.shards,
        status: MemberStatus::Active,
        liveness: Liveness::Alive,
    };
    let member = config.cluster.as_ref().zip(entry);
    let control = match member {
        Some((cluster, entry)) => {
            let control = Control::start(
                ClusterConfig {
                    me: node,
                    members: cluster.members.clone(),
                    peer_secret: cluster.peer_secret,
                    limits,
                    start: config.start,
                    incarnation: cluster.incarnation,
                },
                cluster_time(settings.start),
            )?;
            let listen = cluster.peer_listen.unwrap_or_else(|| {
                let any = match entry.peer.ip() {
                    IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                };
                SocketAddr::new(any, entry.peer.port())
            });
            let socket = UdpSocket::bind(listen)?;
            socket.set_nonblocking(true)?;
            let wanted = HeartbeatBuffer::wanted(cluster.members.len());
            let buffer = HeartbeatBuffer {
                wanted,
                granted: grow_recv_buffer(&socket, wanted)?,
            };
            let peers = TcpListener::bind(listen)?;
            config.tuning.apply(&peers)?;
            Some((control, socket, peers, buffer))
        }
        None => None,
    };
    let published = match &control {
        Some((control, _, _, _)) => control.published().clone(),
        None => Arc::new(Published::single(
            me,
            &limits,
            config.start,
            settings.start,
        )?),
    };
    let seed = getrandom::u64().map_err(io::Error::other)?;
    let mut ports = mesh::<Msg>(shards + 2, MeshConfig::DEFAULT)?;
    let admin = ports
        .pop()
        .ok_or_else(|| io::Error::other("no admin port"))?;
    let control_port = ports
        .pop()
        .ok_or_else(|| io::Error::other("no control port"))?;
    let mut stats = Vec::with_capacity(shards);
    let mut loops = Vec::with_capacity(shards);
    let mut threads = Vec::with_capacity(shards);
    let hub = Arc::new(Hub::new(settings.shards));
    let spin = effective_spin(
        config.loop_config.spin,
        pinned,
        budget.quota,
        Running::cgroup_v1(),
    );
    let loop_config = LoopConfig {
        spin,
        timer_slack: (!config.stream_coalesce.is_zero()).then_some(COALESCE_TIMER_SLACK),
        ..config.loop_config
    };
    for ((id, listener), port) in (0u16..).zip(group.listeners).zip(ports) {
        let shard_stats = Arc::new(ShardStats::default());
        let loop_stats = Arc::new(LoopStats::default());
        stats.push(shard_stats.clone());
        loops.push(loop_stats.clone());
        let cpu = cpus.get(usize::from(id)).filter(|_| pinned);
        let settings = settings.clone();
        let published = published.clone();
        let hub = hub.clone();
        let tls = config.tls.as_ref().map(|tls| tls.clients.clone());
        let thread = thread::Builder::new()
            .name(format!("keep-shard-{id}"))
            .spawn(move || {
                if let Some(cpu) = cpu {
                    pin_current_thread(cpu)?;
                }
                let service = ShardService::new(id, &settings, seed, shard_stats, published, hub);
                let shard = match tls {
                    Some(tls) => Shard::new(
                        loop_config,
                        Some(listener),
                        port,
                        service.over_tls(),
                        loop_stats,
                    )?
                    .tls(tls),
                    None => Shard::new(loop_config, Some(listener), port, service, loop_stats)?,
                };
                shard.run().map(drop)
            })?;
        threads.push(thread);
    }
    let heartbeat_buffer = control.as_ref().map(|(_, _, _, buffer)| *buffer);
    let control = match control {
        Some((control, socket, peers, _)) => {
            let stats = Arc::new(ControlStats::default());
            let mut service = ControlService::new(
                control,
                socket,
                shard_count,
                settings.secret.hasher(),
                stats.clone(),
            );
            if let Some(path) = config.ready_file.clone() {
                service.set_ready_file(path.into_boxed_path());
            }
            let loop_stats = Arc::new(LoopStats::default());
            let tls = config.tls.as_ref().and_then(|tls| tls.peers.clone());
            let thread = thread::Builder::new()
                .name("keep-control".into())
                .spawn(move || {
                    let shard = Shard::new(
                        LoopConfig::DEFAULT,
                        Some(peers),
                        control_port,
                        service,
                        loop_stats,
                    )?;
                    let mut shard = match tls {
                        Some(tls) => shard.tls(tls),
                        None => shard,
                    };
                    shard.watch(ControlService::heartbeat_fd)?;
                    shard.run()?.finish()
                })?;
            Some(ControlThread { thread, stats })
        }
        None => {
            if let Some(path) = &config.ready_file {
                File::create(path)?;
            }
            None
        }
    };
    Ok(Running {
        addr,
        settings,
        memory,
        cpus,
        pinned,
        spin,
        hold,
        steering,
        unsteered,
        applied: group.applied,
        heartbeat_buffer,
        stats,
        loops,
        admin,
        threads,
        control,
    })
}

fn effective_spin(wanted: Duration, pinned: bool, quota: Option<f64>, cgroup_v1: bool) -> Duration {
    if pinned && quota.is_none() && !cgroup_v1 {
        wanted
    } else {
        Duration::ZERO
    }
}

fn handoff_hold(limits: &LockLimits, handoff: &HandoffLimits) -> io::Result<Duration> {
    let max_hold = limits.max_hold();
    if max_hold < handoff.reply {
        return Err(io::Error::other(format!(
            "these lock limits hold a lost slot for at most {} ms, below the {} ms a handoff needs (its reply timeout); raise --max-lock-ttl",
            max_hold.as_millis(),
            handoff.reply.as_millis()
        )));
    }
    Ok(handoff.hold.min(max_hold))
}

pub struct Signals {
    set: libc::sigset_t,
}

impl Signals {
    pub fn block() -> io::Result<Signals> {
        unsafe {
            let mut set: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut set);
            libc::sigaddset(&mut set, libc::SIGINT);
            libc::sigaddset(&mut set, libc::SIGTERM);
            let result = libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
            if result != 0 {
                return Err(io::Error::from_raw_os_error(result));
            }
            Ok(Signals { set })
        }
    }

    pub fn wait(&self, timeout: Duration) -> bool {
        let timeout = libc::timespec {
            tv_sec: libc::time_t::try_from(timeout.as_secs()).unwrap_or(libc::time_t::MAX),
            tv_nsec: libc::c_long::from(timeout.subsec_nanos()),
        };
        let signal = unsafe { libc::sigtimedwait(&self.set, std::ptr::null_mut(), &timeout) };
        signal > 0
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-server/server.rs"]
mod tests;
