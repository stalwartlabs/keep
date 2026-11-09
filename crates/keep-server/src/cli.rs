/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::env;
use std::fmt::{self, Write};
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::builder::PossibleValue;
use clap::{CommandFactory, FromArgMatches, Parser, ValueEnum};
use keep_cluster::{HandoffLimits, Start};
use keep_engine::MemoryUse;
use keep_proto::{ClusterSecret, MAX_VALUE_LEN, NodeId, PeerSecret, Ttl};
use keep_rt::clock::{self, Nanos};
use keep_rt::cpu::CpuList;
use keep_rt::listener::Steering;
use keep_rt::shard::LoopConfig;
use keep_rt::socket::Tuning;
use keep_server::control::Peer;
use keep_server::server::{
    ClusterSetup, Config, Counts, MemoryCap, Pinning, Running, Signals, Spread, Stopped, start,
    total,
};
use keep_server::sources::{self, Alias};
use keep_server::tls::{ServerTls, TlsFiles};

const MAX_SPIN_US: u64 = 1_000;

const MAX_STREAM_COALESCE_US: u64 = 10_000;

const DEFAULT_STREAM_COALESCE_US: u64 = 100;

const DEFAULT_STREAM_COALESCE_FANOUT: u32 = 16;

const NANOS_PER_MICRO: u64 = 1_000;

const MAX_STOP_SECS: u64 = 600;

const SECRET_ENV: &str = "KEEP_SECRET";
const PEER_SECRET_ENV: &str = "KEEP_PEER_SECRET";
const ALIASES: [Alias; 2] = [
    Alias {
        arg: "secret_file",
        var: SECRET_ENV,
    },
    Alias {
        arg: "peer_secret_file",
        var: PEER_SECRET_ENV,
    },
];
const TUNABLES_ENV: &str = "GLIBC_TUNABLES";
const HUGETLB: &str = "glibc.malloc.hugetlb";
const RESTARTED_ENV: &str = "KEEP_HUGETLB_RESTARTED";
const WATCH: Duration = Duration::from_secs(1);
const INCARNATION_HEADROOM: u32 = 1 << 20;
const CORRECTION_WARN_PPM: i64 = 500;

enum HugePages {
    Set,
    Restarted,
    Secure,
    Failed(io::Error),
}

fn ensure_huge_pages() -> HugePages {
    let current = env::var_os(TUNABLES_ENV).unwrap_or_default();
    if current
        .as_bytes()
        .split(|&byte| byte == b':')
        .any(|tunable| tunable.starts_with(HUGETLB.as_bytes()))
    {
        return HugePages::Set;
    }
    if env::var_os(RESTARTED_ENV).is_some() {
        return HugePages::Restarted;
    }
    if unsafe { libc::getauxval(libc::AT_SECURE) } != 0 {
        return HugePages::Secure;
    }
    let mut tunables = current;
    if !tunables.is_empty() {
        tunables.push(":");
    }
    tunables.push(HUGETLB);
    tunables.push("=1");
    let exe = match env::current_exe() {
        Ok(exe) => exe,
        Err(err) => return HugePages::Failed(err),
    };
    let mut args = env::args_os();
    let name = args.next().unwrap_or_default();
    HugePages::Failed(
        Command::new(exe)
            .arg0(name)
            .args(args)
            .env(TUNABLES_ENV, tunables)
            .env(RESTARTED_ENV, "1")
            .exec(),
    )
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
enum PinArg {
    Auto,
    Never,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
enum TlsArg {
    Rpk,
    Ca,
    Off,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
enum StartArg {
    Rejoin,
    Bootstrap,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
enum SteerArg {
    Auto,
    Hash,
    Cpu,
}

#[derive(Debug, Parser)]
#[command(
    name = "keep",
    version,
    about = "Stalwart Keep: an ephemeral key-value and event stream server for clustered applications"
)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:7000")]
    listen: SocketAddr,
    #[arg(
        long,
        help = "address clients reach this server at (topology); default: the bound address"
    )]
    advertise: Option<SocketAddr>,
    #[arg(long, help = "node id (u64); random when absent")]
    node: Option<u64>,
    #[arg(
        long,
        help = "shard count; default: the CPU budget (cpuset and cgroup cpu.max)"
    )]
    shards: Option<usize>,
    #[arg(
        long,
        help = "CPUs for the shards, as in 0-3,8; default: the allowed CPUs"
    )]
    cpus: Option<CpuList>,
    #[arg(long, value_enum, default_value = "auto")]
    pin: PinArg,
    #[arg(
        long,
        value_enum,
        default_value = "auto",
        help = "auto: CPU steering when every shard is pinned and takes one receive queue"
    )]
    steering: SteerArg,
    #[arg(
        long,
        help = "file holding the cluster secret, 32 hex digits; default: the KEEP_SECRET variable"
    )]
    secret_file: Option<PathBuf>,
    #[arg(
        long,
        help = "memory cap in bytes, split evenly between the shards; default: 75% of the memory budget, the smaller of the cgroup's memory.max and the total memory, read once at start"
    )]
    memory: Option<usize>,
    #[arg(long, default_value_t = 600, help = "longest lock TTL in seconds")]
    max_lock_ttl: u32,
    #[arg(long, default_value_t = MAX_VALUE_LEN as usize, help = "largest value in bytes")]
    max_value: usize,
    #[arg(
        long,
        default_value_t = 0,
        help = "seconds between stats lines on stderr; 0 for none"
    )]
    stats_interval: u64,
    #[arg(
        long,
        help = "after each stats line, one line per shard (`keep: shard N stats ...`, same fields); needs --stats-interval"
    )]
    stats_shards: bool,
    #[arg(
        long,
        value_enum,
        default_value = "rejoin",
        help = "rejoin quarantines this server's lock slots; bootstrap only for a first bring-up, when no client can hold a lock from an earlier run"
    )]
    start: StartArg,
    #[arg(
        long,
        value_delimiter = ',',
        help = "every cluster member, this one included, as node@client_ip:port/peer_ip:port/shards; a single server when absent"
    )]
    members: Vec<Peer>,
    #[arg(
        long,
        help = "UDP heartbeat and TCP peer address to bind; default: the unspecified address with this member's peer port"
    )]
    peer_listen: Option<SocketAddr>,
    #[arg(
        long,
        help = "file holding the peer secret, 32 hex digits; default: the KEEP_PEER_SECRET variable"
    )]
    peer_secret_file: Option<PathBuf>,
    #[arg(
        long,
        help = "this run's starting incarnation, above every earlier run's (raised past any newer run the peers remember); default: wall-clock seconds"
    )]
    incarnation: Option<u32>,
    #[arg(
        long,
        default_value_t = 30,
        value_parser = clap::value_parser!(u64).range(..=MAX_STOP_SECS),
        help = "longest pause with a stopped clock, in seconds, after which this server may hear no peer; added to the lock quarantine of a cluster member"
    )]
    max_stop: u64,
    #[arg(
        long,
        default_value_t = DEFAULT_STREAM_COALESCE_US,
        value_parser = clap::value_parser!(u64).range(..=MAX_STREAM_COALESCE_US),
        help = "microseconds a subscriber whose writes average more than one per span may have its output held to coalesce events into fewer writes; 0 for never"
    )]
    stream_coalesce_us: u64,
    #[arg(
        long,
        default_value_t = DEFAULT_STREAM_COALESCE_FANOUT,
        help = "subscriber connections this server needs before --stream-coalesce-us applies; 0 coalesces at any number"
    )]
    stream_coalesce_fanout: u32,
    #[arg(
        long,
        help = "file kept present while this server is ready for the next rolling step: a cluster member while it is settled, a single server once it listens; for a readiness probe"
    )]
    ready_file: Option<PathBuf>,
    #[arg(
        long,
        default_value_t = 0,
        value_parser = clap::value_parser!(u64).range(..=MAX_SPIN_US),
        help = "microseconds an idle shard keeps polling before it parks; 0 parks at once"
    )]
    spin_us: u64,
    #[arg(
        long,
        value_enum,
        default_value = "rpk",
        help = "rpk: mutual TLS with raw public keys; ca: certificates from a cluster CA; off: plaintext with the Auth challenge (PROTOCOL 10)"
    )]
    tls: TlsArg,
    #[arg(long, help = "this server's private key, PEM or DER (rpk and ca)")]
    tls_key: Option<PathBuf>,
    #[arg(
        long,
        help = "the client public keys the client port accepts, PEM (rpk)"
    )]
    tls_clients: Option<PathBuf>,
    #[arg(
        long,
        help = "the server public keys the peer port accepts and dials, this server's included, PEM (rpk, cluster members)"
    )]
    tls_servers: Option<PathBuf>,
    #[arg(long, help = "this server's certificate chain, PEM (ca)")]
    tls_cert: Option<PathBuf>,
    #[arg(long, help = "the CA every client chain leads to, PEM (ca)")]
    tls_ca: Option<PathBuf>,
    #[arg(
        long,
        help = "the CA every peer chain leads to, on the peer port and when dialing members, PEM (ca, cluster members)"
    )]
    tls_peer_ca: Option<PathBuf>,
    #[arg(long, hide = true)]
    tls_key_update_records: Option<NonZeroU64>,
}

fn read_secret(path: &Path) -> io::Result<String> {
    fs::read_to_string(path).map_err(|err| io::Error::other(format!("{}: {err}", path.display())))
}

fn secret(args: &Args) -> io::Result<ClusterSecret> {
    let text = match &args.secret_file {
        Some(path) => read_secret(path)?,
        None => env::var(SECRET_ENV).map_err(|_| {
            io::Error::other("no cluster secret: pass --secret-file or set KEEP_SECRET")
        })?,
    };
    ClusterSecret::from_hex(text.trim())
        .ok_or_else(|| io::Error::other("the cluster secret must be 32 hex digits"))
}

fn peer_secret(args: &Args) -> io::Result<PeerSecret> {
    let text = match &args.peer_secret_file {
        Some(path) => read_secret(path)?,
        None => env::var(PEER_SECRET_ENV).map_err(|_| {
            io::Error::other("no peer secret: pass --peer-secret-file or set KEEP_PEER_SECRET")
        })?,
    };
    PeerSecret::from_hex(text.trim())
        .ok_or_else(|| io::Error::other("the peer secret must be 32 hex digits"))
}

fn cluster(args: &Args) -> io::Result<Option<ClusterSetup>> {
    if args.members.is_empty() {
        if args.peer_listen.is_some()
            || args.peer_secret_file.is_some()
            || args.incarnation.is_some()
        {
            return Err(io::Error::other(
                "--peer-listen, --peer-secret-file and --incarnation need --members",
            ));
        }
        return Ok(None);
    }
    let incarnation = match args.incarnation {
        Some(incarnation) if incarnation > u32::MAX - INCARNATION_HEADROOM => {
            return Err(io::Error::other(format!(
                "--incarnation must be at most {}",
                u32::MAX - INCARNATION_HEADROOM
            )));
        }
        Some(incarnation) => incarnation,
        None => SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|since| u32::try_from(since.as_secs()).ok())
            .ok_or_else(|| io::Error::other("the clock is outside the incarnation range"))?,
    };
    Ok(Some(ClusterSetup {
        members: args.members.clone(),
        peer_listen: args.peer_listen,
        peer_secret: peer_secret(args)?,
        incarnation,
        max_stop: Duration::from_secs(args.max_stop),
    }))
}

fn tls_path(mode: &str, flag: &str, path: Option<&PathBuf>) -> io::Result<Box<str>> {
    let path = path.ok_or_else(|| {
        io::Error::other(format!(
            "--tls {mode} needs {flag} (--tls off serves plaintext)"
        ))
    })?;
    path.to_str()
        .map(Box::from)
        .ok_or_else(|| io::Error::other(format!("{flag} is not a UTF-8 path")))
}

fn tls(args: &Args) -> io::Result<Option<ServerTls>> {
    let (mode, cluster) = (args.tls, !args.members.is_empty());
    let possible = mode.to_possible_value();
    let name = possible.as_ref().map_or("", PossibleValue::get_name);
    let stray = [
        ("--tls-key", args.tls_key.is_some(), mode != TlsArg::Off),
        (
            "--tls-clients",
            args.tls_clients.is_some(),
            mode == TlsArg::Rpk,
        ),
        (
            "--tls-servers",
            args.tls_servers.is_some(),
            mode == TlsArg::Rpk && cluster,
        ),
        ("--tls-cert", args.tls_cert.is_some(), mode == TlsArg::Ca),
        ("--tls-ca", args.tls_ca.is_some(), mode == TlsArg::Ca),
        (
            "--tls-peer-ca",
            args.tls_peer_ca.is_some(),
            mode == TlsArg::Ca && cluster,
        ),
        (
            "--tls-key-update-records",
            args.tls_key_update_records.is_some(),
            mode != TlsArg::Off,
        ),
    ]
    .into_iter()
    .find(|&(_, set, applies)| set && !applies);
    if let Some((flag, ..)) = stray {
        return Err(io::Error::other(format!(
            "{flag} does not apply to --tls {name}{}{}",
            if cluster { "" } else { " on a single server" },
            if mode == TlsArg::Rpk {
                " (--tls defaults to rpk)"
            } else {
                ""
            }
        )));
    }
    let files = match mode {
        TlsArg::Off => return Ok(None),
        TlsArg::Rpk => TlsFiles::RawPublicKeys {
            key: tls_path(name, "--tls-key", args.tls_key.as_ref())?,
            clients: tls_path(name, "--tls-clients", args.tls_clients.as_ref())?,
            servers: args
                .tls_servers
                .as_ref()
                .map(|path| tls_path(name, "--tls-servers", Some(path)))
                .transpose()?,
        },
        TlsArg::Ca => TlsFiles::Authority {
            cert: tls_path(name, "--tls-cert", args.tls_cert.as_ref())?,
            key: tls_path(name, "--tls-key", args.tls_key.as_ref())?,
            ca: tls_path(name, "--tls-ca", args.tls_ca.as_ref())?,
            peer_ca: args
                .tls_peer_ca
                .as_ref()
                .map(|path| tls_path(name, "--tls-peer-ca", Some(path)))
                .transpose()?,
        },
    };
    ServerTls::load(&files, cluster, args.tls_key_update_records).map(Some)
}

fn config(args: &Args) -> io::Result<Config> {
    Ok(Config {
        listen: args.listen,
        advertise: args.advertise,
        node: args.node.map(NodeId),
        shards: args.shards,
        cpus: args.cpus.clone(),
        pinning: match args.pin {
            PinArg::Auto => Pinning::Auto,
            PinArg::Never => Pinning::Never,
        },
        spread: match args.steering {
            SteerArg::Auto => Spread::Auto,
            SteerArg::Hash => Spread::Fixed(Steering::Hash),
            SteerArg::Cpu => Spread::Fixed(Steering::Cpu),
        },
        secret: secret(args)?,
        memory: args.memory,
        max_lock_ttl: Ttl::from_secs(args.max_lock_ttl)
            .ok_or_else(|| io::Error::other("--max-lock-ttl must be at least 1"))?,
        max_value: args.max_value.min(MAX_VALUE_LEN as usize),
        loop_config: LoopConfig {
            spin: Duration::from_micros(args.spin_us),
            ..LoopConfig::DEFAULT
        },
        tuning: Tuning::KEEP,
        start: match args.start {
            StartArg::Rejoin => Start::Rejoin,
            StartArg::Bootstrap => Start::Bootstrap,
        },
        cluster: cluster(args)?,
        stream_coalesce: Duration::from_micros(args.stream_coalesce_us),
        stream_coalesce_fanout: args.stream_coalesce_fanout,
        ready_file: args.ready_file.clone(),
        tls: tls(args)?,
    })
}

fn announce(server: &Running, config: &Config) {
    let settings = &server.settings;
    eprintln!(
        "keep: listening on {} (advertised {}), node {}, {} shards on cpus {:?}, pinned {}, steering {:?}, rto_min {}, user_timeout {}, cap {:?}",
        server.addr,
        settings.advertise,
        settings.node.0,
        settings.shards,
        server.cpus.as_slice(),
        server.pinned,
        server.steering,
        server.applied.rto_min,
        server.applied.user_timeout,
        settings.cap.map(|cap| cap.bytes),
    );
    if let Some(reason) = &server.unsteered {
        eprintln!("keep: CPU steering is off: {reason}");
    }
    match server.memory {
        MemoryCap::Set { bytes } => eprintln!("keep: memory cap {bytes} bytes, from --memory"),
        MemoryCap::Default { bytes, budget } => eprintln!(
            "keep: memory cap {bytes} bytes, {}% of the {budget} byte memory budget; --memory sets another",
            MemoryCap::default_percent(),
        ),
    }
    if !settings.stream_coalesce.is_zero() {
        eprintln!(
            "keep: stream flushes coalesced for up to {} us for subscribers whose writes average more than one per span, from {} subscriber connections",
            settings.stream_coalesce.as_micros(),
            settings.stream_coalesce_fanout,
        );
    }
    if let Some(hold) = server.hold
        && hold < HandoffLimits::DEFAULT.leave_wait
    {
        eprintln!(
            "keep: a leave holds its entries for {} ms, less than the {} ms its receivers wait for them; raise --max-lock-ttl",
            hold.as_millis(),
            HandoffLimits::DEFAULT.leave_wait.as_millis()
        );
    }
    if !server.spin.is_zero() {
        eprintln!(
            "keep: idle shards poll for {} us before they park",
            server.spin.as_micros()
        );
    } else if !config.loop_config.spin.is_zero() {
        eprintln!("keep: --spin-us is off: shards are not pinned or a CPU quota applies");
    }
    if let Some(buffer) = server
        .heartbeat_buffer
        .filter(|buffer| buffer.granted < buffer.wanted)
    {
        eprintln!(
            "keep: heartbeat receive buffer is {} bytes, below the {} its members need; raise net.core.rmem_max",
            buffer.granted, buffer.wanted,
        );
    }
    match &config.cluster {
        Some(cluster) => eprintln!(
            "keep: cluster member of {} servers, start {:?}, incarnation {}",
            cluster.members.len(),
            config.start,
            cluster.incarnation,
        ),
        None => eprintln!("keep: single server, start {:?}", config.start),
    }
    if Running::cgroup_v1() {
        eprintln!("keep: cgroup v1 CPU quota is not read; set --shards to the quota");
        if matches!(server.memory, MemoryCap::Default { .. }) {
            eprintln!("keep: cgroup v1 memory limit is not read; set --memory below it");
        }
    }
}

struct Reported {
    total: Counts,
    shards: Vec<Counts>,
    now: Vec<Counts>,
    line: String,
}

impl Reported {
    fn start(server: &Running, per_shard: bool) -> Reported {
        let shards: Vec<Counts> = if per_shard {
            server.shard_counts().collect()
        } else {
            Vec::new()
        };
        Reported {
            total: if shards.is_empty() {
                server.counts()
            } else {
                total(shards.iter().copied())
            },
            now: Vec::with_capacity(shards.len()),
            shards,
            line: String::new(),
        }
    }

    fn sample(&mut self, shards: impl Iterator<Item = Counts>) -> Counts {
        self.now.clear();
        self.now.extend(shards);
        total(self.now.iter().copied())
    }

    fn render(&mut self, now: Counts) -> &str {
        self.line.clear();
        let _ = writeln!(
            self.line,
            "keep: stats {}",
            Delta {
                now: &now,
                last: &self.total
            }
        );
        self.total = now;
        for ((index, last), now) in (0u16..).zip(&mut self.shards).zip(&self.now) {
            let _ = writeln!(
                self.line,
                "keep: shard {index} stats {}",
                Delta { now, last }
            );
            *last = *now;
        }
        &self.line
    }
}

struct Delta<'a> {
    now: &'a Counts,
    last: &'a Counts,
}

impl fmt::Display for Delta<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ((shard, loops), (last, last_loops)) = (self.now, self.last);
        write!(
            f,
            "requests {} forwarded {} remote {} busy {} conns {} turns {} parks {} published {} delivered {} skipped {} slow {} skipruns {} windows {} heldus {}",
            shard.requests.wrapping_sub(last.requests),
            shard.forwarded.wrapping_sub(last.forwarded),
            shard.remote.wrapping_sub(last.remote),
            shard.busy.wrapping_sub(last.busy),
            loops.accepted.saturating_sub(loops.closed),
            loops.turns.wrapping_sub(last_loops.turns),
            loops.parks.wrapping_sub(last_loops.parks),
            shard.published.wrapping_sub(last.published),
            shard.delivered.wrapping_sub(last.delivered),
            shard.skipped.wrapping_sub(last.skipped),
            shard.slow,
            shard.skip_runs.wrapping_sub(last.skip_runs),
            shard.windows.wrapping_sub(last.windows),
            shard.held_ns.wrapping_sub(last.held_ns) / NANOS_PER_MICRO,
        )
    }
}

struct Memory(MemoryUse);

impl fmt::Display for Memory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "footprint {} large {} evicted {} orphaned {}",
            self.0.footprint, self.0.large_bytes, self.0.large_evicted, self.0.large_orphaned,
        )
    }
}

fn report(server: &Running, reported: &mut Reported) {
    let now = if reported.shards.is_empty() {
        server.counts()
    } else {
        reported.sample(server.shard_counts())
    };
    eprint!("{}", reported.render(now));
    let mut memory = MemoryUse::default();
    for shard in server.memory_use() {
        memory.footprint += shard.footprint;
        memory.large_bytes += shard.large_bytes;
        memory.large_evicted += shard.large_evicted;
        memory.large_orphaned += shard.large_orphaned;
    }
    eprintln!("keep: memory {}", Memory(memory));
    if !reported.shards.is_empty() {
        for (index, shard) in (0u16..).zip(server.memory_use()) {
            eprintln!("keep: shard {index} memory {}", Memory(shard));
        }
    }
    if let Some(control) = server.control_counts() {
        eprintln!(
            "keep: control epoch {} fenced {} lapses {} awaiting {} refused {} unsent {} runouts {} settled {}",
            control.epoch,
            control.fenced,
            control.lapses,
            control.awaiting,
            control.refused,
            control.unsent,
            control.run_outs,
            control.settled,
        );
    }
}

struct ClockWatch {
    source: String,
    peak: Option<i64>,
}

impl ClockWatch {
    fn start() -> ClockWatch {
        let source = clock::source().unwrap_or_default();
        eprintln!("keep: clock source {source}; server time reads CLOCK_MONOTONIC_RAW");
        let mut watch = ClockWatch { source, peak: None };
        watch.check();
        watch
    }

    fn check(&mut self) {
        if let Ok(source) = clock::source()
            && source != self.source
        {
            eprintln!(
                "keep: the kernel's clock source changed from {} to {source}",
                self.source
            );
            self.source = source;
        }
        let Ok(ppm) = clock::kernel_correction_ppm() else {
            return;
        };
        match self.peak {
            None if ppm.abs() > CORRECTION_WARN_PPM => {
                eprintln!(
                    "keep: the kernel corrects this host's clock by {ppm} ppm (NTP frequency and slew); Keep assumes its raw clock within 833 ppm of real time, so if this does not end, check the clock source"
                );
                self.peak = Some(ppm);
            }
            Some(peak) if ppm.abs() <= CORRECTION_WARN_PPM => {
                eprintln!(
                    "keep: the kernel's clock correction is back to {ppm} ppm (peak {peak} ppm)"
                );
                self.peak = None;
            }
            Some(peak) if ppm.abs() > peak.abs() => self.peak = Some(ppm),
            _ => {}
        }
    }
}

fn serve(args: &Args) -> io::Result<()> {
    Nanos::check().map_err(|err| {
        io::Error::other(format!("the server clock (CLOCK_MONOTONIC_RAW): {err}"))
    })?;
    let mut clocks = ClockWatch::start();
    let config = config(args)?;
    let signals = Signals::block()?;
    let mut server = start(&config)?;
    announce(&server, &config);
    let every = (args.stats_interval > 0).then(|| Duration::from_secs(args.stats_interval));
    let mut due = every.map(|every| Instant::now() + every);
    let mut reported = Reported::start(&server, args.stats_shards);
    let stopped = loop {
        if signals.wait(WATCH) {
            break None;
        }
        if let Some(stopped) = server.stopped() {
            break Some(stopped);
        }
        clocks.check();
        if let (Some(every), Some(at)) = (every, due)
            && Instant::now() >= at
        {
            report(&server, &mut reported);
            due = Some(at + every);
        }
    };
    match stopped {
        None => eprintln!("keep: draining"),
        Some(Stopped::Shard(shard)) => {
            eprintln!("keep: shard {shard} stopped; draining the others");
        }
        Some(Stopped::Control) => eprintln!("keep: the control thread stopped; draining"),
    }
    server.drain()?;
    server.join()?;
    match stopped {
        None => Ok(()),
        Some(Stopped::Shard(shard)) => Err(io::Error::other(format!("shard {shard} stopped"))),
        Some(Stopped::Control) => Err(io::Error::other("the control thread stopped")),
    }
}

pub fn main() -> ExitCode {
    let matches = sources::matches(
        Args::command(),
        env::args_os().collect(),
        &ALIASES,
        &|name| env::var_os(name),
    )
    .unwrap_or_else(|err| err.exit());
    let args = Args::from_arg_matches(&matches).unwrap_or_else(|err| err.exit());
    match ensure_huge_pages() {
        HugePages::Set => {}
        HugePages::Restarted => {
            eprintln!("keep: {HUGETLB}=1 was not applied after a restart; huge pages are off");
        }
        HugePages::Secure => {
            eprintln!("keep: secure-execution mode ignores {TUNABLES_ENV}; huge pages are off");
        }
        HugePages::Failed(err) => {
            eprintln!("keep: could not restart with {HUGETLB}=1: {err}");
        }
    }
    match serve(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("keep: {err}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-server/cli.rs"]
mod tests;
