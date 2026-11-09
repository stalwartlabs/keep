/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    dispatch::{Arrival, Looping},
    kv::{ConnShape, KvTarget, Mapping, model::Unseen},
    load::Load,
    stream::{EventReplay, Verify, backend::StreamTarget},
    workload::{
        deployment::{self, Deployment},
        mix::KeyScope,
        trace::TraceNodes,
    },
};
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::{fmt, path::PathBuf, time::Duration};

#[derive(Parser)]
#[command(
    name = "mock-stalwart",
    version,
    about = "A mock Stalwart node: Stalwart's KV and cluster event load against Keep and its competitors"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    Run(Box<RunArgs>),
    Flush(FlushArgs),
    Merge(MergeArgs),
    Profile(ProfileArgs),
}

#[derive(Args)]
pub struct FlushArgs {
    #[arg(long)]
    pub kv: String,
}

#[derive(Args)]
pub struct MergeArgs {
    pub files: Vec<PathBuf>,
    #[arg(long)]
    pub json: Option<PathBuf>,
}

#[derive(Args)]
pub struct ProfileArgs {
    pub name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, serde::Serialize)]
pub enum ArrivalKind {
    Poisson,
    Uniform,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, serde::Serialize)]
pub enum Check {
    Model,
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum ScopeArg {
    Node,
    Cluster,
}

#[derive(Args)]
pub struct RunArgs {
    #[arg(
        long,
        default_value_t = 1,
        help = "Node id of the first instance (0..511)"
    )]
    pub node_id: u16,
    #[arg(
        long,
        default_value_t = 1,
        help = "Instances in this process, each with its own runtime and connections"
    )]
    pub instances: u16,
    #[arg(
        long,
        help = "Tokio worker threads per instance [default: cores / instances]"
    )]
    pub worker_threads: Option<usize>,
    #[arg(
        long,
        default_value_t = 10.0,
        help = "Warm-up seconds before the measurement window"
    )]
    pub warmup: f64,
    #[arg(long, default_value_t = 30.0, help = "Measurement window in seconds")]
    pub duration: f64,
    #[arg(
        long,
        help = "Unix time (seconds, fractional allowed) at which load starts, to align processes"
    )]
    pub start_at: Option<f64>,
    #[arg(
        long,
        default_value_t = 5.0,
        help = "Seconds to wait for outstanding KV ops after the window"
    )]
    pub grace: f64,
    #[arg(
        long,
        default_value_t = 2.0,
        help = "Seconds to keep subscribing after the last publish"
    )]
    pub settle: f64,
    #[arg(long, help = "Directory for node-<id>.json and node-<id>.hlog")]
    pub out: Option<PathBuf>,
    #[arg(
        long,
        default_value_t = 0,
        help = "RNG seed, 0 picks one from the clock"
    )]
    pub seed: u64,
    #[arg(
        long,
        default_value_t = 0.0,
        help = "Dispatcher spin window in microseconds before each arrival"
    )]
    pub spin_us: f64,
    #[arg(
        long,
        default_value_t = 5.0,
        help = "Progress line interval in seconds, 0 disables"
    )]
    pub progress: f64,

    #[arg(
        long,
        default_value = "none",
        help = "none | null | local | redis://h:p | redis-cluster://h:p,h:p | keep://..."
    )]
    pub kv: String,
    #[arg(long, value_enum, default_value_t = Mapping::Stalwart)]
    pub kv_mapping: Mapping,
    #[arg(
        long,
        default_value = "pool-ping:10",
        help = "pool-ping:N (Stalwart today) | pool:N | shared:N"
    )]
    pub kv_conn: String,
    #[arg(
        long,
        default_value_t = 1000.0,
        help = "KV actions per second per instance"
    )]
    pub kv_rate: f64,
    #[arg(long, value_enum, default_value_t = ArrivalKind::Poisson)]
    pub kv_arrival: ArrivalKind,
    #[arg(
        long,
        default_value_t = 0,
        help = "0 spawns one tokio task per op; N > 0 uses N long-lived tasks fed by a queue"
    )]
    pub kv_tasks: usize,
    #[arg(
        long,
        default_value = "mixed",
        help = "mixed | mta | imap | http | <profile.toml> | trace:<kv-trace.tsv>"
    )]
    pub workload: String,
    #[arg(long, value_enum, default_value_t = ScopeArg::Node)]
    pub key_scope: ScopeArg,
    #[arg(
        long,
        default_value = "lab",
        help = "Deployment tier that sets each family's population and live keys (harness/bench/workload)"
    )]
    pub deployment: String,
    #[arg(long, value_enum, default_value_t = Check::Model)]
    pub check: Check,
    #[arg(long, help = "Keys never seen are known to be absent (after `flush`)")]
    pub assume_empty: bool,
    #[arg(
        long,
        default_value_t = 1.0,
        help = "Scale of the live-key preload of each family"
    )]
    pub preload_scale: f64,

    #[arg(
        long,
        default_value = "none",
        help = "none | loopback | redis://h:p | redis-sharded://h:p | nats://h:p | kafka://h:p | fluvio://sc-host:9003[?socket=stock|low-latency] | zenoh-client://tcp/h:p | zenoh-peer://... | zenoh-config:<file> | keep://..."
    )]
    pub stream: String,
    #[arg(
        long,
        default_value_t = 1000.0,
        help = "Broadcast events per second per instance"
    )]
    pub event_rate: f64,
    #[arg(long, value_enum, default_value_t = ArrivalKind::Poisson)]
    pub event_arrival: ArrivalKind,
    #[arg(
        long,
        default_value = "stalwart",
        help = "stalwart (measured mixed sizes) | provisional (sizes up to 512 KiB) | <events.toml> | trace:<events.tsv>"
    )]
    pub events: String,
    #[arg(
        long,
        default_value_t = 1,
        help = "Subscribed nodes in the cluster, for last-delivery accounting"
    )]
    pub expected_nodes: u16,
    #[arg(
        long,
        value_enum,
        default_value_t = EventReplay::Events,
        help = "Replay trace event rows through Stalwart's batching, or publish rows as recorded messages"
    )]
    pub event_replay: EventReplay,
    #[arg(
        long,
        default_value_t = 1,
        help = "Echo an ack for one message in N, 0 disables acks"
    )]
    pub ack_every: u64,
    #[arg(
        long,
        default_value_t = 0.0,
        help = "Synthetic apply cost per received event, microseconds"
    )]
    pub apply_us: f64,
    #[arg(
        long,
        default_value_t = 0.0,
        help = "Synthetic catch_up cost, microseconds"
    )]
    pub catch_up_us: f64,
    #[arg(long, value_enum, default_value_t = Verify::Full)]
    pub verify: Verify,
    #[arg(long, default_value_t = 1)]
    pub kafka_partitions: i32,
    #[arg(
        long,
        default_value_t = 64,
        help = "Ack topic partitions (Kafka and Fluvio)"
    )]
    pub kafka_ack_partitions: i32,
    #[cfg(feature = "kafka")]
    #[arg(long, value_enum, default_value_t = crate::stream::backend::kafka::KafkaConsumer::Assign)]
    pub kafka_consumer: crate::stream::backend::kafka::KafkaConsumer,
    #[cfg(feature = "kafka")]
    #[arg(long, value_enum, default_value_t = crate::stream::backend::kafka::KafkaAcks::One)]
    pub kafka_acks: crate::stream::backend::kafka::KafkaAcks,
    #[arg(
        long,
        default_value_t = 5000,
        help = "Consumer session timeout for group and shared modes (Stalwart default 5000 ms)"
    )]
    pub kafka_session_ms: u32,
    #[cfg(feature = "kafka")]
    #[arg(long, value_enum, default_value_t = crate::stream::backend::kafka::Nagle::Off)]
    pub kafka_nagle: crate::stream::backend::kafka::Nagle,

    #[arg(long, help = "Trace node filter: all | own | <node>  [default: own]")]
    pub trace_node: Option<String>,
    #[arg(long, default_value_t = 1.0, help = "Trace replay speed multiplier")]
    pub trace_speed: f64,
    #[arg(long, help = "Replay traces in a loop")]
    pub trace_loop: bool,

    #[arg(
        long,
        default_value_t = 0.0,
        help = "Synthetic CPU load as a fraction of the runtime's workers (0..0.95)"
    )]
    pub cpu_load: f64,
    #[arg(
        long,
        default_value_t = 50.0,
        help = "Synthetic load chunk between yields, microseconds"
    )]
    pub cpu_load_chunk_us: f64,
}

#[derive(Debug)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn invalid(reason: impl Into<String>) -> ConfigError {
    ConfigError(reason.into())
}

#[derive(Clone, Copy, Debug)]
pub enum TraceSelect {
    All,
    Own,
    Node(u16),
}

impl TraceSelect {
    pub fn nodes(&self, own: u16) -> TraceNodes {
        match self {
            TraceSelect::All => TraceNodes::All,
            TraceSelect::Own => TraceNodes::Only(own),
            TraceSelect::Node(node) => TraceNodes::Only(*node),
        }
    }
}

#[derive(Debug)]
pub struct Replay {
    pub select: TraceSelect,
    pub speed: f64,
    pub looped: Looping,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub enum Issuers {
    Pool(usize),
    TaskPerOp,
}

#[derive(Debug)]
pub struct KvConfig {
    pub target: KvTarget,
    pub mapping: Mapping,
    pub shape: ConnShape,
    pub arrival: Arrival,
    pub issuers: Issuers,
    pub workload: String,
    pub scope: KeyScope,
    pub deployment: &'static Deployment,
    pub check: Check,
    pub unseen: Unseen,
    pub preload_scale: f64,
}

#[derive(Debug)]
pub struct StreamConfig {
    pub target: StreamTarget,
    pub arrival: Arrival,
    pub events: String,
    pub replay: EventReplay,
    pub expected_nodes: u16,
    pub ack_every: u64,
    pub apply: Duration,
    pub catch_up: Duration,
    pub verify: Verify,
}

#[derive(Debug)]
pub struct Config {
    pub node: u16,
    pub instances: u16,
    pub workers: usize,
    pub warmup: Duration,
    pub duration: Duration,
    pub start_at: Option<u64>,
    pub grace: Duration,
    pub settle: Duration,
    pub out: Option<PathBuf>,
    pub seed: u64,
    pub spin: Duration,
    pub progress: Option<Duration>,
    pub replay: Replay,
    pub kv: Option<KvConfig>,
    pub stream: Option<StreamConfig>,
    pub load: Option<Load>,
}

fn seconds(value: f64, name: &str) -> Result<Duration, ConfigError> {
    Duration::try_from_secs_f64(value)
        .map_err(|_| invalid(format!("--{name} must be a non-negative number")))
}

fn micros(value: f64, name: &str) -> Result<Duration, ConfigError> {
    seconds(value / 1e6, name)
}

fn rate(value: f64, name: &str) -> Result<f64, ConfigError> {
    match value.is_finite() && value > 0.0 {
        true => Ok(value),
        false => Err(invalid(format!("--{name} must be positive"))),
    }
}

fn arrival(
    kind: ArrivalKind,
    value: f64,
    name: &str,
    trace: bool,
    replay: &Replay,
) -> Result<Arrival, ConfigError> {
    if trace {
        return Ok(Arrival::Trace {
            speed: replay.speed,
            looped: replay.looped,
        });
    }
    let rate = rate(value, name)?;
    Ok(match kind {
        ArrivalKind::Poisson => Arrival::Poisson { rate },
        ArrivalKind::Uniform => Arrival::Uniform { rate },
    })
}

fn parse_trace_select(spec: Option<&str>) -> Result<TraceSelect, ConfigError> {
    let Some(spec) = spec else {
        return Ok(TraceSelect::Own);
    };
    hashify::fnc_map!(spec.as_bytes(),
        "all" => { return Ok(TraceSelect::All); },
        "own" => { return Ok(TraceSelect::Own); },
        _ => {}
    );
    spec.parse()
        .map(TraceSelect::Node)
        .map_err(|_| invalid("--trace-node is all, own or a node id"))
}

pub fn parse_shape(spec: &str) -> Result<ConnShape, ConfigError> {
    let (kind, size) = spec
        .split_once(':')
        .ok_or_else(|| invalid("--kv-conn is pool-ping:N, pool:N or shared:N"))?;
    let size: usize = size
        .parse()
        .ok()
        .filter(|size| *size > 0)
        .ok_or_else(|| invalid("--kv-conn size must be a positive integer"))?;
    hashify::fnc_map!(kind.as_bytes(),
        "pool-ping" => { return Ok(ConnShape::PoolPing(size)); },
        "pool" => { return Ok(ConnShape::Pool(size)); },
        "shared" => { return Ok(ConnShape::Shared(size)); },
        _ => {}
    );
    Err(invalid("--kv-conn is pool-ping:N, pool:N or shared:N"))
}

fn split_hosts(list: &str) -> Box<[Box<str>]> {
    list.split(',')
        .filter(|host| !host.is_empty())
        .map(Box::from)
        .collect()
}

pub fn parse_kv_target(spec: &str) -> Result<Option<KvTarget>, ConfigError> {
    let (scheme, rest) = spec.split_once("://").unwrap_or((spec, ""));
    let _ = rest;
    hashify::fnc_map!(scheme.as_bytes(),
        "none" => { return Ok(None); },
        "null" => { return Ok(Some(KvTarget::Null)); },
        "local" => { return Ok(Some(KvTarget::Local)); },
        "redis" => { return redis_single(spec); },
        "rediss" => { return redis_single(spec); },
        "redis-cluster" => { return redis_cluster(rest); },
        "keep" => { return Ok(Some(KvTarget::Keep(spec.into()))); },
        _ => {}
    );
    Err(invalid(format!("unknown --kv target {spec}")))
}

#[cfg(feature = "redis")]
fn redis_single(spec: &str) -> Result<Option<KvTarget>, ConfigError> {
    Ok(Some(KvTarget::Redis(
        crate::kv::redis::RedisTarget::Single(spec.into()),
    )))
}

#[cfg(feature = "redis")]
fn redis_cluster(hosts: &str) -> Result<Option<KvTarget>, ConfigError> {
    let urls = split_hosts(hosts)
        .iter()
        .map(|host| format!("redis://{host}").into_boxed_str())
        .collect::<Box<[_]>>();
    match urls.is_empty() {
        true => Err(invalid("redis-cluster:// needs at least one host")),
        false => Ok(Some(KvTarget::Redis(
            crate::kv::redis::RedisTarget::Cluster(urls),
        ))),
    }
}

#[cfg(not(feature = "redis"))]
fn redis_single(_spec: &str) -> Result<Option<KvTarget>, ConfigError> {
    Err(invalid("built without the redis feature"))
}

#[cfg(not(feature = "redis"))]
fn redis_cluster(_hosts: &str) -> Result<Option<KvTarget>, ConfigError> {
    Err(invalid("built without the redis feature"))
}

pub fn parse_stream_target(
    spec: &str,
    args: &RunArgs,
) -> Result<Option<StreamTarget>, ConfigError> {
    let _ = args;
    if let Some(path) = spec.strip_prefix("zenoh-config:") {
        return zenoh_target(ZenohSpec::Config(path));
    }
    let (scheme, rest) = spec.split_once("://").unwrap_or((spec, ""));
    hashify::fnc_map!(scheme.as_bytes(),
        "none" => { return Ok(None); },
        "loopback" => { return Ok(Some(StreamTarget::Loopback)); },
        "redis" => { return redis_stream(format!("redis://{rest}"), Sharding::Off); },
        "redis-sharded" => { return redis_stream(format!("redis://{rest}"), Sharding::On); },
        "nats" => { return nats_target(rest); },
        "kafka" => { return kafka_target(rest, args); },
        "fluvio" => { return fluvio_target(rest, args); },
        "zenoh-client" => { return zenoh_target(ZenohSpec::Client(rest)); },
        "zenoh-peer" => { return zenoh_target(ZenohSpec::Peer(rest)); },
        "keep" => { return Ok(Some(StreamTarget::Keep(spec.into()))); },
        _ => {}
    );
    Err(invalid(format!("unknown --stream target {spec}")))
}

#[derive(Clone, Copy)]
enum Sharding {
    Off,
    On,
}

enum ZenohSpec<'a> {
    Client(&'a str),
    Peer(&'a str),
    Config(&'a str),
}

#[cfg(feature = "redis")]
fn redis_stream(url: String, sharding: Sharding) -> Result<Option<StreamTarget>, ConfigError> {
    use crate::stream::backend::redis::{PubMode, RedisStreamTarget};
    Ok(Some(StreamTarget::Redis(RedisStreamTarget {
        url: url.into_boxed_str(),
        mode: match sharding {
            Sharding::Off => PubMode::Publish,
            Sharding::On => PubMode::Sharded,
        },
    })))
}

#[cfg(not(feature = "redis"))]
fn redis_stream(_url: String, _sharding: Sharding) -> Result<Option<StreamTarget>, ConfigError> {
    Err(invalid("built without the redis feature"))
}

#[cfg(feature = "nats")]
fn nats_target(hosts: &str) -> Result<Option<StreamTarget>, ConfigError> {
    let hosts = split_hosts(hosts)
        .iter()
        .map(|host| format!("nats://{host}").into_boxed_str())
        .collect::<Box<[_]>>();
    match hosts.is_empty() {
        true => Err(invalid("nats:// needs at least one host")),
        false => Ok(Some(StreamTarget::Nats(
            crate::stream::backend::nats::NatsTarget { hosts },
        ))),
    }
}

#[cfg(not(feature = "nats"))]
fn nats_target(_hosts: &str) -> Result<Option<StreamTarget>, ConfigError> {
    Err(invalid("built without the nats feature"))
}

#[cfg(feature = "fluvio")]
fn fluvio_target(spec: &str, args: &RunArgs) -> Result<Option<StreamTarget>, ConfigError> {
    use crate::stream::backend::fluvio::FluvioSocket;
    let (endpoint, socket) = match spec.split_once('?') {
        None => (spec, FluvioSocket::Stock),
        Some((endpoint, query)) => {
            let socket = hashify::map!(query.as_bytes(), FluvioSocket,
                "socket=stock" => FluvioSocket::Stock,
                "socket=low-latency" => FluvioSocket::LowLatency,
            )
            .copied()
            .ok_or_else(|| invalid("fluvio:// takes ?socket=stock or ?socket=low-latency"))?;
            (endpoint, socket)
        }
    };
    let ack_partitions = u32::try_from(args.kafka_ack_partitions)
        .ok()
        .filter(|&partitions| partitions > 0)
        .ok_or_else(|| invalid("fluvio:// needs a positive --kafka-ack-partitions"))?;
    match endpoint.is_empty() {
        true => Err(invalid("fluvio:// needs the SC endpoint host:port")),
        false => Ok(Some(StreamTarget::Fluvio(
            crate::stream::backend::fluvio::FluvioTarget {
                endpoint: endpoint.into(),
                ack_partitions,
                socket,
            },
        ))),
    }
}

#[cfg(not(feature = "fluvio"))]
fn fluvio_target(_endpoint: &str, _args: &RunArgs) -> Result<Option<StreamTarget>, ConfigError> {
    Err(invalid("built without the fluvio feature"))
}

#[cfg(feature = "kafka")]
fn kafka_target(brokers: &str, args: &RunArgs) -> Result<Option<StreamTarget>, ConfigError> {
    if brokers.is_empty() || args.kafka_partitions < 1 || args.kafka_ack_partitions < 1 {
        return Err(invalid(
            "kafka:// needs brokers and positive partition counts",
        ));
    }
    Ok(Some(StreamTarget::Kafka(
        crate::stream::backend::kafka::KafkaTarget {
            brokers: brokers.into(),
            partitions: args.kafka_partitions,
            ack_partitions: args.kafka_ack_partitions,
            consumer: args.kafka_consumer,
            acks: args.kafka_acks,
            nagle: args.kafka_nagle,
            session_timeout_ms: args.kafka_session_ms,
        },
    )))
}

#[cfg(not(feature = "kafka"))]
fn kafka_target(_brokers: &str, _args: &RunArgs) -> Result<Option<StreamTarget>, ConfigError> {
    Err(invalid("built without the kafka feature"))
}

#[cfg(feature = "zenoh")]
fn zenoh_target(spec: ZenohSpec<'_>) -> Result<Option<StreamTarget>, ConfigError> {
    use crate::stream::backend::zenoh::ZenohTarget;
    Ok(Some(StreamTarget::Zenoh(match spec {
        ZenohSpec::Client(endpoints) => ZenohTarget::Client(split_hosts(endpoints)),
        ZenohSpec::Peer(endpoints) => ZenohTarget::Peer(split_hosts(endpoints)),
        ZenohSpec::Config(path) => ZenohTarget::Config(PathBuf::from(path)),
    })))
}

#[cfg(not(feature = "zenoh"))]
fn zenoh_target(_spec: ZenohSpec<'_>) -> Result<Option<StreamTarget>, ConfigError> {
    Err(invalid("built without the zenoh feature"))
}

impl TryFrom<RunArgs> for Config {
    type Error = ConfigError;

    fn try_from(args: RunArgs) -> Result<Self, Self::Error> {
        if args.instances == 0 || u32::from(args.node_id) + u32::from(args.instances) > 512 {
            return Err(invalid("node ids must stay within 0..511"));
        }
        if !(0.0..=0.95).contains(&args.cpu_load) {
            return Err(invalid("--cpu-load must be within 0..0.95"));
        }
        let cores = std::thread::available_parallelism().map_or(1, |cores| cores.get());
        let workers = args
            .worker_threads
            .unwrap_or_else(|| (cores / usize::from(args.instances)).max(1));
        let replay = Replay {
            select: parse_trace_select(args.trace_node.as_deref())?,
            speed: rate(args.trace_speed, "trace-speed")?,
            looped: match args.trace_loop {
                true => Looping::Repeat,
                false => Looping::Once,
            },
        };
        let kv = match parse_kv_target(&args.kv)? {
            Some(target) => Some(KvConfig {
                target,
                mapping: args.kv_mapping,
                shape: parse_shape(&args.kv_conn)?,
                arrival: arrival(
                    args.kv_arrival,
                    args.kv_rate,
                    "kv-rate",
                    args.workload.starts_with("trace:"),
                    &replay,
                )?,
                issuers: match args.kv_tasks {
                    0 => Issuers::TaskPerOp,
                    tasks => Issuers::Pool(tasks),
                },
                workload: args.workload.clone(),
                scope: match args.key_scope {
                    ScopeArg::Node => KeyScope::Node,
                    ScopeArg::Cluster => KeyScope::Cluster,
                },
                deployment: deployment::builtin(&args.deployment).ok_or_else(|| {
                    invalid(format!(
                        "unknown --deployment {:?}; built-in: {}",
                        args.deployment,
                        deployment::DEPLOYMENTS.join(", ")
                    ))
                })?,
                check: args.check,
                unseen: match args.assume_empty {
                    true => Unseen::Absent,
                    false => Unseen::Unknown,
                },
                preload_scale: args.preload_scale.max(0.0),
            }),
            None => None,
        };
        if args.event_replay == EventReplay::Messages && !args.events.starts_with("trace:") {
            return Err(invalid(
                "--event-replay messages needs --events trace:<file>",
            ));
        }
        let stream = match parse_stream_target(&args.stream, &args)? {
            Some(target) => Some(StreamConfig {
                target,
                arrival: arrival(
                    args.event_arrival,
                    args.event_rate,
                    "event-rate",
                    args.events.starts_with("trace:"),
                    &replay,
                )?,
                events: args.events.clone(),
                replay: args.event_replay,
                expected_nodes: args.expected_nodes.max(1),
                ack_every: args.ack_every,
                apply: micros(args.apply_us, "apply-us")?,
                catch_up: micros(args.catch_up_us, "catch-up-us")?,
                verify: args.verify,
            }),
            None => None,
        };
        if kv.is_none() && stream.is_none() {
            return Err(invalid("nothing to do: set --kv and/or --stream"));
        }
        Ok(Config {
            node: args.node_id,
            instances: args.instances,
            workers,
            warmup: seconds(args.warmup, "warmup")?,
            duration: seconds(args.duration, "duration")?,
            start_at: match args.start_at {
                Some(at) => Some(
                    Duration::try_from_secs_f64(at)
                        .map(crate::clock::duration_ns)
                        .map_err(|_| invalid("--start-at must be unix seconds"))?,
                ),
                None => None,
            },
            grace: seconds(args.grace, "grace")?,
            settle: seconds(args.settle, "settle")?,
            out: args.out,
            seed: args.seed,
            spin: micros(args.spin_us, "spin-us")?,
            progress: match args.progress {
                0.0 => None,
                progress => Some(seconds(progress, "progress")?),
            },
            replay,
            load: match args.cpu_load {
                0.0 => None,
                fraction => Some(Load {
                    fraction,
                    chunk: micros(args.cpu_load_chunk_us, "cpu-load-chunk-us")?,
                }),
            },
            kv,
            stream,
        })
    }
}
