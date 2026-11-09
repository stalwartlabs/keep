/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use hdrhistogram::Histogram;
use hdrhistogram::serialization::V2DeflateSerializer;
use hdrhistogram::serialization::interval_log::{IntervalLogWriter, IntervalLogWriterBuilder};
use keep_proto::{ClusterSecret, ServerInfo};
use serde::Serialize;

use crate::cli::{ProtocolArg, RouteArg, RunArgs, SelectArg, seconds};
use crate::clock::{Clock, duration_ns, unix_now_ns};
use crate::op::OpKind;
use crate::protocol::Protocol;
use crate::protocol::resp::{self, LeaseRelease, LeaseRenew, Resp, RespMapping};
use crate::protocol::shard::ShardCollect;
use crate::report::{self, ErrorCounts, PointAgg, PointContext, PointSummary};
use crate::setup;
use crate::stats::{MAX_POINTS, Percentiles, Recorder, hist_template};
use crate::sys;
use crate::tls::{Stream, TlsClients, TlsInfo};
use crate::worker::{
    Idle, Msg, NodeMap, PRELOAD_PHASE, Pacing, PreloadMode, Routing, Segment, Source, Worker,
    WorkerSetup,
};
use crate::workload::profile::{self, Overrides};
use crate::workload::trace::{self, Trace};
use crate::workload::values::value_pool;
use crate::workload::{Generator, Ttl, VirtualTime, Workload};

const DAY_S: u64 = 86_400;
const RESERVED_FDS: u64 = 256;

#[derive(Clone, Copy, Debug)]
struct MarkSample {
    retrans: Option<u64>,
    cpu_ns: u64,
}

type PointMarks = [Option<MarkSample>; 2];
const START_LEAD: Duration = Duration::from_millis(200);
const NS_PER_SEC: u64 = 1_000_000_000;

#[derive(Debug)]
pub struct RunError(pub String);

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RunError {}

fn fail<T>(msg: impl Into<String>) -> Result<T, RunError> {
    Err(RunError(msg.into()))
}

fn wrap<E: fmt::Display>(context: &str) -> impl FnOnce(E) -> RunError + '_ {
    move |e| RunError(format!("{context}: {e}"))
}

enum Loaded {
    Profile(Arc<Workload>),
    Trace(Trace),
}

#[derive(Serialize)]
struct HostInfo {
    hostname: String,
    os: &'static str,
    arch: &'static str,
    cpus: usize,
}

#[derive(Serialize)]
struct ClassInfo {
    op: &'static str,
    keyspace: String,
    share: f64,
}

#[derive(Serialize)]
struct KeyspaceInfo {
    name: String,
    keys: u64,
    popularity: String,
    ttl: String,
}

#[derive(Serialize)]
struct SourceInfo {
    kind: &'static str,
    name: String,
    description: String,
    records: Option<u64>,
    classes: Vec<ClassInfo>,
    keyspaces: Vec<KeyspaceInfo>,
    dropped: Vec<String>,
}

#[derive(Serialize)]
struct PreloadSummary {
    #[serde(skip_serializing_if = "PreloadMode::is_write")]
    mode: PreloadMode,
    keys: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    present: Option<u64>,
    seconds: f64,
    rate: f64,
    errors: ErrorCounts,
    misses: u64,
    latency: Percentiles,
}

#[derive(Serialize)]
struct RunSummary {
    tool: &'static str,
    version: &'static str,
    label: String,
    args: Vec<String>,
    host: HostInfo,
    start_unix_ms: u64,
    protocol: String,
    resp_lease: Option<[&'static str; 2]>,
    io: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls: Option<TlsInfo>,
    target: String,
    cluster_nodes: Vec<String>,
    shard_collect: Option<u16>,
    keep_auth: bool,
    select: Option<&'static str>,
    shard_collect_attempts: Option<usize>,
    threads: usize,
    connections_per_thread: Vec<usize>,
    pacing: String,
    idle: String,
    virtual_time_origin_s: u64,
    source: SourceInfo,
    preload: Option<PreloadSummary>,
    points: Vec<PointSummary>,
    failures: Vec<String>,
    valid: bool,
    clock: ClockInfo,
    outlier_us: f64,
    flows: Vec<Flow>,
    worker_tids: Vec<i64>,
    worker_cpus: Vec<usize>,
}

#[derive(Serialize, Clone, Copy, Debug, Default)]
struct ClockInfo {
    unix_base_ns: u64,
    mono_base_ns: u64,
}

fn open_stream(
    args: &RunArgs,
    secret: Option<&ClusterSecret>,
    tls: &mut Option<TlsClients>,
    node: &str,
) -> Result<(Stream, ServerInfo), String> {
    let stream = setup::connect(node).map_err(|e| format!("connect: {e}"))?;
    let mut stream = match tls {
        Some(clients) => clients
            .connect(node, stream)
            .map_err(|e| format!("connect: {e}"))?,
        None => {
            let mut stream = stream;
            if let Some(auth) = &args.auth {
                setup::auth(&mut stream, auth).map_err(|e| format!("auth: {e}"))?;
            }
            Stream::Plain(stream)
        }
    };
    let info = match args.protocol {
        ProtocolArg::Keep => {
            setup::keep_hello(&mut stream, secret).map_err(|e| format!("hello: {e}"))?
        }
        ProtocolArg::Resp | ProtocolArg::Meta => ServerInfo::default(),
    };
    setup::clear_timeouts(stream.tcp()).map_err(|e| format!("socket: {e}"))?;
    Ok((stream, info))
}

#[derive(Serialize)]
struct Flow {
    thread: usize,
    conn: usize,
    local: String,
    peer: String,
}

pub fn execute(args: &RunArgs, argv: Vec<String>) -> Result<bool, RunError> {
    let threads = args.threads.max(1);
    if threads > MAX_POINTS {
        return fail("too many threads");
    }
    let rates = args.rates().map_err(RunError)?;
    if rates.len() > PRELOAD_PHASE as usize {
        return fail(format!("at most {PRELOAD_PHASE} load points"));
    }
    let cpus = args.cpu_list().map_err(RunError)?;
    let idle = args.idle_mode().map_err(RunError)?;
    let connections = args.connections.unwrap_or(threads);
    if connections < threads {
        return fail("--connections must be at least --threads");
    }
    if args.cluster && args.protocol != ProtocolArg::Resp {
        return fail("--cluster needs --protocol resp");
    }
    let keep = args.protocol == ProtocolArg::Keep;
    let secret = if keep {
        args.cluster_secret().map_err(RunError)?
    } else {
        None
    };
    let mut tls = args.tls_config().map_err(RunError)?.map(TlsClients::new);
    let mut tls_info = None;
    let discovered = match (keep, args.route) {
        (_, RouteArg::None) => None,
        (true, RouteArg::Shards) => match args.shard_collect {
            Some(count) => Some(count),
            None => {
                let (_, info) =
                    open_stream(args, secret.as_ref(), &mut tls, &args.target).map_err(RunError)?;
                let shard = info.shard.ok_or_else(|| {
                    RunError(String::from(
                        "--route shards: the Hello reply names no shard (TLV 2 missing)",
                    ))
                })?;
                Some(shard.count().get())
            }
        },
        (false, RouteArg::Shards) => return fail("--route shards needs --protocol keep"),
    };
    let collect_shards = args.shard_collect.or(discovered);
    let mut collect = None;
    let (nodes, node_map) = match (args.cluster, collect_shards) {
        (true, _) => {
            let mut seed = setup::connect(&args.target).map_err(wrap("cluster seed"))?;
            if let Some(auth) = &args.auth {
                setup::auth(&mut seed, auth).map_err(wrap("cluster seed"))?;
            }
            let map =
                setup::cluster_slots(&mut seed, &args.target).map_err(wrap("cluster seed"))?;
            (map.nodes.clone(), Some(NodeMap::Slots(Arc::new(map))))
        }
        (false, Some(shards)) => {
            if !keep {
                return fail(
                    "shard collection needs --protocol keep (the Hello reply names the shard)",
                );
            }
            let map = args
                .shard_map(shards, secret.as_ref())
                .map_err(|e| RunError(format!("shard collection: {e}")))?;
            collect = Some(ShardCollect::new(&map));
            (
                vec![args.target.clone(); usize::from(shards)],
                Some(NodeMap::Shards(map)),
            )
        }
        (false, None) => (vec![args.target.clone()], None),
    };
    let routed = args.routed();
    sys::ensure_open_files((connections * nodes.len()) as u64 + RESERVED_FDS).map_err(RunError)?;
    let protocol = match args.protocol {
        ProtocolArg::Keep => Protocol::Keep,
        ProtocolArg::Meta => Protocol::Meta,
        ProtocolArg::Resp => {
            let mapping = args.mapping();
            let mut wire = Resp {
                mapping,
                script_sha: Box::default(),
                lease_renew_sha: Box::default(),
                lease_release_sha: Box::default(),
                lease_renew: LeaseRenew::Script,
                lease_release: LeaseRelease::Script,
            };
            let probe_node = match &node_map {
                Some(NodeMap::Slots(map)) => map.node_of(resp::LEASE_PROBE_KEY),
                _ => 0,
            };
            for (index, node) in nodes.iter().enumerate() {
                let mut stream = setup::connect(node).map_err(wrap("script load"))?;
                if let Some(auth) = &args.auth {
                    setup::auth(&mut stream, auth).map_err(wrap("script load"))?;
                }
                let scripts = [
                    (&mut wire.script_sha, resp::INCR_EXPIRE_SCRIPT),
                    (&mut wire.lease_renew_sha, resp::LEASE_RENEW_SCRIPT),
                    (&mut wire.lease_release_sha, resp::LEASE_RELEASE_SCRIPT),
                ];
                for (sha, script) in scripts {
                    match setup::load_script(&mut stream, script) {
                        Ok(loaded) => *sha = loaded,
                        Err(err) if mapping == RespMapping::Stalwart => {
                            return Err(wrap("script load")(err));
                        }
                        Err(_) => {}
                    }
                }
                if mapping == RespMapping::Native && index == probe_node {
                    (wire.lease_renew, wire.lease_release) =
                        setup::lease_commands(&mut stream, resp::LEASE_PROBE_KEY)
                            .map_err(wrap("lease probe"))?;
                }
            }
            Protocol::Resp(wire)
        }
    };

    let mut dropped = Vec::new();
    let loaded = match &args.trace {
        None => {
            let mut workload = load_profile(args)?;
            let unsupported: Vec<OpKind> = workload
                .kinds()
                .filter(|&kind| !protocol.supports(kind, routed))
                .collect();
            if !unsupported.is_empty() {
                if !args.drop_unsupported {
                    return fail(format!(
                        "{} cannot express {:?}; set their weight to 0 or pass --drop-unsupported",
                        protocol_name(args),
                        unsupported.iter().map(|k| k.name()).collect::<Vec<_>>()
                    ));
                }
                dropped = workload
                    .retain_kinds(|kind| protocol.supports(kind, routed))
                    .ok_or_else(|| RunError(String::from("no op left in the profile")))?
                    .into_iter()
                    .map(|kind| kind.name().to_string())
                    .collect();
            }
            if keep {
                let lock_classes = workload
                    .classes()
                    .iter()
                    .filter(|class| matches!(class.kind, OpKind::TryLock | OpKind::Renew));
                for class in lock_classes {
                    let keyspace = &workload.keyspaces()[class.keyspace as usize];
                    if keyspace.ttl == Ttl::None {
                        return fail(format!(
                            "Keep needs a TTL for {} on keyspace {}",
                            class.kind.name(),
                            keyspace.name
                        ));
                    }
                }
            }
            Loaded::Profile(Arc::new(workload))
        }
        Some(path) => {
            let options = args.trace_options().map_err(RunError)?;
            let mut trace = trace::load(path, threads, &options).map_err(wrap("trace"))?;
            let removed = trace.retain(|op| {
                let lock = matches!(op.kind, OpKind::TryLock | OpKind::Renew);
                protocol.supports(op.kind, routed)
                    && (!keep || ((1..=255).contains(&op.key_len) && (!lock || op.ttl > 0)))
            });
            if removed > 0 {
                if !args.drop_unsupported {
                    return fail(format!(
                        "{removed} trace records cannot be expressed by {}; pass --drop-unsupported",
                        protocol_name(args)
                    ));
                }
                dropped.push(format!("{removed} trace records"));
            }
            if trace.records == 0 {
                return fail("no trace record left");
            }
            Loaded::Trace(trace)
        }
    };

    let pacing = match (&loaded, rates.is_empty()) {
        (Loaded::Trace(_), true) => Pacing::Recorded {
            speed: args.trace_speed.max(1e-9),
        },
        (Loaded::Profile(_), true) if !args.preload_only => {
            return fail("--rate is required with a profile");
        }
        _ => Pacing::Arrival(args.arrival()),
    };

    let warmup_ns = duration_ns(seconds(args.warmup));
    let origin_s = match args.boundary_at {
        None => unix_now_ns() / NS_PER_SEC,
        Some(at) => {
            let next_day = (unix_now_ns() / NS_PER_SEC / DAY_S + 1) * DAY_S;
            next_day - (warmup_ns / NS_PER_SEC + at) % DAY_S
        }
    };
    let time = VirtualTime { origin_s };

    let per_thread: Vec<usize> = (0..threads)
        .map(|t| connections / threads + usize::from(t < connections % threads))
        .collect();
    let seed = args.seed;
    let (kinds, source_info, shards, span_ns): (Vec<OpKind>, SourceInfo, Vec<_>, u64) = match loaded
    {
        Loaded::Profile(workload) => {
            let values: Arc<[u8]> = value_pool(workload.max_value(), seed).into();
            let kinds = workload.kinds().collect();
            let info = profile_info(&workload, dropped);
            let shards = (0..threads)
                .map(|t| {
                    Source::Synthetic(Generator::new(
                        workload.clone(),
                        values.clone(),
                        time,
                        worker_seed(seed, t),
                    ))
                })
                .collect();
            (kinds, info, shards, 0)
        }
        Loaded::Trace(trace) => {
            let values = trace::shared_values(trace.max_value);
            let info = SourceInfo {
                kind: "trace",
                name: args
                    .trace
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default(),
                description: format!("node filter {:?}", args.trace_node),
                records: Some(trace.records),
                classes: Vec::new(),
                keyspaces: Vec::new(),
                dropped,
            };
            let span_ns = trace.span_ns;
            let kinds = trace.kinds.clone();
            let shards = trace
                .shards
                .into_iter()
                .map(|shard| Source::Trace {
                    shard,
                    values: values.clone(),
                    span_ns,
                    repeat: args.trace_repeat,
                })
                .collect();
            (kinds, info, shards, span_ns)
        }
    };

    let mut preloaders: Vec<Option<Generator>> = (0..threads).map(|_| None).collect();
    if args.trace.is_some() && (args.preload || args.preload_only) {
        let workload = Arc::new(load_profile(args)?);
        let values: Arc<[u8]> = value_pool(workload.max_value(), seed).into();
        for (t, slot) in preloaders.iter_mut().enumerate() {
            *slot = Some(Generator::new(
                workload.clone(),
                values.clone(),
                time,
                worker_seed(seed, t),
            ));
        }
    }
    let io = args.io_kind();
    let rec_template = hist_template::<u32>().map_err(wrap("histogram"))?;
    let agg_template = hist_template::<u64>().map_err(wrap("histogram"))?;
    let mut setups = Vec::with_capacity(threads);
    let mut flows = Vec::with_capacity(connections * nodes.len());
    let mut collect_attempts = 0;
    for (t, (source, preloader)) in shards.into_iter().zip(preloaders).enumerate() {
        let conns_here = per_thread.get(t).copied().unwrap_or(0);
        let mut streams = Vec::with_capacity(conns_here * nodes.len());
        match &collect {
            Some(collect) => {
                let (collected, attempts) = collect
                    .collect(conns_here, || {
                        open_stream(args, secret.as_ref(), &mut tls, &args.target)
                    })
                    .map_err(RunError)?;
                collect_attempts += attempts;
                streams = collected;
            }
            None => {
                for node in &nodes {
                    for _ in 0..conns_here {
                        let (stream, _) =
                            open_stream(args, secret.as_ref(), &mut tls, node).map_err(RunError)?;
                        streams.push(stream);
                    }
                }
            }
        }
        if tls_info.is_none()
            && let Some(info) = streams.first().and_then(Stream::tls_info)
        {
            eprintln!("tls: {} {}", info.version, info.suite);
            tls_info = Some(info);
        }
        for (conn, stream) in streams.iter().map(Stream::tcp).enumerate() {
            flows.push(Flow {
                thread: t,
                conn,
                local: stream
                    .local_addr()
                    .map(|a| a.to_string())
                    .unwrap_or_default(),
                peer: stream
                    .peer_addr()
                    .map(|a| a.to_string())
                    .unwrap_or_default(),
            });
        }
        let routing = match &node_map {
            Some(map) => Routing::Nodes {
                map: map.clone(),
                per_node: conns_here,
                next: vec![0; nodes.len()],
            },
            None => match args.select {
                SelectArg::Rr => Routing::RoundRobin { next: 0 },
                SelectArg::Least => Routing::Least { next: 0 },
            },
        };
        setups.push(WorkerSetup {
            id: t,
            threads,
            cpu: cpus.get(t).copied(),
            io,
            streams,
            protocol: protocol.clone(),
            routing,
            source,
            preloader,
            rec: Recorder::new(
                kinds.iter().copied(),
                rec_template.clone(),
                outlier_ns(args.outlier_us),
            ),
            seed: worker_seed(seed, t),
        });
    }

    let settle_ns = duration_ns(seconds(args.settle));
    let duration = duration_ns(seconds(args.duration));
    let mut host_rates = Vec::new();
    let mut segments = Vec::new();
    let mut cursor = 0u64;
    match pacing {
        Pacing::Recorded { speed } => {
            let replay_ns = (span_ns as f64 / speed) as u64;
            let measure = if args.trace_repeat {
                duration
            } else {
                replay_ns.saturating_sub(warmup_ns)
            };
            host_rates.push(0.0);
            segments.push(Segment {
                rate: 0.0,
                warm_start_ns: 0,
                meas_start_ns: warmup_ns,
                meas_end_ns: warmup_ns + measure,
            });
        }
        Pacing::Arrival(_) if !args.preload_only => {
            for (k, &rate) in rates.iter().enumerate() {
                let warm = if k == 0 { warmup_ns } else { settle_ns };
                let samples_ns = if args.min_samples > 0 {
                    (args.min_samples as f64 / rate * 1e9) as u64
                } else {
                    0
                };
                let measure = duration.max(samples_ns);
                host_rates.push(rate);
                segments.push(Segment {
                    rate: rate / threads as f64,
                    warm_start_ns: cursor,
                    meas_start_ns: cursor + warm,
                    meas_end_ns: cursor + warm + measure,
                });
                cursor += warm + measure;
            }
        }
        Pacing::Arrival(_) => {}
    }

    let plan = Plan {
        segments: &segments,
        pacing: &pacing,
        idle,
        drain_ns: duration_ns(seconds(args.drain)),
    };
    let preloading = args.preload || args.preload_only;
    let preload_depth = args.preload_depth.max(1);
    let preload_mode = match args.preload_check {
        true => PreloadMode::Check,
        false => PreloadMode::Write,
    };
    let mut out = Output::open(args.out.as_ref(), args.outlier_us > 0.0)?;
    let mut hlog_file = out.hlog.take();
    let mut outlier_file = out.outliers.take();
    let mut csv = String::from(report::CSV_HEADER);
    let mut clock_info = ClockInfo::default();
    let outcome = thread::scope(|scope| -> Result<Outcome, RunError> {
        let (tx, rx) = mpsc::channel::<Msg>();
        let preload_clock = Clock::new(Instant::now());
        let mut starts = Vec::with_capacity(threads);
        for setup in setups {
            let (start_tx, start_rx) = mpsc::channel::<Clock>();
            starts.push(start_tx);
            let tx = tx.clone();
            let preload = preloading.then_some((preload_clock, preload_depth, preload_mode));
            let plan = &plan;
            scope.spawn(move || worker_thread(setup, preload, &start_rx, plan, &tx));
        }
        drop(tx);
        let preload = match preloading {
            true => Some(collect_preload(
                &rx,
                threads,
                &preload_clock,
                &agg_template,
                preload_mode,
            )?),
            false => None,
        };
        if let Some(preload) = &preload {
            let errors = preload.errors.total();
            match preload.present {
                None => eprintln!(
                    "preload: {} keys in {:.1} s ({:.0}/s), {} errors",
                    preload.keys, preload.seconds, preload.rate, errors
                ),
                Some(present) => eprintln!(
                    "preload check: {} keys in {:.1} s, {} present, {} absent, {} errors",
                    preload.keys, preload.seconds, present, preload.misses, errors
                ),
            }
        }
        let lead = match args.start_at {
            Some(ms) => {
                let now_ms = unix_now_ns() / 1_000_000;
                if ms <= now_ms {
                    return fail("--start-at is in the past");
                }
                Duration::from_millis(ms - now_ms)
            }
            None => START_LEAD,
        };
        let clock = Clock::new(Instant::now() + lead);
        let start_unix_ms = clock.unix_base_ns() / 1_000_000;
        clock_info = ClockInfo {
            unix_base_ns: clock.unix_base_ns(),
            mono_base_ns: clock.mono_base_ns(),
        };
        if segments.is_empty() {
            return Ok(Outcome {
                preload,
                start_unix_ms,
                summaries: Vec::new(),
                failures: Vec::new(),
                worker_tids: Vec::new(),
            });
        }
        let mut serializer = V2DeflateSerializer::new();
        let hlog = match hlog_file.as_mut() {
            Some(file) => Some(
                IntervalLogWriterBuilder::new()
                    .add_comment(
                        "loadgen: tags p<point>.<op>.latency|service and p<point>.lag, values in ns",
                    )
                    .with_start_time(SystemTime::now() + lead)
                    .begin_log_with(file, &mut serializer)
                    .map_err(wrap("hdr log"))?,
            ),
            None => None,
        };
        for start in &starts {
            let _ = start.send(clock);
        }
        let mut sinks = Sinks {
            hlog,
            csv: &mut csv,
            outliers: outlier_file.as_mut(),
        };
        let collected = collect_points(
            &rx,
            &clock,
            &plan,
            &host_rates,
            args,
            &agg_template,
            &mut sinks,
        )?;
        Ok(Outcome {
            preload,
            start_unix_ms,
            summaries: collected.summaries,
            failures: collected.failures,
            worker_tids: collected.tids,
        })
    })?;
    let Outcome {
        preload,
        start_unix_ms,
        summaries,
        failures,
        worker_tids,
    } = outcome;
    if !segments.is_empty() {
        out.write("points.csv", csv.as_bytes())?;
    }
    if let Some(mut file) = hlog_file {
        file.flush().map_err(wrap("hdr log"))?;
    }
    if let Some(mut file) = outlier_file {
        file.flush().map_err(wrap("outliers"))?;
    }

    let valid = failures.is_empty()
        && summaries.iter().all(|s| s.validity.valid)
        && (args.preload_only || !summaries.is_empty());
    let summary = RunSummary {
        tool: "loadgen",
        version: env!("CARGO_PKG_VERSION"),
        label: args.label.clone(),
        args: argv,
        host: HostInfo {
            hostname: sys::hostname(),
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            cpus: sys::available_cpus(),
        },
        start_unix_ms,
        protocol: protocol_name(args),
        resp_lease: match &protocol {
            Protocol::Resp(wire) => Some([wire.lease_renew.name(), wire.lease_release.name()]),
            Protocol::Meta | Protocol::Keep => None,
        },
        io: args.io_kind().name(),
        tls: tls_info,
        target: args.target.clone(),
        cluster_nodes: if args.cluster {
            nodes.clone()
        } else {
            Vec::new()
        },
        shard_collect: collect_shards,
        keep_auth: secret.is_some(),
        select: match (&node_map, args.select) {
            (Some(_), _) => None,
            (None, SelectArg::Rr) => Some("rr"),
            (None, SelectArg::Least) => Some("least"),
        },
        shard_collect_attempts: collect.as_ref().map(|_| collect_attempts),
        threads,
        connections_per_thread: per_thread.iter().map(|c| c * nodes.len()).collect(),
        pacing: match pacing {
            Pacing::Arrival(arrival) => format!("{arrival:?}"),
            Pacing::Recorded { speed } => format!("recorded x{speed}"),
        },
        idle: match idle {
            Idle::Spin => String::from("spin"),
            Idle::Sleep { threshold_ns } => format!("sleep:{}", threshold_ns / 1_000),
        },
        virtual_time_origin_s: origin_s,
        source: source_info,
        preload,
        points: summaries,
        failures,
        valid,
        clock: clock_info,
        outlier_us: args.outlier_us,
        flows,
        worker_tids,
        worker_cpus: cpus.iter().copied().take(threads).collect(),
    };
    let json = serde_json::to_vec_pretty(&summary).map_err(wrap("summary"))?;
    out.write("summary.json", &json)?;
    Ok(valid)
}

fn collect_points<W: Write>(
    rx: &Receiver<Msg>,
    clock: &Clock,
    plan: &Plan<'_>,
    host_rates: &[f64],
    args: &RunArgs,
    agg_template: &Histogram<u64>,
    sinks: &mut Sinks<'_, '_, '_, W>,
) -> Result<Collected, RunError> {
    let segments = plan.segments;
    let threads = args.threads.max(1);
    let mut marks: Vec<(u64, usize, bool)> = segments
        .iter()
        .enumerate()
        .flat_map(|(k, seg)| [(seg.meas_start_ns, k, false), (seg.meas_end_ns, k, true)])
        .collect();
    marks.sort_unstable_by_key(|&(t, ..)| t);
    let mut samples: Vec<PointMarks> = vec![[None, None]; segments.len()];
    let mut aggs: Vec<PointAgg> = segments
        .iter()
        .map(|_| PointAgg::new(agg_template))
        .collect();
    let mut finished = vec![false; segments.len()];
    let mut pending: VecDeque<usize> = VecDeque::new();
    let mut summaries: Vec<PointSummary> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    let mut tids = vec![-1i64; threads];
    let mut next_mark = 0;
    loop {
        while let Some(&(t, point, end)) = marks.get(next_mark) {
            if clock.now_ns() < t {
                break;
            }
            samples[point][usize::from(end)] = Some(MarkSample {
                retrans: sys::tcp_retrans_segs(),
                cpu_ns: sys::process_cpu_ns(),
            });
            next_mark += 1;
        }
        let timeout = marks
            .get(next_mark)
            .map_or(Duration::from_millis(500), |&(t, ..)| {
                Duration::from_nanos(t.saturating_sub(clock.now_ns()))
            });
        match rx.recv_timeout(timeout) {
            Ok(Msg::Phase {
                worker,
                tid,
                point,
                mut stats,
            }) => {
                if let Some(slot) = tids.get_mut(worker) {
                    *slot = tid;
                }
                let Some(agg) = aggs.get_mut(point) else {
                    continue;
                };
                agg.add(worker, &mut stats);
                if agg.workers == threads {
                    finished[point] = true;
                    pending.push_back(point);
                }
            }
            Ok(Msg::Failed { worker, error }) => {
                eprintln!("worker {worker} failed: {error}");
                failures.push(format!("worker {worker}: {error}"));
            }
            Ok(Msg::Preloaded { .. }) | Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        while let Some(&point) = pending.front() {
            let end_mark = segments[point].meas_end_ns;
            if samples[point][1].is_none() && clock.now_ns() < end_mark {
                break;
            }
            if samples[point][1].is_none() {
                samples[point][1] = Some(MarkSample {
                    retrans: sys::tcp_retrans_segs(),
                    cpu_ns: sys::process_cpu_ns(),
                });
            }
            pending.pop_front();
            let mut summary = finish_point(
                point,
                &aggs[point],
                segments,
                host_rates,
                &samples,
                args,
                &failures,
            );
            stamp_window(&mut summary, &segments[point], clock);
            emit(&summary, &aggs[point], &segments[point], sinks)?;
            summaries.push(summary);
        }
    }
    for (point, agg) in aggs.iter().enumerate() {
        if finished[point] || agg.workers == 0 {
            continue;
        }
        let mut summary = finish_point(point, agg, segments, host_rates, &samples, args, &failures);
        stamp_window(&mut summary, &segments[point], clock);
        summary.validity.valid = false;
        summary.validity.reasons.push(format!(
            "only {} of {threads} workers reported",
            agg.workers
        ));
        eprintln!("{}", report::console_line(&summary));
        emit(&summary, agg, &segments[point], sinks)?;
        summaries.push(summary);
    }
    summaries.sort_by_key(|s| s.point);
    Ok(Collected {
        summaries,
        failures,
        tids,
    })
}

struct Collected {
    summaries: Vec<PointSummary>,
    failures: Vec<String>,
    tids: Vec<i64>,
}

fn stamp_window(summary: &mut PointSummary, seg: &Segment, clock: &Clock) {
    summary.meas_start_unix_ns = clock.unix_base_ns() + seg.meas_start_ns;
    summary.meas_end_unix_ns = clock.unix_base_ns() + seg.meas_end_ns;
}

fn finish_point(
    point: usize,
    agg: &PointAgg,
    segments: &[Segment],
    host_rates: &[f64],
    samples: &[PointMarks],
    args: &RunArgs,
    failures: &[String],
) -> PointSummary {
    let seg = &segments[point];
    let (retrans, cpu) = match samples[point] {
        [Some(start), Some(end)] => (
            start
                .retrans
                .zip(end.retrans)
                .map(|(a, b)| b.saturating_sub(a)),
            Some(end.cpu_ns.saturating_sub(start.cpu_ns)),
        ),
        _ => (None, None),
    };
    let ctx = PointContext {
        point,
        offered_rate: host_rates.get(point).copied().unwrap_or(0.0),
        measure_ns: seg.meas_end_ns - seg.meas_start_ns,
        threads: args.threads.max(1),
        thresholds: args.thresholds(),
        tcp_retrans: retrans,
        process_cpu_ns: cpu,
    };
    let mut summary = agg.summarize(&ctx);
    if !failures.is_empty() {
        summary.validity.valid = false;
        summary
            .validity
            .reasons
            .push(String::from("a worker failed during the run"));
    }
    eprintln!("{}", report::console_line(&summary));
    summary
}

struct Sinks<'a, 'b, 'c, W: Write> {
    hlog: Option<IntervalLogWriter<'a, 'b, W, V2DeflateSerializer>>,
    csv: &'c mut String,
    outliers: Option<&'c mut BufWriter<fs::File>>,
}

fn emit<W: Write>(
    summary: &PointSummary,
    agg: &PointAgg,
    seg: &Segment,
    sinks: &mut Sinks<'_, '_, '_, W>,
) -> Result<(), RunError> {
    report::csv_rows(summary, sinks.csv);
    if let Some(log) = sinks.hlog.as_mut() {
        agg.write_hlog(
            log,
            summary.point,
            Duration::from_nanos(seg.meas_start_ns),
            Duration::from_nanos(seg.meas_end_ns - seg.meas_start_ns),
        )
        .map_err(wrap("hdr log"))?;
    }
    if let Some(file) = sinks.outliers.as_mut() {
        for (worker, outlier) in &agg.outliers {
            writeln!(
                file,
                "{{\"point\":{},\"thread\":{worker},\"conn\":{},\"op\":\"{}\",\"intended_ns\":{},\"lag_ns\":{},\"latency_ns\":{}}}",
                summary.point,
                outlier.conn,
                outlier.kind.name(),
                outlier.intended_ns,
                outlier.lag_ns,
                outlier.latency_ns
            )
            .map_err(wrap("outliers"))?;
        }
    }
    Ok(())
}

struct Plan<'a> {
    segments: &'a [Segment],
    pacing: &'a Pacing,
    idle: Idle,
    drain_ns: u64,
}

struct Outcome {
    preload: Option<PreloadSummary>,
    start_unix_ms: u64,
    summaries: Vec<PointSummary>,
    failures: Vec<String>,
    worker_tids: Vec<i64>,
}

fn worker_thread(
    setup: WorkerSetup,
    preload: Option<(Clock, usize, PreloadMode)>,
    start: &Receiver<Clock>,
    plan: &Plan<'_>,
    tx: &Sender<Msg>,
) {
    if let Some(warning) = setup.pin() {
        eprintln!("{warning}");
    }
    let id = setup.id;
    let mut worker = match Worker::new(setup) {
        Ok(worker) => worker,
        Err(e) => {
            let _ = tx.send(Msg::Failed {
                worker: id,
                error: format!("i/o setup: {e}"),
            });
            return;
        }
    };
    if let Some((clock, depth, mode)) = preload {
        worker.preload(&clock, depth, mode, tx);
    }
    let Ok(clock) = start.recv() else {
        return;
    };
    if let Some(wait) = clock
        .base()
        .saturating_duration_since(Instant::now())
        .checked_sub(Duration::from_millis(2))
    {
        thread::sleep(wait);
    }
    worker.run(
        &clock,
        plan.segments,
        plan.pacing,
        plan.idle,
        plan.drain_ns,
        tx,
    );
}

fn collect_preload(
    rx: &Receiver<Msg>,
    threads: usize,
    clock: &Clock,
    template: &Histogram<u64>,
    mode: PreloadMode,
) -> Result<PreloadSummary, RunError> {
    let mut agg = PointAgg::new(template);
    let mut failures = Vec::new();
    let mut reported = 0;
    while reported < threads {
        match rx.recv() {
            Ok(Msg::Preloaded { worker, mut stats }) => {
                agg.add(worker, &mut stats);
                reported += 1;
            }
            Ok(Msg::Failed { worker, error }) => {
                failures.push(format!("worker {worker}: {error}"));
                reported += 1;
            }
            Ok(Msg::Phase { .. }) => {}
            Err(_) => break,
        }
    }
    if !failures.is_empty() {
        return fail(format!("preload failed: {}", failures.join("; ")));
    }
    let seconds = clock.now_ns() as f64 / 1e9;
    let summary = agg.summarize(&PointContext {
        point: PRELOAD_PHASE as usize,
        offered_rate: 0.0,
        measure_ns: clock.now_ns(),
        threads,
        thresholds: report::Thresholds {
            max_util: 1.0,
            max_lag_p99_ns: u64::MAX,
            max_lag_p999_ns: u64::MAX,
        },
        tcp_retrans: None,
        process_cpu_ns: None,
    });
    let present = match mode {
        PreloadMode::Write => None,
        PreloadMode::Check => Some(
            summary
                .issued
                .saturating_sub(summary.all.miss.saturating_add(summary.errors.total())),
        ),
    };
    Ok(PreloadSummary {
        mode,
        keys: summary.issued,
        present,
        seconds,
        rate: summary.issued as f64 / seconds.max(1e-9),
        errors: summary.errors,
        misses: summary.all.miss,
        latency: summary.all.latency,
    })
}

struct Output {
    dir: Option<PathBuf>,
    hlog: Option<BufWriter<fs::File>>,
    outliers: Option<BufWriter<fs::File>>,
}

impl Output {
    fn open(dir: Option<&PathBuf>, outliers: bool) -> Result<Self, RunError> {
        let Some(dir) = dir else {
            return Ok(Output {
                dir: None,
                hlog: None,
                outliers: None,
            });
        };
        fs::create_dir_all(dir).map_err(wrap("output directory"))?;
        let hlog = fs::File::create(dir.join("hdr.hlog")).map_err(wrap("hdr log"))?;
        let outliers = match outliers {
            true => Some(BufWriter::new(
                fs::File::create(dir.join("outliers.jsonl")).map_err(wrap("outliers"))?,
            )),
            false => None,
        };
        Ok(Output {
            dir: Some(dir.clone()),
            hlog: Some(BufWriter::new(hlog)),
            outliers,
        })
    }

    fn write(&self, name: &str, bytes: &[u8]) -> Result<(), RunError> {
        match &self.dir {
            Some(dir) => fs::write(dir.join(name), bytes).map_err(wrap(name)),
            None => Ok(()),
        }
    }
}

fn load_profile(args: &RunArgs) -> Result<Workload, RunError> {
    let text: Cow<'_, str> = match profile::builtin_text(&args.profile) {
        Some(text) => text.map_err(wrap("profile"))?,
        None => Cow::Owned(fs::read_to_string(&args.profile).map_err(wrap("cannot read profile"))?),
    };
    let overrides = Overrides {
        popularity: args.popularity().map_err(RunError)?,
        keys_scale: args.keys_scale,
    };
    profile::parse(&text, &overrides).map_err(wrap("profile"))
}

fn outlier_ns(us: f64) -> u64 {
    match us > 0.0 {
        true => (us * 1e3) as u64,
        false => u64::MAX,
    }
}

fn worker_seed(seed: u64, worker: usize) -> u64 {
    seed.wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add((worker as u64 + 1).wrapping_mul(0xbf58_476d_1ce4_e5b9))
}

fn protocol_name(args: &RunArgs) -> String {
    match args.protocol {
        ProtocolArg::Resp => format!(
            "resp-{}{}",
            match args.mapping() {
                RespMapping::Stalwart => "stalwart",
                RespMapping::Native => "native",
            },
            if args.cluster { "-cluster" } else { "" }
        ),
        ProtocolArg::Meta => String::from("memcached-meta"),
        ProtocolArg::Keep => String::from("keep-v1"),
    }
}

fn profile_info(workload: &Workload, dropped: Vec<String>) -> SourceInfo {
    SourceInfo {
        kind: "profile",
        name: workload.name().to_string(),
        description: workload.description().to_string(),
        records: None,
        classes: workload
            .classes()
            .iter()
            .map(|class| ClassInfo {
                op: class.kind.name(),
                keyspace: workload.keyspaces()[class.keyspace as usize]
                    .name
                    .to_string(),
                share: class.share,
            })
            .collect(),
        keyspaces: workload
            .keyspaces()
            .iter()
            .map(|ks| KeyspaceInfo {
                name: ks.name.to_string(),
                keys: ks.count,
                popularity: format!("{:?}", ks.popularity),
                ttl: format!("{:?}", ks.ttl),
            })
            .collect(),
        dropped,
    }
}
