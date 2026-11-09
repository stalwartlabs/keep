/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

mod input;
mod timeline;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

use self::input::{port_of, read_lines, read_run};
use self::timeline::{Host, Side};

pub struct Options<'a> {
    pub run: &'a Path,
    pub client: Option<&'a Path>,
    pub server: Option<&'a Path>,
    pub min_us: Option<f64>,
    pub tolerance_us: f64,
    pub explain_share: f64,
    pub out: Option<&'a Path>,
}

macro_rules! causes {
    ($($variant:ident => $name:literal),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        enum Cause {
            $($variant),+
        }

        impl Cause {
            fn name(self) -> &'static str {
                match self {
                    $(Cause::$variant => $name),+
                }
            }
        }
    };
}

causes! {
    TcpRetransmit => "tcp_retransmit",
    ClientLag => "client_schedule_lag",
    ClientSched => "client_preempt_or_runq",
    ClientSteal => "client_steal",
    ClientIrq => "client_irq",
    ShardQueueing => "server_shard_queueing",
    CrossShardHop => "server_cross_shard_hop",
    ServerExec => "server_exec",
    ServerSend => "server_send",
    ServerSched => "server_preempt_or_runq",
    ServerSteal => "server_steal",
    ServerIrq => "server_irq",
    GlobalStall => "global_stall",
    Unattributed => "unattributed",
}

const NS_PER_MS: u64 = 1_000_000;
const STALL_SHARE: f64 = 0.25;
const STALL_MIN_CONNS: usize = 4;

struct Stalls {
    conns: HashMap<(usize, u64), HashSet<(usize, usize)>>,
    spread: HashMap<(usize, u64), usize>,
    threshold: usize,
}

impl Stalls {
    fn new(threshold: usize) -> Stalls {
        Stalls {
            conns: HashMap::new(),
            spread: HashMap::new(),
            threshold,
        }
    }

    fn add(&mut self, point: usize, done_ms: u64, conn: (usize, usize)) {
        self.conns.entry((point, done_ms)).or_default().insert(conn);
    }

    fn conns_around(&mut self, point: usize, done_ms: u64) -> usize {
        if let Some(&spread) = self.spread.get(&(point, done_ms)) {
            return spread;
        }
        let spread = (done_ms.saturating_sub(1)..=done_ms + 1)
            .filter_map(|ms| self.conns.get(&(point, ms)))
            .flatten()
            .collect::<HashSet<_>>()
            .len();
        self.spread.insert((point, done_ms), spread);
        spread
    }

    fn events(&mut self) -> BTreeMap<usize, u64> {
        let mut keys: Vec<(usize, u64)> = self.conns.keys().copied().collect();
        keys.sort_unstable();
        let mut events: BTreeMap<usize, u64> = BTreeMap::new();
        let mut last: Option<(usize, u64)> = None;
        for (point, ms) in keys {
            if self.conns_around(point, ms) < self.threshold {
                continue;
            }
            if last.is_none_or(|(p, m)| p != point || ms > m + 1) {
                *events.entry(point).or_default() += 1;
            }
            last = Some((point, ms));
        }
        events
    }
}

struct Window {
    start: u64,
    send: u64,
    end: u64,
}

pub fn attribute(options: &Options<'_>) -> Result<String, String> {
    let (info, outliers) = read_run(options.run)?;
    let client = options
        .client
        .map(|path| read_lines(path).map(|events| Host::from_events(events, Side::Client)))
        .transpose()?;
    let server = options
        .server
        .map(|path| read_lines(path).map(|events| Host::from_events(events, Side::Server)))
        .transpose()?;
    let server_shift = match (
        client.as_ref().and_then(Host::offset),
        server.as_ref().and_then(Host::offset),
    ) {
        (Some(client), Some(server)) => Some(server - client),
        _ => None,
    };
    let to_server = |mono: u64| -> u64 {
        let shifted = i128::from(mono) - server_shift.unwrap_or(0);
        u64::try_from(shifted.max(0)).unwrap_or(u64::MAX)
    };
    let flows: HashMap<(usize, usize), (u16, u16)> = info
        .flows
        .iter()
        .filter_map(|flow| {
            Some((
                (flow.thread, flow.conn),
                (port_of(&flow.local)?, port_of(&flow.peer)?),
            ))
        })
        .collect();
    let points: HashMap<usize, (u64, u64)> = info
        .points
        .iter()
        .map(|point| {
            let p50 = (point.all.latency.p50_us * 1e3) as u64;
            let floor = match options.min_us {
                Some(us) => (us * 1e3) as u64,
                None => (point.all.latency.p999_us * 1e3) as u64,
            };
            (point.point, (p50, floor))
        })
        .collect();
    let tolerance = (options.tolerance_us * 1e3) as u64;
    let mut out = options
        .out
        .map(|path| {
            fs::File::create(path)
                .map(BufWriter::new)
                .map_err(|e| format!("{}: {e}", path.display()))
        })
        .transpose()?;
    let selected = |outlier: &&input::Outlier| {
        points
            .get(&outlier.point)
            .is_some_and(|&(_, floor)| outlier.latency_ns >= floor)
    };
    let mut stalls =
        Stalls::new(((info.flows.len() as f64 * STALL_SHARE) as usize).max(STALL_MIN_CONNS));
    for outlier in outliers.iter().filter(selected) {
        stalls.add(
            outlier.point,
            (outlier.intended_ns + outlier.latency_ns) / NS_PER_MS,
            (outlier.thread, outlier.conn),
        );
    }
    let mut tally: BTreeMap<usize, BTreeMap<Cause, u64>> = BTreeMap::new();
    let mut found: Vec<(Cause, u64, Option<&str>)> = Vec::with_capacity(16);
    for outlier in outliers.iter().filter(selected) {
        let Some(&(p50, _)) = points.get(&outlier.point) else {
            continue;
        };
        let start = info.clock.mono_base_ns + outlier.intended_ns;
        let window = Window {
            start,
            send: start + outlier.lag_ns,
            end: start + outlier.latency_ns,
        };
        let flow = flows.get(&(outlier.thread, outlier.conn)).copied();
        let tid = info
            .worker_tids
            .get(outlier.thread)
            .copied()
            .filter(|&t| t > 0);
        let cpu = info.worker_cpus.get(outlier.thread).copied();
        found.clear();
        found.push((Cause::ClientLag, outlier.lag_ns, None));
        if let Some(client) = &client {
            if let Some(loss) = flow.and_then(|flow| {
                client.retransmit(
                    flow,
                    window.start.saturating_sub(tolerance),
                    window.end + tolerance,
                )
            }) {
                found.push((Cause::TcpRetransmit, outlier.latency_ns, Some(loss.name())));
            }
            found.push((
                Cause::ClientSched,
                client.sched(tid, window.start, window.end),
                None,
            ));
            found.push((
                Cause::ClientIrq,
                client.irq(cpu, window.start, window.end),
                None,
            ));
            found.push((
                Cause::ClientSteal,
                client.steal(cpu, window.start, window.end),
                None,
            ));
        }
        if let Some(server) = &server {
            let from = to_server(window.send).saturating_sub(tolerance);
            let to = to_server(window.end) + tolerance;
            if let Some(flow) = flow {
                if let Some(loss) =
                    server.retransmit(flow, to_server(window.start).saturating_sub(tolerance), to)
                {
                    found.push((Cause::TcpRetransmit, outlier.latency_ns, Some(loss.name())));
                }
                if let Some(stage) = server.stage(flow.0, from, to) {
                    let tag = stage.cause.as_deref();
                    found.push((Cause::ShardQueueing, stage.queue_ns, None));
                    found.push((Cause::CrossShardHop, stage.hop_ns, None));
                    found.push((Cause::ServerExec, stage.exec_ns, tag));
                    found.push((Cause::ServerSend, stage.send_ns, None));
                }
            }
            found.push((Cause::ServerSched, server.sched(None, from, to), None));
            found.push((Cause::ServerIrq, server.irq(None, from, to), None));
            found.push((Cause::ServerSteal, server.steal(None, from, to), None));
        }
        let excess = outlier.latency_ns.saturating_sub(p50);
        let needed = (excess as f64 * options.explain_share) as u64;
        let primary = found
            .iter()
            .filter(|&&(_, explained, _)| explained > 0 && explained >= needed)
            .max_by_key(|&&(cause, explained, _)| (explained, std::cmp::Reverse(cause)))
            .copied();
        let stall_conns = stalls.conns_around(
            outlier.point,
            (outlier.intended_ns + outlier.latency_ns) / NS_PER_MS,
        );
        let cause = match primary {
            Some((cause, ..)) => cause,
            None if stall_conns >= stalls.threshold => Cause::GlobalStall,
            None => Cause::Unattributed,
        };
        *tally
            .entry(outlier.point)
            .or_default()
            .entry(cause)
            .or_default() += 1;
        if let Some(file) = out.as_mut() {
            let mut line = format!(
                "{{\"point\":{},\"thread\":{},\"conn\":{},\"op\":\"{}\",\"intended_ns\":{},\"latency_us\":{:.1},\"lag_us\":{:.1},\"stall_conns\":{stall_conns},\"primary\":\"{}\"",
                outlier.point,
                outlier.thread,
                outlier.conn,
                outlier.op,
                outlier.intended_ns,
                outlier.latency_ns as f64 / 1e3,
                outlier.lag_ns as f64 / 1e3,
                cause.name()
            );
            if let Some(tag) = primary.and_then(|(_, _, tag)| tag) {
                let _ = write!(line, ",\"tag\":\"{tag}\"");
            }
            line.push_str(",\"explained_us\":{");
            let mut first = true;
            for &(cause, explained, _) in found.iter().filter(|(_, e, _)| *e > 0) {
                let _ = write!(
                    line,
                    "{}\"{}\":{:.1}",
                    if first { "" } else { "," },
                    cause.name(),
                    explained as f64 / 1e3
                );
                first = false;
            }
            line.push_str("}}\n");
            file.write_all(line.as_bytes())
                .map_err(|e| format!("attribution output: {e}"))?;
        }
    }
    if let Some(mut file) = out {
        file.flush()
            .map_err(|e| format!("attribution output: {e}"))?;
    }
    let stall_events = stalls.events();
    Ok(report(
        &tally,
        &stall_events,
        stalls.threshold,
        server_shift,
        options,
    ))
}

fn report(
    tally: &BTreeMap<usize, BTreeMap<Cause, u64>>,
    stall_events: &BTreeMap<usize, u64>,
    stall_threshold: usize,
    server_shift: Option<i128>,
    options: &Options<'_>,
) -> String {
    let mut text = String::new();
    let _ = writeln!(
        text,
        "Outliers {} attributed when one cause explains at least {:.0}% of the latency above the point's p50.",
        match options.min_us {
            Some(us) => format!("above {us} us"),
            None => String::from("at or above each point's p99.9"),
        },
        options.explain_share * 100.0
    );
    match server_shift {
        Some(shift) => {
            let _ = writeln!(
                text,
                "Server monotonic clock minus the client's: {:.1} us (from both hosts' realtime/monotonic pairs; assumes their Unix clocks agree within the {} us tolerance).\n",
                shift as f64 / 1e3,
                options.tolerance_us
            );
        }
        None if options.server.is_some() => {
            text.push_str(
                "No clock samples on both hosts: server events compared without a shift.\n\n",
            );
        }
        None => text.push('\n'),
    }
    let _ = writeln!(
        text,
        "global_stall: no probe explains the outlier and at least {stall_threshold} connections complete outliers within the same +-1 ms.\n"
    );
    text.push_str(
        "| point | outliers | attributed | stall events | causes |\n|---|---|---|---|---|\n",
    );
    for (point, causes) in tally {
        let total: u64 = causes.values().sum();
        let unattributed = causes.get(&Cause::Unattributed).copied().unwrap_or(0);
        let mut ranked: Vec<(&Cause, &u64)> = causes.iter().collect();
        ranked.sort_unstable_by(|a, b| b.1.cmp(a.1));
        let _ = write!(
            text,
            "| {point} | {total} | {:.1}% | {} | ",
            (total - unattributed) as f64 * 100.0 / total.max(1) as f64,
            stall_events.get(point).copied().unwrap_or(0)
        );
        for (i, (cause, count)) in ranked.iter().enumerate() {
            let _ = write!(
                text,
                "{}{} {count}",
                if i > 0 { ", " } else { "" },
                cause.name()
            );
        }
        text.push_str(" |\n");
    }
    text
}
