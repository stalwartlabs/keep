/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use loadgen::cli::parse_cpus;
use loadgen::server::{ServerConfig, ServerProtocol, cluster_slots_reply, serve};

#[derive(ValueEnum, Clone, Copy, Debug)]
enum ProtocolArg {
    Resp,
    Meta,
    Keep,
}

#[derive(Parser, Debug)]
#[command(
    name = "nullserver",
    about = "Canned-reply server for the generator ceiling check"
)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:6379")]
    listen: String,
    #[arg(long, value_enum, default_value = "resp")]
    protocol: ProtocolArg,
    #[arg(long, default_value_t = 1)]
    threads: usize,
    #[arg(long, help = "CPUs to pin worker threads to, e.g. 0-15 (Linux)")]
    cpus: Option<String>,
    #[arg(long, help = "Busy-poll instead of blocking in epoll")]
    spin: bool,
    #[arg(
        long,
        value_delimiter = ',',
        help = "host:port list announced by CLUSTER SLOTS, slots split evenly"
    )]
    cluster_nodes: Vec<String>,
    #[arg(long, default_value_t = 1.0, help = "Seconds between rate reports")]
    report_every: f64,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let cpus = match args.cpus.as_deref().map(parse_cpus).transpose() {
        Ok(cpus) => cpus.unwrap_or_default(),
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut nodes = Vec::with_capacity(args.cluster_nodes.len().max(1));
    for node in &args.cluster_nodes {
        match node
            .rsplit_once(':')
            .and_then(|(h, p)| Some((h.to_string(), p.parse().ok()?)))
        {
            Some(node) => nodes.push(node),
            None => {
                eprintln!("error: invalid cluster node {node:?}");
                return ExitCode::FAILURE;
            }
        }
    }
    if nodes.is_empty() {
        let (host, port) = args
            .listen
            .rsplit_once(':')
            .unwrap_or(("127.0.0.1", "6379"));
        let host = if host == "0.0.0.0" { "127.0.0.1" } else { host };
        nodes.push((host.to_string(), port.parse().unwrap_or(6379)));
    }
    let protocol = match args.protocol {
        ProtocolArg::Resp => ServerProtocol::Resp,
        ProtocolArg::Meta => ServerProtocol::Meta,
        ProtocolArg::Keep => ServerProtocol::Keep,
    };
    let config = ServerConfig {
        listen: args.listen,
        protocol,
        threads: args.threads,
        cpus,
        spin: args.spin,
        cluster_slots: cluster_slots_reply(&nodes),
        report_every: Duration::from_secs_f64(args.report_every.max(0.1)),
    };
    match serve(config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
