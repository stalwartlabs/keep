/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use keep_jepsen::check::{CLOCK_MARGIN_PPM, check_locks};
use keep_jepsen::history::parse;
use keep_jepsen::large;
use keep_jepsen::owners::write_proxy;
use keep_jepsen::proxy;
use keep_jepsen::tls::RpkFiles;
use keep_proto::ClusterSecret;

const SECRET: &str = "00112233445566778899aabbccddeeff";

#[derive(Parser)]
#[command(
    about = "Keep Jepsen support: client proxy, large-value writer, TLS keys, key owners and the lock checker"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Proxy {
        #[arg(long, default_value = "127.0.0.1:7100")]
        listen: SocketAddr,
        #[arg(long)]
        url: String,
    },
    LargeWriter {
        #[arg(long)]
        url: String,
        #[arg(long, default_value_t = 20)]
        period_ms: u64,
    },
    RpkKeys {
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        servers: u8,
    },
    ProxyOwners {
        #[arg(long)]
        members: String,
        #[arg(long)]
        keys: u64,
        #[arg(long)]
        out: PathBuf,
    },
    CheckLocks {
        history: PathBuf,
        #[arg(long, default_value_t = CLOCK_MARGIN_PPM)]
        margin_ppm: u64,
    },
}

enum Verdict {
    Holds,
    Violated,
    Malformed,
}

impl From<Verdict> for ExitCode {
    fn from(verdict: Verdict) -> ExitCode {
        match verdict {
            Verdict::Holds => ExitCode::SUCCESS,
            Verdict::Violated => ExitCode::FAILURE,
            Verdict::Malformed => ExitCode::from(2),
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let result = match Cli::parse().command {
        Command::Proxy { listen, url } => proxy::serve(listen, url).await.map(|()| Verdict::Holds),
        Command::LargeWriter { url, period_ms } => {
            large::write(url, Duration::from_millis(period_ms))
                .await
                .map(|()| Verdict::Holds)
        }
        Command::RpkKeys { out, servers } => {
            RpkFiles::generate(&out, servers).map(|_| Verdict::Holds)
        }
        Command::ProxyOwners { members, keys, out } => proxy_owners(&members, keys, &out),
        Command::CheckLocks {
            history,
            margin_ppm,
        } => locks(&history, margin_ppm),
    };
    match result {
        Ok(verdict) => verdict.into(),
        Err(error) => {
            eprintln!("keep-jepsen: {error}");
            ExitCode::from(2)
        }
    }
}

fn proxy_owners(members: &str, keys: u64, out: &Path) -> io::Result<Verdict> {
    let secret = ClusterSecret::from_hex(SECRET)
        .ok_or_else(|| io::Error::other("invalid cluster secret"))?;
    write_proxy(out, members, secret.hasher(), keys)?;
    Ok(Verdict::Holds)
}

fn locks(path: &Path, margin_ppm: u64) -> io::Result<Verdict> {
    let text = fs::read_to_string(path)?;
    let history =
        parse(&text).map_err(|error| io::Error::other(format!("{}: {error}", path.display())))?;
    let report = check_locks(&history, margin_ppm);
    for violation in &report.violations {
        println!(
            "key {}: token {} (client {}, record {}, {} to {} ns) overlaps token {} (client {}, record {}, {} to {} ns)",
            violation.key,
            violation.later.token,
            violation.later.client,
            violation.later.record,
            violation.later.start_ns,
            violation.later.end_ns,
            violation.earlier.token,
            violation.earlier.client,
            violation.earlier.record,
            violation.earlier.start_ns,
            violation.earlier.end_ns,
        );
    }
    println!(
        "locks: {} records, {} holds, {} malformed, {} violations",
        history.len(),
        report.holds,
        report.malformed,
        report.violations.len()
    );
    Ok(match (report.violations.len(), report.malformed) {
        (0, 0) => Verdict::Holds,
        (0, _) => Verdict::Malformed,
        _ => Verdict::Violated,
    })
}
