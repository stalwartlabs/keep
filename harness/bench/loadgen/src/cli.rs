/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use keep_client::{Identity, Tls};
use keep_proto::ClusterSecret;

use crate::arrival::Arrival;
use crate::io::IoKind;
use crate::protocol::resp::RespMapping;
use crate::protocol::shard::ShardMap;
use crate::report::Thresholds;
use crate::worker::Idle;
use crate::workload::keys::Popularity;
use crate::workload::profile;
use crate::workload::trace::{Salt, TraceOptions};

#[derive(Parser, Debug)]
#[command(
    name = "loadgen",
    about = "Open-loop KV load generator for Stalwart Keep"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Run(Box<RunArgs>),
    Profile { name: String },
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolArg {
    Resp,
    Meta,
    Keep,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum MappingArg {
    Stalwart,
    Native,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoArg {
    Epoll,
    Uring,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsArg {
    Off,
    Rpk,
    Ca,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteArg {
    None,
    Shards,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectArg {
    Rr,
    Least,
}

const SECRET_ENV: &str = "KEEP_SECRET";

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArrivalArg {
    Poisson,
    Constant,
}

#[derive(Args, Debug)]
pub struct RunArgs {
    #[arg(
        long,
        required = true,
        help = "Server address host:port; with --cluster, the seed node"
    )]
    pub target: String,
    #[arg(long, value_enum, default_value = "resp")]
    pub protocol: ProtocolArg,
    #[arg(long, value_enum, default_value = "stalwart", help = "RESP op mapping")]
    pub resp_mapping: MappingArg,
    #[arg(long, help = "Redis Cluster slot-aware mode (RESP only)")]
    pub cluster: bool,
    #[arg(
        long,
        conflicts_with = "cluster",
        help = "Keep shards behind the one port of --target: reconnect until every shard named by the Hello reply holds --connections connections; every key goes to its owner's connection"
    )]
    pub shard_collect: Option<u16>,
    #[arg(
        long,
        value_parser = parse_secret,
        help = "Cluster secret keying shard placement, 32 hex digits; default: the Keep cluster secret"
    )]
    pub placement_secret: Option<ClusterSecret>,
    #[arg(
        long,
        value_enum,
        default_value = "none",
        help = "Keep routing: none, or shards (hold --connections connections per shard behind the one port of --target, shard count from the Hello reply, every key to its shard's connection)"
    )]
    pub route: RouteArg,
    #[arg(
        long,
        value_enum,
        default_value = "rr",
        help = "Connection choice without routing: rr (round robin) or least (fewest requests in flight)"
    )]
    pub select: SelectArg,
    #[arg(
        long,
        help = "File holding the Keep cluster secret, 32 hex digits, for plaintext Auth (PROTOCOL 10.2) and placement; default: the KEEP_SECRET variable"
    )]
    pub secret_file: Option<PathBuf>,
    #[arg(long, help = "RESP AUTH as PASSWORD or USER:PASSWORD")]
    pub auth: Option<String>,
    #[arg(
        long,
        value_enum,
        default_value = "epoll",
        help = "Socket I/O: epoll (mio readiness) or uring (io_uring, Linux)"
    )]
    pub io: IoArg,
    #[arg(
        long,
        value_enum,
        default_value = "off",
        help = "TLS 1.3 under --protocol keep, configured by keep-client: rpk (raw public keys: --tls-key and --tls-servers), ca (--tls-ca, optional --tls-server-name, --tls-cert with --tls-key for a client identity) or off; runs on epoll"
    )]
    pub tls: TlsArg,
    #[arg(
        long,
        help = "TLS private key: the client's raw public key (rpk) or the key of --tls-cert (ca)"
    )]
    pub tls_key: Option<String>,
    #[arg(long, help = "TLS rpk: the servers' public keys")]
    pub tls_servers: Option<String>,
    #[arg(long, help = "TLS ca: the cluster CA the servers' chains lead to")]
    pub tls_ca: Option<String>,
    #[arg(long, help = "TLS ca: the client's certificate chain, with --tls-key")]
    pub tls_cert: Option<String>,
    #[arg(
        long,
        help = "TLS ca: the name the server certificates must carry (default: the target host)"
    )]
    pub tls_server_name: Option<String>,
    #[arg(long, default_value_t = 1)]
    pub threads: usize,
    #[arg(
        long,
        help = "CPUs to pin worker threads to, e.g. 0-15 or 0,2,4 (Linux)"
    )]
    pub cpus: Option<String>,
    #[arg(
        long,
        help = "Connections per host (per node with --cluster, per shard with --shard-collect or --route shards); default one per thread"
    )]
    pub connections: Option<usize>,
    #[arg(
        long,
        default_value = "stalwart-mix",
        help = "Built-in profile name or TOML file"
    )]
    pub profile: String,
    #[arg(
        long,
        help = "Override key popularity of every keyspace: uniform or zipf:<s>"
    )]
    pub popularity: Option<String>,
    #[arg(
        long,
        default_value_t = 1.0,
        help = "Scale the key count of every keyspace"
    )]
    pub keys_scale: f64,
    #[arg(long, help = "Replay a Stalwart KV trace (TSV) instead of a profile")]
    pub trace: Option<PathBuf>,
    #[arg(long, help = "Replay only this Stalwart node of the trace")]
    pub trace_node: Option<u16>,
    #[arg(
        long,
        default_value_t = 1.0,
        help = "Recorded-time replay speed factor"
    )]
    pub trace_speed: f64,
    #[arg(long, help = "Loop the trace when it ends")]
    pub trace_repeat: bool,
    #[arg(
        long,
        help = "XOR this salt into key bytes 1..=8 (emulates another Stalwart node)"
    )]
    pub trace_salt: Option<u64>,
    #[arg(
        long,
        value_delimiter = ',',
        help = "Hex key prefixes the salt applies to, e.g. 0e,0f,10,11 (default all)"
    )]
    pub trace_salt_prefixes: Vec<String>,
    #[arg(long, value_enum, default_value = "poisson")]
    pub arrival: ArrivalArg,
    #[arg(
        long,
        value_delimiter = ',',
        help = "Offered ops/s of this host per load point, e.g. 100k,200k,1M; with --trace, re-times the trace"
    )]
    pub rate: Vec<String>,
    #[arg(
        long,
        default_value_t = 30.0,
        help = "Warm-up before the first point, seconds"
    )]
    pub warmup: f64,
    #[arg(
        long,
        default_value_t = 5.0,
        help = "Warm-up before every later point, seconds"
    )]
    pub settle: f64,
    #[arg(
        long,
        default_value_t = 60.0,
        help = "Measurement time per point, seconds"
    )]
    pub duration: f64,
    #[arg(
        long,
        default_value_t = 0,
        help = "Extend each point to at least this many requests"
    )]
    pub min_samples: u64,
    #[arg(
        long,
        default_value_t = 2.0,
        help = "Wait for late replies after each point, seconds"
    )]
    pub drain: f64,
    #[arg(
        long,
        help = "Preload the live-key population of the profile before measuring"
    )]
    pub preload: bool,
    #[arg(
        long,
        default_value_t = 64,
        help = "Preload requests in flight per connection"
    )]
    pub preload_depth: usize,
    #[arg(long, help = "Exit after preloading")]
    pub preload_only: bool,
    #[arg(
        long,
        requires = "preload_only",
        help = "With --preload-only: send EXISTS for every preload key instead of writing it; misses are the absent keys"
    )]
    pub preload_check: bool,
    #[arg(
        long,
        help = "Start the plan at this Unix time in ms (aligns several hosts)"
    )]
    pub start_at: Option<u64>,
    #[arg(
        long,
        help = "Place a day (and every window) boundary this many seconds into the first measurement"
    )]
    pub boundary_at: Option<u64>,
    #[arg(
        long,
        default_value = "spin",
        help = "spin, or sleep:<us> to block when the next send is further away"
    )]
    pub idle: String,
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    #[arg(
        long,
        help = "Output directory for summary.json, points.csv and hdr.hlog"
    )]
    pub out: Option<PathBuf>,
    #[arg(long, default_value = "", help = "Free text recorded in the summary")]
    pub label: String,
    #[arg(
        long,
        default_value_t = 0.70,
        help = "Validity: max generator busy fraction per thread"
    )]
    pub max_util: f64,
    #[arg(
        long,
        default_value_t = 10.0,
        help = "Validity: max schedule lag p99, microseconds"
    )]
    pub max_lag_p99_us: f64,
    #[arg(
        long,
        default_value_t = 200.0,
        help = "Validity: max schedule lag p99.9, microseconds"
    )]
    pub max_lag_p999_us: f64,
    #[arg(long, help = "Drop ops the protocol cannot express instead of failing")]
    pub drop_unsupported: bool,
    #[arg(
        long,
        default_value_t = 0.0,
        help = "Log every request slower than this many microseconds (from the intended send) to outliers.jsonl; 0 disables"
    )]
    pub outlier_us: f64,
}

impl RunArgs {
    pub fn arrival(&self) -> Arrival {
        match self.arrival {
            ArrivalArg::Poisson => Arrival::Poisson,
            ArrivalArg::Constant => Arrival::Constant,
        }
    }

    pub fn io_kind(&self) -> IoKind {
        match (self.tls, self.io) {
            (TlsArg::Rpk | TlsArg::Ca, _) => IoKind::Tls,
            (TlsArg::Off, IoArg::Epoll) => IoKind::Poll,
            (TlsArg::Off, IoArg::Uring) => IoKind::Uring,
        }
    }

    pub fn tls_config(&self) -> Result<Option<Tls>, String> {
        if self.tls == TlsArg::Off {
            return match (
                &self.tls_key,
                &self.tls_servers,
                &self.tls_ca,
                &self.tls_cert,
                &self.tls_server_name,
            ) {
                (None, None, None, None, None) => Ok(None),
                _ => Err(String::from("--tls-* files need --tls rpk or --tls ca")),
            };
        }
        if self.protocol != ProtocolArg::Keep {
            return Err(String::from("--tls needs --protocol keep"));
        }
        if self.io != IoArg::Epoll {
            return Err(String::from("--tls runs on --io epoll only"));
        }
        if self.auth.is_some() {
            return Err(String::from("--tls and --auth (RESP AUTH) do not combine"));
        }
        let tls = match self.tls {
            TlsArg::Rpk => {
                if self.tls_ca.is_some()
                    || self.tls_cert.is_some()
                    || self.tls_server_name.is_some()
                {
                    return Err(String::from(
                        "--tls rpk takes --tls-key and --tls-servers only",
                    ));
                }
                let (Some(key), Some(servers)) = (&self.tls_key, &self.tls_servers) else {
                    return Err(String::from("--tls rpk needs --tls-key and --tls-servers"));
                };
                Tls::RawPublicKeys {
                    key: key.as_str().into(),
                    servers: servers.as_str().into(),
                }
            }
            TlsArg::Ca => {
                if self.tls_servers.is_some() {
                    return Err(String::from("--tls ca does not take --tls-servers"));
                }
                let Some(ca) = &self.tls_ca else {
                    return Err(String::from("--tls ca needs --tls-ca"));
                };
                let identity = match (&self.tls_cert, &self.tls_key) {
                    (Some(chain), Some(key)) => Some(Identity {
                        chain: chain.as_str().into(),
                        key: key.as_str().into(),
                    }),
                    (None, None) => None,
                    _ => return Err(String::from("--tls-cert and --tls-key go together")),
                };
                Tls::Authority {
                    ca: ca.as_str().into(),
                    server_name: self.tls_server_name.as_deref().map(Into::into),
                    identity,
                }
            }
            TlsArg::Off => return Ok(None),
        };
        Ok(Some(tls))
    }

    pub fn mapping(&self) -> RespMapping {
        match self.resp_mapping {
            MappingArg::Stalwart => RespMapping::Stalwart,
            MappingArg::Native => RespMapping::Native,
        }
    }

    pub fn routed(&self) -> bool {
        self.cluster || self.shard_collect.is_some() || self.route == RouteArg::Shards
    }

    pub fn cluster_secret(&self) -> Result<Option<ClusterSecret>, String> {
        let text = match &self.secret_file {
            Some(path) => std::fs::read_to_string(path)
                .map_err(|e| format!("--secret-file {}: {e}", path.display()))?,
            None => match std::env::var(SECRET_ENV) {
                Ok(text) => text,
                Err(_) => return Ok(None),
            },
        };
        ClusterSecret::from_hex(text.trim())
            .map(Some)
            .ok_or_else(|| String::from("the Keep cluster secret must be 32 hex digits"))
    }

    pub fn shard_map(
        &self,
        shards: u16,
        cluster: Option<&ClusterSecret>,
    ) -> Result<ShardMap, String> {
        let secret = self
            .placement_secret
            .as_ref()
            .or(cluster)
            .ok_or("shard routing needs --placement-secret, --secret-file or KEEP_SECRET")?;
        ShardMap::keep(secret, shards)
            .ok_or_else(|| String::from("the shard count must be at least 1"))
    }

    pub fn thresholds(&self) -> Thresholds {
        Thresholds {
            max_util: self.max_util,
            max_lag_p99_ns: (self.max_lag_p99_us * 1e3) as u64,
            max_lag_p999_ns: (self.max_lag_p999_us * 1e3) as u64,
        }
    }

    pub fn popularity(&self) -> Result<Option<Popularity>, String> {
        self.popularity
            .as_deref()
            .map(|spec| profile::parse_popularity("--popularity", spec).map_err(|e| e.to_string()))
            .transpose()
    }

    pub fn rates(&self) -> Result<Vec<f64>, String> {
        self.rate.iter().map(|r| parse_rate(r)).collect()
    }

    pub fn trace_options(&self) -> Result<TraceOptions, String> {
        let prefixes = self
            .trace_salt_prefixes
            .iter()
            .map(|p| u8::from_str_radix(p.trim(), 16).map_err(|_| format!("invalid prefix {p:?}")))
            .collect::<Result<Vec<u8>, String>>()?;
        Ok(TraceOptions {
            node: self.trace_node,
            salt: self.trace_salt.map(|value| Salt { value, prefixes }),
        })
    }

    pub fn cpu_list(&self) -> Result<Vec<usize>, String> {
        self.cpus.as_deref().map_or(Ok(Vec::new()), parse_cpus)
    }

    pub fn idle_mode(&self) -> Result<Idle, String> {
        match self.idle.split_once(':') {
            None if self.idle == "spin" => Ok(Idle::Spin),
            Some(("sleep", us)) => us
                .parse::<u64>()
                .map(|us| Idle::Sleep {
                    threshold_ns: us * 1_000,
                })
                .map_err(|_| format!("invalid --idle {:?}", self.idle)),
            _ => Err(format!("invalid --idle {:?}", self.idle)),
        }
    }
}

pub fn seconds(secs: f64) -> Duration {
    Duration::from_secs_f64(secs.max(0.0))
}

pub fn parse_rate(text: &str) -> Result<f64, String> {
    let text = text.trim();
    let (number, scale) = if let Some(number) = text.strip_suffix(['k', 'K']) {
        (number, 1e3)
    } else if let Some(number) = text.strip_suffix(['m', 'M']) {
        (number, 1e6)
    } else {
        (text, 1.0)
    };
    number
        .parse::<f64>()
        .ok()
        .map(|n| n * scale)
        .filter(|rate| *rate > 0.0 && rate.is_finite())
        .ok_or_else(|| format!("invalid rate {text:?}"))
}

fn parse_secret(hex: &str) -> Result<ClusterSecret, String> {
    ClusterSecret::from_hex(hex).ok_or_else(|| String::from("expected 32 hex digits"))
}

pub fn parse_cpus(text: &str) -> Result<Vec<usize>, String> {
    let mut cpus = Vec::new();
    for part in text.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let bad = || format!("invalid cpu list {text:?}");
        match part.split_once('-') {
            Some((lo, hi)) => {
                let lo: usize = lo.parse().map_err(|_| bad())?;
                let hi: usize = hi.parse().map_err(|_| bad())?;
                if lo > hi {
                    return Err(bad());
                }
                cpus.extend(lo..=hi);
            }
            None => cpus.push(part.parse().map_err(|_| bad())?),
        }
    }
    Ok(cpus)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Rates accept k and M suffixes; cpu lists accept ranges.
    #[test]
    fn parses_rates_and_cpus() {
        assert_eq!(parse_rate("250k"), Ok(250_000.0));
        assert_eq!(parse_rate("1.5M"), Ok(1_500_000.0));
        assert_eq!(parse_rate("1000"), Ok(1000.0));
        assert!(parse_rate("0").is_err());
        assert!(parse_rate("x").is_err());
        assert_eq!(parse_cpus("0-3,8"), Ok(vec![0, 1, 2, 3, 8]));
        assert!(parse_cpus("3-1").is_err());
    }

    fn run_args(extra: &[&str]) -> RunArgs {
        let argv = [
            "loadgen",
            "run",
            "--target",
            "127.0.0.1:7000",
            "--rate",
            "1000",
        ]
        .into_iter()
        .chain(extra.iter().copied());
        match Cli::try_parse_from(argv).expect("parse").command {
            Command::Run(args) => *args,
            Command::Profile { .. } => panic!("run expected"),
        }
    }

    // TLS settings parse into keep-client's Tls and refuse inconsistent combinations.
    #[test]
    fn tls_settings() {
        assert_eq!(run_args(&[]).tls_config(), Ok(None));
        assert_eq!(run_args(&[]).io_kind(), IoKind::Poll);
        let rpk = run_args(&[
            "--protocol",
            "keep",
            "--tls",
            "rpk",
            "--tls-key",
            "k",
            "--tls-servers",
            "s",
        ]);
        assert_eq!(
            rpk.tls_config(),
            Ok(Some(Tls::RawPublicKeys {
                key: "k".into(),
                servers: "s".into()
            }))
        );
        assert_eq!(rpk.io_kind(), IoKind::Tls);
        let ca = run_args(&[
            "--protocol",
            "keep",
            "--tls",
            "ca",
            "--tls-ca",
            "c",
            "--tls-cert",
            "x",
            "--tls-key",
            "k",
        ]);
        assert_eq!(
            ca.tls_config(),
            Ok(Some(Tls::Authority {
                ca: "c".into(),
                server_name: None,
                identity: Some(Identity {
                    chain: "x".into(),
                    key: "k".into()
                }),
            }))
        );
        for bad in [
            &["--tls-key", "k"][..],
            &[
                "--protocol",
                "resp",
                "--tls",
                "rpk",
                "--tls-key",
                "k",
                "--tls-servers",
                "s",
            ],
            &["--protocol", "keep", "--tls", "rpk", "--tls-key", "k"],
            &[
                "--protocol",
                "keep",
                "--tls",
                "rpk",
                "--tls-key",
                "k",
                "--tls-servers",
                "s",
                "--io",
                "uring",
            ],
            &[
                "--protocol",
                "keep",
                "--tls",
                "rpk",
                "--tls-key",
                "k",
                "--tls-servers",
                "s",
                "--tls-ca",
                "c",
            ],
            &["--protocol", "keep", "--tls", "ca"],
            &[
                "--protocol",
                "keep",
                "--tls",
                "ca",
                "--tls-ca",
                "c",
                "--tls-cert",
                "x",
            ],
            &[
                "--protocol",
                "keep",
                "--tls",
                "ca",
                "--tls-ca",
                "c",
                "--tls-servers",
                "s",
            ],
        ] {
            assert!(run_args(bad).tls_config().is_err(), "{bad:?}");
        }
    }
}
