/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::num::{NonZeroU8, NonZeroU16};
use std::time::Duration;

use keep_proto::ClusterSecret;
use rustls::pki_types::ServerName;

use crate::error::Error;

/// Client settings. `from_url` reads them from
/// `keep://host:port[,host:port...]?name=value&...`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientConfig {
    /// The servers as `host:port`, or the cluster seeds when `discovery` is `Cluster`.
    pub endpoints: Vec<Box<str>>,
    /// Where the client learns its servers from; the URL's `cluster=`.
    pub discovery: Discovery,
    /// Connections per server under `Routing::None`; the URL's `conns=`,
    /// 16 by default.
    pub connections: usize,
    /// Connections per shard under `Routing::Shard`; the URL's
    /// `shard_conns=`, 2 by default when routing by shard and 1
    /// otherwise.
    pub shard_connections: NonZeroU8,
    /// Which unrouted connection takes a request; the URL's `select=`.
    pub selection: Selection,
    /// Whether requests go to the key's shard connection; the URL's `route=`.
    pub routing: Routing,
    /// Which servers subscriptions and publishes use; the URL's `stream=`.
    pub stream: StreamMode,
    /// The shard each stream connection is placed on; the URL's `feeds=`,
    /// random by default.
    pub feeds: FeedPlacement,
    /// The node index `feeds=spread` and `feeds=k` place by; the URL's
    /// `node=`, drawn at random per client when absent.
    pub node: Option<u64>,
    /// A connection stalled this long hands requests to a healthy one; the URL's
    /// `escape=us:N`, off by default.
    pub escape: Option<Duration>,
    /// The cluster secret, needed for shard routing and cluster discovery; the URL's `secret=`.
    pub secret: Option<ClusterSecret>,
    /// The minimum TCP retransmission timeout where the OS supports it; the URL's
    /// `rto_min=us:N`, 5 ms by default.
    pub rto_min: Option<Duration>,
    /// `TCP_USER_TIMEOUT` of every connection; the URL's `user_timeout=ms:N`, 1 s by default.
    pub user_timeout: Option<Duration>,
    /// How long `Client::connect` waits for the servers; the URL's `connect_timeout=ms:N`.
    pub connect_timeout: Duration,
    /// How long one attempt waits for its reply; the URL's `timeout=ms:N`, 1 s by default.
    pub attempt_timeout: Duration,
    /// How long a call may take over all its attempts; the URL's `deadline=ms:N`, 5 s by default.
    pub deadline: Duration,
    /// Idle time after which a connection is probed with a ping; the URL's
    /// `keepalive=ms:N`, 15 s by default.
    pub keepalive: Option<Duration>,
    /// The delay between reconnect attempts; the URL's `backoff=`.
    pub backoff: Backoff,
    /// Whether connection events are reported on stderr; the URL's `report=`.
    pub report: Report,
    /// TLS settings, or `None` for plain TCP; the URL's `tls=` and its key parameters.
    pub tls: Option<Tls>,
}

/// Where the client learns its servers from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Discovery {
    /// One server, the first endpoint: every key goes to it.
    Static,
    /// A Keep cluster: the endpoints are seeds for the `Topology` op, and keys are placed over its
    /// members.
    Cluster,
}

/// Which unrouted connection of a server takes a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Selection {
    /// The less loaded of two connections.
    Least,
    /// The connection of the calling tokio worker thread.
    Worker,
}

/// Whether requests go to the connection of the key's shard. A URL with `secret=`
/// and no `route=` routes by shard unless it names `conns=` or `select=`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Routing {
    /// `connections` per server; the server forwards misrouted keys.
    None,
    /// `shard_connections` per shard, collected by the Hello reply's shard
    /// id; needs the cluster secret.
    Shard,
}

/// Which servers a subscription and a publish use in a cluster.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamMode {
    /// Subscribe on every placing member; publish on one chosen per client.
    Everywhere,
    /// Subscribe and publish on the topic's home member.
    Owner,
}

/// Which shard of each server a stream connection is placed on, chosen
/// when it connects from the shard the Hello reply names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedPlacement {
    /// Wherever the server puts the connection.
    Random,
    /// The shard `node` mod the server's shard count.
    Spread,
    /// The shard `node` mod the smaller of k and the server's shard count.
    Concentrate(NonZeroU16),
}

/// TLS 1.3 settings. Every path names a PEM or DER file, read when the client connects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Tls {
    /// Mutual TLS with raw public keys (RFC 7250), the default mode: the
    /// client presents the public key of its private key `key` and accepts
    /// a server only if its public key is one of `servers`.
    RawPublicKeys {
        /// The client's private key file.
        key: Box<str>,
        /// The file of server public keys the client accepts.
        servers: Box<str>,
    },
    /// Certificates from a cluster CA: the server's chain must lead to `ca`
    /// and name `server_name` (the endpoint's host when absent); `identity`
    /// is the client's own chain, for servers that require one.
    Authority {
        /// The cluster CA certificate file.
        ca: Box<str>,
        /// The name the server certificate must carry.
        server_name: Option<Box<str>>,
        /// The client's own certificate chain and key.
        identity: Option<Identity>,
    },
}

/// A client certificate chain and its private key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    /// The certificate chain file.
    pub chain: Box<str>,
    /// The private key file.
    pub key: Box<str>,
}

/// How the reconnect delay is drawn from the current backoff step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backoff {
    /// The full step.
    Fixed,
    /// A random delay up to the step.
    Jitter,
    /// Half the step plus a random delay up to the other half, the default.
    Equal,
}

/// Where connection events are reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Report {
    /// Nowhere, the default.
    Silent,
    /// On standard error.
    Stderr,
}

const SCHEME: &str = "keep";
const MAX_CONNECTIONS: usize = 1024;
const DEFAULT_CONNECTIONS: usize = 16;
const MAX_SHARD_CONNECTIONS: u8 = 8;
const DEFAULT_SHARD_CONNECTIONS: NonZeroU8 = NonZeroU8::MIN.saturating_add(1);

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            endpoints: Vec::new(),
            discovery: Discovery::Static,
            connections: DEFAULT_CONNECTIONS,
            shard_connections: NonZeroU8::MIN,
            selection: Selection::Least,
            routing: Routing::None,
            stream: StreamMode::Everywhere,
            feeds: FeedPlacement::Random,
            node: None,
            escape: None,
            secret: None,
            rto_min: Some(Duration::from_millis(5)),
            user_timeout: Some(Duration::from_secs(1)),
            connect_timeout: Duration::from_secs(5),
            attempt_timeout: Duration::from_secs(1),
            deadline: Duration::from_secs(5),
            keepalive: Some(Duration::from_secs(15)),
            backoff: Backoff::Equal,
            report: Report::Silent,
            tls: None,
        }
    }
}

impl ClientConfig {
    /// Parses a `keep://` URL into a validated config.
    pub fn from_url(url: &str) -> Result<ClientConfig, Error> {
        let rest = match url.split_once("://") {
            Some((SCHEME, rest)) => rest,
            Some(_) => return Err(config_error(format!("URL scheme must be {SCHEME}://"))),
            None => url,
        };
        let (hosts, query) = rest.split_once('?').unwrap_or((rest, ""));
        let mut config = ClientConfig {
            endpoints: hosts
                .trim_end_matches('/')
                .split(',')
                .filter(|host| !host.is_empty())
                .map(Box::from)
                .collect(),
            ..ClientConfig::default()
        };
        let mut tls = TlsParams::default();
        let mut routing = None;
        let mut shard_connections = None;
        let mut unrouted_params = UnroutedParams::Absent;
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            let (name, value) = pair
                .split_once('=')
                .ok_or_else(|| config_error(format!("parameter {pair} has no value")))?;
            hashify::fnc_map!(name.as_bytes(),
                "conns" => {
                    config.connections = parse_connections(value)?;
                    unrouted_params = UnroutedParams::Named;
                },
                "shard_conns" => { shard_connections = Some(parse_shard_connections(value)?); },
                "select" => {
                    config.selection = parse_selection(value)?;
                    unrouted_params = UnroutedParams::Named;
                },
                "route" => { routing = Some(parse_routing(value)?); },
                "stream" => { config.stream = parse_stream(value)?; },
                "feeds" => { config.feeds = parse_feeds(value)?; },
                "node" => { config.node = Some(parse_node(value)?); },
                "escape" => { config.escape = parse_optional(name, value, "us:", Duration::from_micros)?; },
                "secret" => {
                    config.secret = Some(ClusterSecret::from_hex(value).ok_or_else(|| {
                        config_error("secret must be 32 hex digits".into())
                    })?);
                },
                "cluster" => { config.discovery = parse_discovery(value)?; },
                "rto_min" => { config.rto_min = parse_optional(name, value, "us:", Duration::from_micros)?; },
                "user_timeout" => { config.user_timeout = parse_optional(name, value, "ms:", Duration::from_millis)?; },
                "connect_timeout" => { config.connect_timeout = parse_duration(name, value, "ms:", Duration::from_millis)?; },
                "timeout" => { config.attempt_timeout = parse_duration(name, value, "ms:", Duration::from_millis)?; },
                "deadline" => { config.deadline = parse_duration(name, value, "ms:", Duration::from_millis)?; },
                "keepalive" => { config.keepalive = parse_optional(name, value, "ms:", Duration::from_millis)?; },
                "backoff" => { config.backoff = parse_backoff(value)?; },
                "report" => { config.report = parse_report(value)?; },
                "tls" => { tls.mode = parse_tls(value)?; },
                "key" => { tls.key = Some(value); },
                "servers" => { tls.servers = Some(value); },
                "ca" => { tls.ca = Some(value); },
                "sni" => { tls.server_name = Some(value); },
                "cert" => { tls.chain = Some(value); },
                _ => { return Err(config_error(format!("unknown parameter {name}"))); }
            );
        }
        if routing.is_none()
            && shard_connections.is_some()
            && matches!(unrouted_params, UnroutedParams::Named)
        {
            return Err(config_error(
                "shard_conns with conns or select needs route=shard or route=none".into(),
            ));
        }
        config.routing = routing.unwrap_or(match (config.secret, unrouted_params) {
            (Some(_), UnroutedParams::Absent) => Routing::Shard,
            _ => Routing::None,
        });
        config.shard_connections = match (shard_connections, config.routing) {
            (Some(count), _) => count,
            (None, Routing::Shard) => DEFAULT_SHARD_CONNECTIONS,
            (None, Routing::None) => NonZeroU8::MIN,
        };
        config.tls = tls.build()?;
        config.validate()?;
        Ok(config)
    }

    /// Checks that the settings are consistent; `Client::connect` also needs a secret with `Cluster`.
    pub fn validate(&self) -> Result<(), Error> {
        if self.endpoints.is_empty() {
            return Err(config_error("no endpoint".into()));
        }
        if !(1..=MAX_CONNECTIONS).contains(&self.connections) {
            return Err(config_error(format!("conns must be 1..={MAX_CONNECTIONS}")));
        }
        if self.shard_connections.get() > MAX_SHARD_CONNECTIONS {
            return Err(config_error(format!(
                "shard_conns must be 1..={MAX_SHARD_CONNECTIONS}"
            )));
        }
        if self.routing == Routing::Shard && self.secret.is_none() {
            return Err(config_error("route=shard needs secret=HEX".into()));
        }
        if self.routing != Routing::Shard && self.shard_connections.get() > 1 {
            return Err(config_error(
                "shard_conns needs shard routing (secret=HEX, no route=none)".into(),
            ));
        }
        if self.keepalive.is_some_and(|keepalive| keepalive.is_zero()) {
            return Err(config_error("keepalive must be positive or off".into()));
        }
        if self.attempt_timeout.is_zero() || self.deadline < self.attempt_timeout {
            return Err(config_error(
                "timeout must be positive and at most the deadline".into(),
            ));
        }
        if let Some(Tls::Authority {
            server_name: Some(name),
            ..
        }) = &self.tls
            && ServerName::try_from(&**name).is_err()
        {
            return Err(config_error(format!(
                "sni must be a DNS name or an IP address, got {name:?}"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum UnroutedParams {
    Absent,
    Named,
}

#[derive(Clone, Copy, Default)]
enum TlsMode {
    #[default]
    Off,
    RawPublicKeys,
    Authority,
}

#[derive(Default)]
struct TlsParams<'a> {
    mode: TlsMode,
    key: Option<&'a str>,
    servers: Option<&'a str>,
    ca: Option<&'a str>,
    server_name: Option<&'a str>,
    chain: Option<&'a str>,
}

impl TlsParams<'_> {
    fn build(self) -> Result<Option<Tls>, Error> {
        let path = |value: Option<&str>| value.filter(|path| !path.is_empty()).map(Box::from);
        match self.mode {
            TlsMode::Off => {
                if [
                    self.key,
                    self.servers,
                    self.ca,
                    self.server_name,
                    self.chain,
                ]
                .iter()
                .any(Option::is_some)
                {
                    return Err(config_error(
                        "key, servers, ca, sni and cert need tls=rpk or tls=ca".into(),
                    ));
                }
                Ok(None)
            }
            TlsMode::RawPublicKeys => {
                if self.ca.is_some() || self.server_name.is_some() || self.chain.is_some() {
                    return Err(config_error("tls=rpk takes key and servers only".into()));
                }
                let (Some(key), Some(servers)) = (path(self.key), path(self.servers)) else {
                    return Err(config_error(
                        "tls=rpk needs key=PATH and servers=PATH".into(),
                    ));
                };
                Ok(Some(Tls::RawPublicKeys { key, servers }))
            }
            TlsMode::Authority => {
                if self.servers.is_some() {
                    return Err(config_error("servers needs tls=rpk".into()));
                }
                let ca =
                    path(self.ca).ok_or_else(|| config_error("tls=ca needs ca=PATH".into()))?;
                let identity = match (self.chain, self.key) {
                    (None, None) => None,
                    (chain, key) => match (path(chain), path(key)) {
                        (Some(chain), Some(key)) => Some(Identity { chain, key }),
                        _ => {
                            return Err(config_error("cert=PATH and key=PATH go together".into()));
                        }
                    },
                };
                Ok(Some(Tls::Authority {
                    ca,
                    server_name: self.server_name.map(Box::from),
                    identity,
                }))
            }
        }
    }
}

fn config_error(reason: String) -> Error {
    Error::Config(reason.into_boxed_str())
}

fn parse_connections(value: &str) -> Result<usize, Error> {
    value
        .parse::<usize>()
        .ok()
        .filter(|count| (1..=MAX_CONNECTIONS).contains(count))
        .ok_or_else(|| config_error(format!("conns must be 1..={MAX_CONNECTIONS}")))
}

fn parse_shard_connections(value: &str) -> Result<NonZeroU8, Error> {
    value
        .parse::<NonZeroU8>()
        .ok()
        .filter(|count| count.get() <= MAX_SHARD_CONNECTIONS)
        .ok_or_else(|| config_error(format!("shard_conns must be 1..={MAX_SHARD_CONNECTIONS}")))
}

fn parse_selection(value: &str) -> Result<Selection, Error> {
    hashify::map!(value.as_bytes(), Selection, "least" => Selection::Least, "worker" => Selection::Worker)
        .copied()
        .ok_or_else(|| config_error(format!("select must be least or worker, got {value}")))
}

fn parse_routing(value: &str) -> Result<Routing, Error> {
    hashify::map!(value.as_bytes(), Routing, "none" => Routing::None, "shard" => Routing::Shard)
        .copied()
        .ok_or_else(|| config_error(format!("route must be none or shard, got {value}")))
}

fn parse_stream(value: &str) -> Result<StreamMode, Error> {
    hashify::map!(value.as_bytes(), StreamMode, "everywhere" => StreamMode::Everywhere, "owner" => StreamMode::Owner)
        .copied()
        .ok_or_else(|| config_error(format!("stream must be everywhere or owner, got {value}")))
}

fn parse_feeds(value: &str) -> Result<FeedPlacement, Error> {
    hashify::map!(value.as_bytes(), FeedPlacement, "random" => FeedPlacement::Random, "spread" => FeedPlacement::Spread)
        .copied()
        .or_else(|| value.parse().ok().map(FeedPlacement::Concentrate))
        .ok_or_else(|| {
            config_error(format!(
                "feeds must be random, spread or 1..={}, got {value}",
                u16::MAX
            ))
        })
}

fn parse_node(value: &str) -> Result<u64, Error> {
    value.parse().map_err(|_| {
        config_error(format!(
            "node must be an unsigned 64-bit integer, got {value}"
        ))
    })
}

fn parse_discovery(value: &str) -> Result<Discovery, Error> {
    hashify::map!(value.as_bytes(), Discovery, "off" => Discovery::Static, "on" => Discovery::Cluster)
        .copied()
        .ok_or_else(|| config_error(format!("cluster must be on or off, got {value}")))
}

fn parse_tls(value: &str) -> Result<TlsMode, Error> {
    hashify::map!(
        value.as_bytes(),
        TlsMode,
        "off" => TlsMode::Off,
        "rpk" => TlsMode::RawPublicKeys,
        "ca" => TlsMode::Authority,
    )
    .copied()
    .ok_or_else(|| config_error(format!("tls must be off, rpk or ca, got {value}")))
}

fn parse_backoff(value: &str) -> Result<Backoff, Error> {
    hashify::map!(
        value.as_bytes(),
        Backoff,
        "fixed" => Backoff::Fixed,
        "jitter" => Backoff::Jitter,
        "equal" => Backoff::Equal,
    )
    .copied()
    .ok_or_else(|| config_error(format!("unknown backoff {value}")))
}

fn parse_report(value: &str) -> Result<Report, Error> {
    hashify::map!(value.as_bytes(), Report, "silent" => Report::Silent, "stderr" => Report::Stderr)
        .copied()
        .ok_or_else(|| config_error(format!("unknown report {value}")))
}

fn parse_duration(
    name: &str,
    value: &str,
    unit: &str,
    make: fn(u64) -> Duration,
) -> Result<Duration, Error> {
    value
        .strip_prefix(unit)
        .and_then(|amount| amount.parse().ok())
        .map(make)
        .ok_or_else(|| config_error(format!("{name} must be {unit}N, got {value}")))
}

fn parse_optional(
    name: &str,
    value: &str,
    unit: &str,
    make: fn(u64) -> Duration,
) -> Result<Option<Duration>, Error> {
    match value {
        "off" => Ok(None),
        _ => parse_duration(name, value, unit, make).map(Some),
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-client/config.rs"]
mod tests;
