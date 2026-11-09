/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use keep_proto::{Command, KeyHasher, LockToken, Status, fmix64};
use tokio::net::lookup_host;
use tokio::task::coop::consume_budget;

use crate::answer::{Answer, Expect};
use crate::config::{ClientConfig, Discovery, Report, Routing};
use crate::conn::{Conn, Flush, Gated, Opener, Openers, Pass, Reply, Signals, TlsSetup};
use crate::error::{Cause, Error};
use crate::observe::Probe;
use crate::route::{Cluster, Layout, Policy, Router, Server, Target};
use crate::stats::Stats;
use crate::stream::{Source, Streams};

pub(crate) const BACKOFF_START: Duration = Duration::from_millis(2);
pub(crate) const BACKOFF_MAX: Duration = Duration::from_millis(50);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Safety {
    Repeatable,
    Removal,
    Once,
}

/// The Keep client. Cloning shares the connections.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<Inner>,
    pub(crate) probe: Probe,
}

pub(crate) struct Inner {
    router: Router,
    hasher: Option<KeyHasher>,
    tokens: Tokens,
    pub(crate) deadline: Duration,
    pub(crate) attempt_timeout: Duration,
    pub(crate) connect_timeout: Duration,
    pub(crate) streams: Arc<Streams>,
    report: Report,
    retries: AtomicU64,
    timeouts: AtomicU64,
    fail_fast: AtomicU64,
    owner_timeouts: AtomicU64,
}

#[derive(Clone, Copy)]
struct Attempts {
    until: Option<Instant>,
    owner_wait: Option<Instant>,
    pauses: u8,
    unknown: Option<Cause>,
}

impl Attempts {
    const FIRST: Attempts = Attempts {
        until: None,
        owner_wait: None,
        pauses: 0,
        unknown: None,
    };

    fn backoff(&self) -> Duration {
        (BACKOFF_START * (1 << self.pauses)).min(BACKOFF_MAX)
    }

    fn paused(&mut self) {
        if self.backoff() < BACKOFF_MAX {
            self.pauses += 1;
        }
    }

    fn outcome(&self, error: &Error) -> Option<Error> {
        self.unknown
            .filter(|_| error.not_executed())
            .map(Error::Unknown)
    }

    #[cold]
    fn expired(&self) -> Error {
        let busy = Error::busy();
        self.outcome(&busy).unwrap_or(busy)
    }
}

enum First<'a> {
    Sent(Target<'a>, u32),
    Held,
    Unsent,
}

struct Tokens {
    key: u64,
    next: AtomicU64,
}

impl Tokens {
    fn new() -> Result<Tokens, Error> {
        let mut key = [0u8; 8];
        getrandom::fill(&mut key)
            .map_err(|_| Error::Config("no randomness for lock tokens".into()))?;
        Ok(Tokens {
            key: u64::from_le_bytes(key),
            next: AtomicU64::new(0),
        })
    }

    fn fresh(&self) -> LockToken {
        LockToken::from_u64(fmix64(self.key ^ self.next.fetch_add(1, Ordering::Relaxed)))
    }
}

async fn resolve(endpoint: &str) -> Result<SocketAddr, Error> {
    lookup_host(endpoint)
        .await
        .map_err(|err| Error::Connect(err.kind()))?
        .next()
        .ok_or_else(|| Error::Config(format!("{endpoint} does not resolve").into()))
}

fn host_of(endpoint: &str) -> &str {
    endpoint
        .rsplit_once(':')
        .map_or(endpoint, |(host, _)| host)
        .trim_start_matches('[')
        .trim_end_matches(']')
}

impl Client {
    /// Connects with the settings of a `keep://` URL.
    pub async fn connect_url(url: &str) -> Result<Client, Error> {
        Client::connect(&ClientConfig::from_url(url)?).await
    }

    /// Connects to the configured servers, or to the cluster they seed.
    pub async fn connect(config: &ClientConfig) -> Result<Client, Error> {
        config.validate()?;
        if config.discovery == Discovery::Cluster && config.secret.is_none() {
            return Err(Error::Config("cluster=on needs secret=HEX".into()));
        }
        let signals = Arc::new(Signals::new());
        let policy = Arc::new(Policy {
            selection: config.selection,
            escape: config.escape,
            attempt_timeout: config.attempt_timeout,
            keepalive: config.keepalive,
            backoff: config.backoff,
            fingerprint: config.secret.map(|secret| secret.fingerprint()),
            signals: Arc::clone(&signals),
        });
        let openers = Openers {
            base: Opener {
                rto_min: config.rto_min,
                user_timeout: config.user_timeout,
                connect_timeout: config.connect_timeout,
                tls: None,
                secret: config.secret,
            },
            tls: config.tls.as_ref().map(TlsSetup::new).transpose()?,
        };
        let deadline = Instant::now() + config.connect_timeout;
        let (router, source) = match config.discovery {
            Discovery::Static => {
                let endpoint = config
                    .endpoints
                    .first()
                    .ok_or_else(|| Error::Config("no endpoint".into()))?;
                let addr = resolve(endpoint).await?;
                let opener = openers.for_host(host_of(endpoint))?;
                let first = opener.open(addr).await?;
                let layout = match (config.routing, first.shard()) {
                    (Routing::Shard, Some(shard)) if first.info.placement == policy.fingerprint => {
                        Layout::Routed {
                            shards: shard.count(),
                            per_shard: config.shard_connections,
                        }
                    }
                    _ => Layout::Unrouted(config.connections),
                };
                let server = Server::start(
                    addr,
                    layout,
                    Arc::clone(&policy),
                    opener.clone(),
                    Some(first),
                );
                server.settle(deadline).await;
                (Router::Static(server), Source::Static { addr, opener })
            }
            Discovery::Cluster => {
                let mut seeds = Vec::with_capacity(config.endpoints.len());
                for endpoint in &config.endpoints {
                    seeds.push(resolve(endpoint).await?);
                }
                let cluster = Cluster::start(
                    seeds.into_boxed_slice(),
                    Arc::clone(&policy),
                    openers,
                    config.routing,
                    config.connections,
                    config.shard_connections,
                    deadline,
                )
                .await?;
                (
                    Router::Cluster(Arc::clone(&cluster)),
                    Source::Cluster(cluster),
                )
            }
        };
        let streams = Arc::new(Streams::new(config, source, policy)?);
        Ok(Client {
            inner: Arc::new(Inner {
                router,
                hasher: config
                    .secret
                    .filter(|_| {
                        config.routing == Routing::Shard || config.discovery == Discovery::Cluster
                    })
                    .map(|secret| secret.hasher()),
                tokens: Tokens::new()?,
                deadline: config.deadline,
                attempt_timeout: config.attempt_timeout,
                connect_timeout: config.connect_timeout,
                streams,
                report: config.report,
                retries: AtomicU64::new(0),
                timeouts: AtomicU64::new(0),
                fail_fast: AtomicU64::new(0),
                owner_timeouts: AtomicU64::new(0),
            }),
            probe: Probe::UNOBSERVED,
        })
    }

    /// The client's counters, summed over every connection.
    pub fn stats(&self) -> Stats {
        self.inner.stats()
    }

    /// Whether the client sends each key to its shard's connection (static
    /// mode: the server announced shards and the secret matched).
    pub fn is_routed(&self) -> bool {
        match &self.inner.router {
            Router::Static(server) => server.is_routable(),
            Router::Cluster(cluster) => cluster.servers().iter().all(|s| s.is_routable()),
        }
    }

    pub(crate) fn fresh_token(&self) -> LockToken {
        self.inner.tokens.fresh()
    }

    pub(crate) fn execute(
        &self,
        key: Option<&[u8]>,
        command: &Command<'_>,
        expect: Expect,
        safety: Safety,
    ) -> impl Future<Output = Result<Answer, Error>> {
        self.resume(key, command, expect, safety, Attempts::FIRST)
    }

    async fn resume(
        &self,
        key: Option<&[u8]>,
        command: &Command<'_>,
        expect: Expect,
        safety: Safety,
        mut attempts: Attempts,
    ) -> Result<Answer, Error> {
        let inner = &*self.inner;
        let hash = key
            .zip(inner.hasher.as_ref())
            .map(|(key, hasher)| hasher.hash(key));
        let mut seen = None;
        loop {
            let Some(conn) = inner.router.target(hash) else {
                inner.router.nudge();
                let failed = if inner.router.may_move(hash) {
                    let now = Instant::now();
                    let until = *attempts.until.get_or_insert(now + inner.deadline);
                    let limit = *attempts
                        .owner_wait
                        .get_or_insert(until.min(now + inner.attempt_timeout));
                    if now < limit {
                        Box::pin(inner.router.moved(limit - now)).await;
                        continue;
                    }
                    &inner.owner_timeouts
                } else {
                    &inner.fail_fast
                };
                failed.fetch_add(1, Ordering::Relaxed);
                consume_budget().await;
                return Err(attempts
                    .outcome(&Error::Unavailable)
                    .unwrap_or(Error::Unavailable));
            };
            let result = {
                let claim = match key.is_some() && conn.gate().state() == Gated::Closed {
                    false => None,
                    true => {
                        let Some(views) = seen else {
                            seen = Some(conn.views());
                            continue;
                        };
                        let until = *attempts
                            .until
                            .get_or_insert_with(|| Instant::now() + inner.deadline);
                        match conn.pass(until, views).await {
                            Pass::Probe(claim) => Some(claim),
                            Pass::Retry => {
                                seen = None;
                                continue;
                            }
                            Pass::Expired => return Err(attempts.expired()),
                        }
                    }
                };
                let sent = self.probe.stamp_at(conn.server());
                let result = conn.call(command, expect).await;
                sent.finish(result.as_ref().err());
                if let Some(claim) = claim {
                    claim.settle(&result);
                }
                result
            };
            let error = match result {
                Ok(answer) => match attempts.unknown {
                    Some(cause) if safety == Safety::Removal && answer.status != Status::Ok => {
                        return Err(Error::Unknown(cause));
                    }
                    _ => return Ok(answer),
                },
                Err(error) => error,
            };
            let gated = match key {
                Some(_) => conn.gate().state(),
                None => Gated::Open,
            };
            drop(conn);
            self.retry(&error, safety, &mut attempts, gated).await?;
        }
    }

    async fn retry(
        &self,
        error: &Error,
        safety: Safety,
        attempts: &mut Attempts,
        gated: Gated,
    ) -> Result<(), Error> {
        let inner = &*self.inner;
        if let Error::Unknown(cause) = *error {
            if cause == Cause::TimedOut {
                inner.timeouts.fetch_add(1, Ordering::Relaxed);
            }
            attempts.unknown.get_or_insert(cause);
        }
        let now = Instant::now();
        let until = *attempts.until.get_or_insert(now + inner.deadline);
        if !self.retryable(error, safety) || now >= until {
            return Err(attempts.outcome(error).unwrap_or_else(|| error.clone()));
        }
        inner.retries.fetch_add(1, Ordering::Relaxed);
        let pause = match error {
            Error::Unavailable | Error::Unknown(_) => None,
            Error::Server {
                status: Status::Busy,
                ..
            } if gated == Gated::Closed => None,
            Error::Moved(_) => {
                let wait = inner.attempt_timeout.min(until - now);
                (!Box::pin(inner.router.moved(wait)).await).then(|| attempts.backoff())
            }
            _ => Some(attempts.backoff()),
        };
        match pause {
            Some(pause) => {
                tokio::time::sleep(pause).await;
                attempts.paused();
            }
            None => consume_budget().await,
        }
        Ok(())
    }

    fn retryable(&self, error: &Error, safety: Safety) -> bool {
        match error {
            Error::Unavailable
            | Error::Server {
                status: Status::Busy,
                ..
            } => true,
            Error::Unknown(_) => safety != Safety::Once,
            Error::Moved(_) => matches!(self.inner.router, Router::Cluster(_)),
            _ => false,
        }
    }

    pub(crate) async fn execute_many(
        &self,
        requests: &[(&[u8], Command<'_>)],
        expect: Expect,
        safety: Safety,
    ) -> Vec<Result<Answer, Error>> {
        let inner = &*self.inner;
        let mut stamp = self.probe.stamp();
        let mut sent = Vec::with_capacity(requests.len());
        for (key, command) in requests {
            let hash = inner.hasher.as_ref().map(|hasher| hasher.hash(key));
            sent.push(match inner.router.target(hash) {
                Some(conn) if conn.gate().state() == Gated::Closed => First::Held,
                Some(conn) => match conn.submit(command, expect, Flush::Later) {
                    Ok(id) => First::Sent(conn, id),
                    Err(_) => First::Unsent,
                },
                None => First::Unsent,
            });
        }
        let mut corked: Vec<&Conn> = Vec::new();
        for sent in &sent {
            if let First::Sent(conn, _) = sent
                && !corked.iter().any(|seen| ptr::eq(*seen, &**conn))
            {
                corked.push(conn);
                conn.uncork();
            }
        }
        let mut replies: Vec<Option<Reply<'_>>> = sent
            .iter()
            .map(|sent| match sent {
                First::Sent(conn, id) => Some(Reply::new(conn, *id)),
                First::Held | First::Unsent => None,
            })
            .collect();
        let mut results = Vec::with_capacity(requests.len());
        let until = Instant::now() + inner.deadline;
        let mut batch = Attempts {
            until: Some(until),
            ..Attempts::FIRST
        };
        for (((key, command), reply), sent) in requests.iter().zip(replies.iter_mut()).zip(&sent) {
            let first = match (reply, sent) {
                (Some(reply), _) => reply.await,
                (None, First::Held) => Err(Error::busy()),
                (None, _) => Err(Error::Unavailable),
            };
            stamp.observe(first.as_ref().err());
            results.push(match first {
                Ok(answer) => Ok(answer),
                Err(error) => {
                    batch
                        .owner_wait
                        .get_or_insert_with(|| until.min(Instant::now() + inner.attempt_timeout));
                    let mut attempts = batch;
                    let gated = match sent {
                        First::Sent(conn, _) => conn.gate().state(),
                        First::Held => Gated::Closed,
                        First::Unsent => Gated::Open,
                    };
                    match self.retry(&error, safety, &mut attempts, gated).await {
                        Ok(()) => {
                            self.resume(Some(key), command, expect, safety, attempts)
                                .await
                        }
                        Err(error) => Err(error),
                    }
                }
            });
        }
        stamp.settle();
        results
    }

    pub(crate) async fn everywhere(&self, command: &Command<'_>) -> Result<(), Error> {
        let mut ran = false;
        for server in self.inner.router.servers() {
            let mut attempts = Attempts::FIRST;
            loop {
                let error = match server.pick(None) {
                    None => Error::Unavailable,
                    Some(conn) => {
                        let sent = self.probe.stamp();
                        let result = conn.call(command, Expect::Status).await;
                        sent.finish(result.as_ref().err());
                        match result {
                            Ok(_) => break,
                            Err(error) => error,
                        }
                    }
                };
                if let Error::Unknown(cause) = error {
                    attempts.unknown.get_or_insert(cause);
                }
                let retry = matches!(
                    error,
                    Error::Unavailable
                        | Error::Unknown(_)
                        | Error::Server {
                            status: Status::Busy,
                            ..
                        }
                );
                let now = Instant::now();
                let until = *attempts.until.get_or_insert(now + self.inner.deadline);
                if !retry || now >= until {
                    let partial = (ran && error.not_executed()).then_some(Cause::Partial);
                    return Err(attempts
                        .outcome(&error)
                        .or(partial.map(Error::Unknown))
                        .unwrap_or(error));
                }
                tokio::time::sleep(BACKOFF_MAX).await;
            }
            ran = true;
        }
        Ok(())
    }
}

#[cfg(keep_observe)]
impl Client {
    /// A handle on the same connections that records the earliest send of a request that
    /// may have taken effect, read with `take_sent` and `take_server` after every call.
    pub fn observed(&self) -> Client {
        Client {
            inner: Arc::clone(&self.inner),
            probe: Probe::fresh(),
        }
    }

    /// Returns and clears the earliest send time `observed` records; `None` when the handle
    /// is not observed or no request may have taken effect.
    pub fn take_sent(&self) -> Option<Instant> {
        self.probe.take()
    }

    /// Returns and clears the server of the last attempt that may have taken effect;
    /// `None` when the handle is not observed or no such attempt was made.
    pub fn take_server(&self) -> Option<SocketAddr> {
        self.probe.take_server()
    }
}

impl Inner {
    fn stats(&self) -> Stats {
        let mut total = self.router.stats();
        total += self.streams.stats();
        total.retries += self.retries.load(Ordering::Relaxed);
        total.timeouts += self.timeouts.load(Ordering::Relaxed);
        total.fail_fast += self.fail_fast.load(Ordering::Relaxed);
        total.owner_timeouts += self.owner_timeouts.load(Ordering::Relaxed);
        total
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.report == Report::Stderr {
            let mut line = String::from("{\"keep_client_stats\":{");
            for (index, (name, value)) in self.stats().fields().enumerate() {
                if index > 0 {
                    line.push(',');
                }
                let _ = write!(line, "\"{name}\":{value}");
            }
            line.push_str("}}");
            eprintln!("{line}");
        }
        self.streams.shutdown();
        self.router.shutdown();
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-client/client.rs"]
mod tests;
