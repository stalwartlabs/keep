/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::{ConnShape, KvError, Mapping, Probe, model::IncrTtl};
use crate::clock::duration_ns;
use deadpool::{
    Runtime,
    managed::{self, Manager, Metrics, Object, Pool, RecycleResult},
};
use futures::future::try_join_all;
use redis::{
    AsyncCommands, Client, Cmd, ErrorKind, RedisError, RedisResult, RetryMethod, Script, Value,
    aio::{ConnectionLike, MultiplexedConnection},
    cluster::{ClusterClient, ClusterClientBuilder},
    cluster_async::ClusterConnection,
    cluster_routing::{MultipleNodeRoutingInfo, RoutingInfo, SingleNodeRoutingInfo},
    from_redis_value,
};
use std::{
    io,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const POOL_TIMEOUT: Duration = Duration::from_secs(30);
const SCAN_COUNT: usize = 100;
const NATIVE_LOCK_VALUE: &[u8] = b"1";
const LEASE_PROBE_KEY: &[u8] = b"ms:lease-probe";
const LEASE_PROBE_TOKEN: &[u8] = b"0";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LeaseRenew {
    Script,
    SetIfEq,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LeaseRelease {
    Script,
    DelIfEq,
    DelexIfEq,
}

#[derive(Debug, Clone)]
pub enum RedisTarget {
    Single(Box<str>),
    Cluster(Box<[Box<str>]>),
}

#[derive(Clone, Copy, Debug)]
pub enum Recycle {
    Ping,
    Skip,
}

pub struct SingleManager {
    client: Client,
    recycle: Recycle,
}

pub struct ClusterManager {
    client: ClusterClient,
    recycle: Recycle,
}

struct Rotation<C> {
    conns: Box<[C]>,
    next: AtomicUsize,
}

enum Conns {
    Pool(Pool<SingleManager>),
    ClusterPool(Pool<ClusterManager>),
    Shared(Rotation<MultiplexedConnection>),
    ClusterShared(Rotation<ClusterConnection>),
}

pub struct RedisKv {
    conns: Conns,
    mapping: Mapping,
    incr_expire: Script,
    renew_script: Script,
    release_script: Script,
    lease_renew: LeaseRenew,
    lease_release: LeaseRelease,
}

impl Manager for SingleManager {
    type Type = MultiplexedConnection;
    type Error = RedisError;

    async fn create(&self) -> Result<MultiplexedConnection, RedisError> {
        match tokio::time::timeout(
            CONNECT_TIMEOUT,
            Box::pin(self.client.get_multiplexed_async_connection()),
        )
        .await
        {
            Ok(conn) => conn,
            Err(_) => Err(timeout_error()),
        }
    }

    async fn recycle(
        &self,
        conn: &mut MultiplexedConnection,
        _: &Metrics,
    ) -> RecycleResult<RedisError> {
        recycle(self.recycle, conn).await
    }
}

impl Manager for ClusterManager {
    type Type = ClusterConnection;
    type Error = RedisError;

    async fn create(&self) -> Result<ClusterConnection, RedisError> {
        match tokio::time::timeout(CONNECT_TIMEOUT, self.client.get_async_connection()).await {
            Ok(conn) => conn,
            Err(_) => Err(timeout_error()),
        }
    }

    async fn recycle(
        &self,
        conn: &mut ClusterConnection,
        _: &Metrics,
    ) -> RecycleResult<RedisError> {
        recycle(self.recycle, conn).await
    }
}

async fn recycle(mode: Recycle, conn: &mut impl ConnectionLike) -> RecycleResult<RedisError> {
    match mode {
        Recycle::Ping => conn
            .req_packed_command(&redis::cmd("PING"))
            .await
            .map(|_| ())
            .map_err(managed::RecycleError::Backend),
        Recycle::Skip => Ok(()),
    }
}

fn timeout_error() -> RedisError {
    RedisError::from(io::Error::new(
        io::ErrorKind::TimedOut,
        "Connection Timeout",
    ))
}

impl<C: Clone> Rotation<C> {
    fn pick(&self) -> C {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.conns.len();
        self.conns[index].clone()
    }
}

fn build_pool<M: Manager>(manager: M, size: usize) -> Result<Pool<M>, KvError> {
    Pool::builder(manager)
        .runtime(Runtime::Tokio1)
        .max_size(size)
        .create_timeout(Some(POOL_TIMEOUT))
        .wait_timeout(Some(POOL_TIMEOUT))
        .recycle_timeout(Some(POOL_TIMEOUT))
        .build()
        .map_err(|err| KvError::Connect(err.to_string().into_boxed_str()))
}

fn connect_error(err: impl std::fmt::Display) -> KvError {
    KvError::Connect(err.to_string().into_boxed_str())
}

macro_rules! with_conn {
    ($kv:expr, $probe:expr, |$conn:ident| $body:expr) => {
        match &$kv.conns {
            Conns::Pool(pool) => {
                let checkout = Instant::now();
                let mut object = pool
                    .get()
                    .await
                    .map_err(|err| KvError::Pool(err.to_string().into_boxed_str()))?;
                $probe.checkout_ns = duration_ns(checkout.elapsed());
                let $conn = &mut *object;
                let result = $body.await;
                release(object, result)
            }
            Conns::ClusterPool(pool) => {
                let checkout = Instant::now();
                let mut object = pool
                    .get()
                    .await
                    .map_err(|err| KvError::Pool(err.to_string().into_boxed_str()))?;
                $probe.checkout_ns = duration_ns(checkout.elapsed());
                let $conn = &mut *object;
                let result = $body.await;
                release(object, result)
            }
            Conns::Shared(rotation) => {
                let mut conn = rotation.pick();
                let $conn = &mut conn;
                $body.await.map_err(KvError::Redis)
            }
            Conns::ClusterShared(rotation) => {
                let mut conn = rotation.pick();
                let $conn = &mut conn;
                $body.await.map_err(KvError::Redis)
            }
        }
    };
}

fn release<M: Manager, T>(object: Object<M>, result: RedisResult<T>) -> Result<T, KvError> {
    match result {
        Ok(value) => Ok(value),
        Err(err) => {
            if is_stale_connection(&err) {
                drop(Object::take(object));
            }
            Err(KvError::Redis(err))
        }
    }
}

fn is_stale_connection(err: &RedisError) -> bool {
    matches!(
        err.retry_method(),
        RetryMethod::Reconnect
            | RetryMethod::ReconnectFromInitialConnections
            | RetryMethod::RefreshSlotsAndRetry
            | RetryMethod::MovedRedirect
            | RetryMethod::AskRedirect
    )
}

impl RedisKv {
    pub async fn connect(
        target: &RedisTarget,
        mapping: Mapping,
        shape: ConnShape,
    ) -> Result<RedisKv, KvError> {
        let recycle = match shape {
            ConnShape::PoolPing(_) => Recycle::Ping,
            ConnShape::Pool(_) | ConnShape::Shared(_) => Recycle::Skip,
        };
        let conns = match (target, shape) {
            (RedisTarget::Single(url), ConnShape::PoolPing(size) | ConnShape::Pool(size)) => {
                Conns::Pool(build_pool(
                    SingleManager {
                        client: Client::open(url.as_ref()).map_err(connect_error)?,
                        recycle,
                    },
                    size,
                )?)
            }
            (RedisTarget::Cluster(urls), ConnShape::PoolPing(size) | ConnShape::Pool(size)) => {
                Conns::ClusterPool(build_pool(
                    ClusterManager {
                        client: cluster_client(urls)?,
                        recycle,
                    },
                    size,
                )?)
            }
            (RedisTarget::Single(url), ConnShape::Shared(count)) => {
                let client = Client::open(url.as_ref()).map_err(connect_error)?;
                let mut conns = Vec::with_capacity(count);
                for _ in 0..count {
                    conns.push(
                        client
                            .get_multiplexed_async_connection()
                            .await
                            .map_err(connect_error)?,
                    );
                }
                Conns::Shared(Rotation {
                    conns: conns.into_boxed_slice(),
                    next: AtomicUsize::new(0),
                })
            }
            (RedisTarget::Cluster(urls), ConnShape::Shared(count)) => {
                let client = cluster_client(urls)?;
                let mut conns = Vec::with_capacity(count);
                for _ in 0..count {
                    conns.push(client.get_async_connection().await.map_err(connect_error)?);
                }
                Conns::ClusterShared(Rotation {
                    conns: conns.into_boxed_slice(),
                    next: AtomicUsize::new(0),
                })
            }
        };
        let mut kv = RedisKv {
            conns,
            mapping,
            incr_expire: Script::new(
                "redis.call('INCRBY', KEYS[1], ARGV[1])
                 redis.call('EXPIRE', KEYS[1], ARGV[2])
                 return redis.call('GET', KEYS[1])",
            ),
            renew_script: Script::new(
                "if redis.call('GET', KEYS[1]) == ARGV[1] then
                    return redis.call('EXPIRE', KEYS[1], ARGV[2])
                 end
                 return 0",
            ),
            release_script: Script::new(
                "if redis.call('GET', KEYS[1]) == ARGV[1] then
                    return redis.call('DEL', KEYS[1])
                 end
                 return 0",
            ),
            lease_renew: LeaseRenew::Script,
            lease_release: LeaseRelease::Script,
        };
        kv.ping(&mut Probe::default()).await?;
        if mapping == Mapping::Native {
            kv.probe_lease_commands().await?;
        }
        Ok(kv)
    }

    async fn probe_lease_commands(&mut self) -> Result<(), KvError> {
        let mut set_ifeq = redis::cmd("SET");
        set_ifeq
            .arg(LEASE_PROBE_KEY)
            .arg(LEASE_PROBE_TOKEN)
            .arg("IFEQ")
            .arg(LEASE_PROBE_TOKEN);
        let mut delifeq = redis::cmd("DELIFEQ");
        delifeq.arg(LEASE_PROBE_KEY).arg(LEASE_PROBE_TOKEN);
        let mut delex = redis::cmd("DELEX");
        delex
            .arg(LEASE_PROBE_KEY)
            .arg("IFEQ")
            .arg(LEASE_PROBE_TOKEN);
        let mut probe = Probe::default();
        if self.understands(&set_ifeq, &mut probe).await? {
            self.lease_renew = LeaseRenew::SetIfEq;
        }
        if self.understands(&delifeq, &mut probe).await? {
            self.lease_release = LeaseRelease::DelIfEq;
        } else if self.understands(&delex, &mut probe).await? {
            self.lease_release = LeaseRelease::DelexIfEq;
        }
        eprintln!(
            "redis native lease commands: renew {:?}, release {:?}",
            self.lease_renew, self.lease_release
        );
        Ok(())
    }

    async fn understands(&self, cmd: &Cmd, probe: &mut Probe) -> Result<bool, KvError> {
        match with_conn!(self, probe, |conn| cmd.query_async::<Value>(conn)) {
            Ok(_) => Ok(true),
            Err(KvError::Redis(err))
                if matches!(err.kind(), ErrorKind::Server(_) | ErrorKind::Extension) =>
            {
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }

    async fn ping(&self, probe: &mut Probe) -> Result<(), KvError> {
        with_conn!(self, probe, |conn| redis::cmd("PING")
            .query_async::<()>(conn))
    }

    pub fn incr_ttl(&self) -> IncrTtl {
        match self.mapping {
            Mapping::Stalwart => IncrTtl::Refresh,
            Mapping::Native => IncrTtl::OnCreate,
        }
    }

    pub async fn get(&self, key: &[u8], probe: &mut Probe) -> Result<Option<Vec<u8>>, KvError> {
        with_conn!(self, probe, |conn| redis::cmd("GET")
            .arg(key)
            .query_async::<Option<Vec<u8>>>(conn))
    }

    pub async fn exists(&self, key: &[u8], probe: &mut Probe) -> Result<bool, KvError> {
        with_conn!(self, probe, |conn| conn.exists::<_, bool>(key))
    }

    pub async fn set(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Option<u64>,
        probe: &mut Probe,
    ) -> Result<(), KvError> {
        with_conn!(self, probe, |conn| set(conn, key, value, ttl))
    }

    pub async fn delete(&self, key: &[u8], probe: &mut Probe) -> Result<(), KvError> {
        with_conn!(self, probe, |conn| conn.del::<_, ()>(key))
    }

    pub async fn incr(
        &self,
        key: &[u8],
        delta: i64,
        ttl: Option<u64>,
        probe: &mut Probe,
    ) -> Result<i64, KvError> {
        match (self.mapping, ttl) {
            (_, None) => with_conn!(self, probe, |conn| conn.incr::<_, _, i64>(key, delta)),
            (Mapping::Stalwart, Some(ttl)) => with_conn!(self, probe, |conn| self
                .incr_expire
                .key(key)
                .arg(delta)
                .arg(ttl as i64)
                .invoke_async::<i64>(conn)),
            (Mapping::Native, Some(ttl)) => {
                with_conn!(self, probe, |conn| native_incr(conn, key, delta, ttl))
            }
        }
    }

    pub async fn try_lock(&self, key: &[u8], ttl: u64, probe: &mut Probe) -> Result<bool, KvError> {
        match self.mapping {
            Mapping::Stalwart => with_conn!(self, probe, |conn| redis::cmd("SET")
                .arg(key)
                .arg(unix_secs() + ttl)
                .arg("NX")
                .arg("EX")
                .arg(ttl as i64)
                .query_async::<Option<String>>(conn))
            .map(|reply| reply.is_some()),
            Mapping::Native => with_conn!(self, probe, |conn| redis::cmd("SET")
                .arg(key)
                .arg(NATIVE_LOCK_VALUE)
                .arg("NX")
                .arg("EX")
                .arg(ttl)
                .query_async::<Value>(conn))
            .map(|reply| matches!(reply, Value::Okay)),
        }
    }

    pub async fn renew(&self, key: &[u8], ttl: u64, probe: &mut Probe) -> Result<(), KvError> {
        match self.mapping {
            Mapping::Stalwart => with_conn!(self, probe, |conn| redis::cmd("SET")
                .arg(key)
                .arg(unix_secs() + ttl)
                .arg("EX")
                .arg(ttl as i64)
                .query_async::<()>(conn)),
            Mapping::Native => with_conn!(self, probe, |conn| redis::cmd("SET")
                .arg(key)
                .arg(NATIVE_LOCK_VALUE)
                .arg("EX")
                .arg(ttl)
                .query_async::<()>(conn)),
        }
    }

    pub async fn try_lease(
        &self,
        key: &[u8],
        token: u64,
        ttl: u64,
        probe: &mut Probe,
    ) -> Result<bool, KvError> {
        with_conn!(self, probe, |conn| redis::cmd("SET")
            .arg(key)
            .arg(token)
            .arg("NX")
            .arg("EX")
            .arg(ttl as i64)
            .query_async::<Option<String>>(conn))
        .map(|reply| reply.is_some())
    }

    pub async fn renew_lease(
        &self,
        key: &[u8],
        token: u64,
        ttl: u64,
        probe: &mut Probe,
    ) -> Result<bool, KvError> {
        match self.lease_renew {
            LeaseRenew::Script => with_conn!(self, probe, |conn| self
                .renew_script
                .key(key)
                .arg(token)
                .arg(ttl as i64)
                .invoke_async::<i64>(conn))
            .map(|result| result > 0),
            LeaseRenew::SetIfEq => with_conn!(self, probe, |conn| redis::cmd("SET")
                .arg(key)
                .arg(token)
                .arg("IFEQ")
                .arg(token)
                .arg("EX")
                .arg(ttl as i64)
                .query_async::<Option<String>>(conn))
            .map(|reply| reply.is_some()),
        }
    }

    pub async fn release_lease(
        &self,
        key: &[u8],
        token: u64,
        probe: &mut Probe,
    ) -> Result<bool, KvError> {
        match self.lease_release {
            LeaseRelease::Script => with_conn!(self, probe, |conn| self
                .release_script
                .key(key)
                .arg(token)
                .invoke_async::<i64>(conn)),
            LeaseRelease::DelIfEq => with_conn!(self, probe, |conn| redis::cmd("DELIFEQ")
                .arg(key)
                .arg(token)
                .query_async::<i64>(conn)),
            LeaseRelease::DelexIfEq => with_conn!(self, probe, |conn| redis::cmd("DELEX")
                .arg(key)
                .arg("IFEQ")
                .arg(token)
                .query_async::<i64>(conn)),
        }
        .map(|result| result > 0)
    }

    pub async fn set_many(
        &self,
        items: &[(&[u8], &[u8])],
        ttl: Option<u64>,
        probe: &mut Probe,
    ) -> Result<(), KvError> {
        let mut pipe = redis::Pipeline::with_capacity(items.len());
        for (key, value) in items {
            pipe.cmd("SET").arg(*key).arg(*value);
            if let Some(ttl) = ttl {
                pipe.arg("EX").arg(ttl);
            }
            pipe.ignore();
        }
        with_conn!(self, probe, |conn| pipe.query_async::<()>(conn))
    }

    pub async fn delete_prefix(&self, prefix: &[u8], probe: &mut Probe) -> Result<(), KvError> {
        let pattern = scan_pattern(prefix);
        let unlink = match self.mapping {
            Mapping::Stalwart => Delete::Del,
            Mapping::Native => Delete::Unlink,
        };
        match &self.conns {
            Conns::Pool(_) | Conns::Shared(_) => {
                with_conn!(self, probe, |conn| delete_matching(conn, &pattern, unlink))
            }
            Conns::ClusterPool(pool) => {
                let mut object = pool
                    .get()
                    .await
                    .map_err(|err| KvError::Pool(err.to_string().into_boxed_str()))?;
                let result = cluster_delete_matching(&mut object, &pattern).await;
                release(object, result)
            }
            Conns::ClusterShared(rotation) => {
                let mut conn = rotation.pick();
                cluster_delete_matching(&mut conn, &pattern)
                    .await
                    .map_err(KvError::Redis)
            }
        }
    }

    pub async fn flush(&self) -> Result<(), KvError> {
        let probe = &mut Probe::default();
        match &self.conns {
            Conns::Pool(_) | Conns::Shared(_) => {
                with_conn!(self, probe, |conn| redis::cmd("FLUSHALL")
                    .query_async::<()>(conn))
            }
            Conns::ClusterPool(pool) => {
                let mut object = pool
                    .get()
                    .await
                    .map_err(|err| KvError::Pool(err.to_string().into_boxed_str()))?;
                let result = flush_cluster(&mut object).await;
                release(object, result)
            }
            Conns::ClusterShared(rotation) => flush_cluster(&mut rotation.pick())
                .await
                .map_err(KvError::Redis),
        }
    }
}

async fn flush_cluster(conn: &mut ClusterConnection) -> RedisResult<()> {
    conn.route_command(
        redis::cmd("FLUSHALL"),
        RoutingInfo::MultiNode((MultipleNodeRoutingInfo::AllMasters, None)),
    )
    .await
    .map(|_| ())
}

fn cluster_client(urls: &[Box<str>]) -> Result<ClusterClient, KvError> {
    ClusterClientBuilder::new(urls.iter().map(|url| url.as_ref()))
        .build()
        .map_err(connect_error)
}

async fn set<C: AsyncCommands>(
    conn: &mut C,
    key: &[u8],
    value: &[u8],
    ttl: Option<u64>,
) -> RedisResult<()> {
    match ttl {
        Some(ttl) => conn.set_ex(key, value, ttl).await,
        None => conn.set(key, value).await,
    }
}

async fn native_incr<C: AsyncCommands>(
    conn: &mut C,
    key: &[u8],
    delta: i64,
    ttl: u64,
) -> RedisResult<i64> {
    let (value,): (i64,) = redis::pipe()
        .cmd("INCRBY")
        .arg(key)
        .arg(delta)
        .cmd("EXPIRE")
        .arg(key)
        .arg(ttl)
        .arg("NX")
        .ignore()
        .query_async(conn)
        .await?;
    Ok(value)
}

#[derive(Clone, Copy)]
enum Delete {
    Del,
    Unlink,
}

impl Delete {
    fn command(self) -> &'static str {
        match self {
            Delete::Del => "DEL",
            Delete::Unlink => "UNLINK",
        }
    }
}

async fn delete_matching<C: AsyncCommands>(
    conn: &mut C,
    pattern: &[u8],
    delete: Delete,
) -> RedisResult<()> {
    let mut cursor = 0;
    loop {
        let (next, keys): (u64, Vec<Vec<u8>>) = scan_cmd(cursor, pattern).query_async(conn).await?;
        if !keys.is_empty() {
            redis::cmd(delete.command())
                .arg(&keys)
                .query_async::<()>(conn)
                .await?;
        }
        match next {
            0 => return Ok(()),
            next => cursor = next,
        }
    }
}

async fn cluster_delete_matching(conn: &mut ClusterConnection, pattern: &[u8]) -> RedisResult<()> {
    let Value::Map(primaries) = conn
        .route_command(
            scan_cmd(0, pattern),
            RoutingInfo::MultiNode((MultipleNodeRoutingInfo::AllMasters, None)),
        )
        .await?
    else {
        return Err(RedisError::from((
            ErrorKind::UnexpectedReturnType,
            "Unexpected reply to a SCAN sent to all cluster primaries",
        )));
    };
    try_join_all(primaries.into_iter().map(|(node, first_page)| {
        cluster_node_delete_matching(conn.clone(), pattern, node, first_page)
    }))
    .await?;
    Ok(())
}

async fn cluster_node_delete_matching(
    mut conn: ClusterConnection,
    pattern: &[u8],
    node: Value,
    first_page: Value,
) -> RedisResult<()> {
    let node = node_routing(node)?;
    let (mut cursor, mut keys): (u64, Vec<Vec<u8>>) =
        from_redis_value(first_page.extract_error()?)?;
    loop {
        if !keys.is_empty() {
            redis::cmd("UNLINK")
                .arg(&keys)
                .query_async::<()>(&mut conn)
                .await?;
        }
        if cursor == 0 {
            return Ok(());
        }
        (cursor, keys) = from_redis_value(
            conn.route_command(scan_cmd(cursor, pattern), node.clone())
                .await?
                .extract_error()?,
        )?;
    }
}

fn node_routing(node: Value) -> RedisResult<RoutingInfo> {
    let node: String = from_redis_value(node)?;
    let Some((host, port)) = node
        .rsplit_once(':')
        .and_then(|(host, port)| Some((host, port.parse().ok()?)))
    else {
        return Err(RedisError::from((
            ErrorKind::UnexpectedReturnType,
            "Invalid cluster node address",
            node,
        )));
    };
    Ok(RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
        host: host.to_string(),
        port,
    }))
}

fn scan_pattern(prefix: &[u8]) -> Vec<u8> {
    let mut pattern = Vec::with_capacity(prefix.len() * 2 + 1);
    for &byte in prefix {
        if matches!(byte, b'*' | b'?' | b'[' | b'\\') {
            pattern.push(b'\\');
        }
        pattern.push(byte);
    }
    pattern.push(b'*');
    pattern
}

fn scan_cmd(cursor: u64, pattern: &[u8]) -> Cmd {
    let mut cmd = redis::cmd("SCAN");
    cmd.cursor_arg(cursor)
        .arg("MATCH")
        .arg(pattern)
        .arg("COUNT")
        .arg(SCAN_COUNT);
    cmd
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}
