/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

pub mod keep;
pub mod local;
pub mod model;
pub mod op;
pub mod preload;
#[cfg(feature = "redis")]
pub mod redis;
pub mod stats;
pub mod worker;

use crate::clock::Clock;
use keep::KeepKv;
use local::LocalKv;
use model::IncrTtl;
use std::fmt;

#[derive(Debug, Clone)]
pub enum KvTarget {
    Null,
    Local,
    #[cfg(feature = "redis")]
    Redis(redis::RedisTarget),
    Keep(Box<str>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
pub enum Mapping {
    Stalwart,
    Native,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub enum ConnShape {
    PoolPing(usize),
    Pool(usize),
    Shared(usize),
}

pub enum KvBackend {
    Null,
    Local(LocalKv),
    #[cfg(feature = "redis")]
    Redis(redis::RedisKv),
    Keep(KeepKv),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Probe {
    pub checkout_ns: u64,
}

#[derive(Debug)]
pub enum KvError {
    Unsupported(&'static str),
    Connect(Box<str>),
    #[cfg(feature = "redis")]
    Redis(::redis::RedisError),
    #[cfg(feature = "redis")]
    Pool(Box<str>),
    #[cfg(feature = "keep")]
    Keep(keep_client::Error),
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvError::Unsupported(what) => write!(f, "unsupported: {what}"),
            KvError::Connect(reason) => write!(f, "connect: {reason}"),
            #[cfg(feature = "redis")]
            KvError::Redis(err) => write!(f, "redis: {err}"),
            #[cfg(feature = "redis")]
            KvError::Pool(reason) => write!(f, "pool: {reason}"),
            #[cfg(feature = "keep")]
            KvError::Keep(err) => write!(f, "keep: {err}"),
        }
    }
}

impl KvBackend {
    pub async fn connect(
        target: &KvTarget,
        mapping: Mapping,
        shape: ConnShape,
        clock: Clock,
    ) -> Result<KvBackend, KvError> {
        let _ = (mapping, shape);
        match target {
            KvTarget::Null => Ok(KvBackend::Null),
            KvTarget::Local => Ok(KvBackend::Local(LocalKv::new(clock))),
            #[cfg(feature = "redis")]
            KvTarget::Redis(target) => redis::RedisKv::connect(target, mapping, shape)
                .await
                .map(KvBackend::Redis),
            KvTarget::Keep(url) => KeepKv::connect(url).await.map(KvBackend::Keep),
        }
    }

    pub fn client_counters(&self, out: &mut Vec<(&'static str, u64)>) {
        #[cfg(not(feature = "keep"))]
        let _ = &out;
        match self {
            #[cfg(feature = "keep")]
            KvBackend::Keep(keep) => out.extend(keep.stats().fields()),
            _ => {}
        }
    }

    pub fn incr_ttl(&self) -> Option<IncrTtl> {
        match self {
            KvBackend::Null => None,
            KvBackend::Local(_) => Some(IncrTtl::OnCreate),
            #[cfg(feature = "redis")]
            KvBackend::Redis(redis) => Some(redis.incr_ttl()),
            KvBackend::Keep(keep) => keep.incr_ttl(),
        }
    }

    pub async fn get(&self, key: &[u8], probe: &mut Probe) -> Result<Option<Vec<u8>>, KvError> {
        let _ = &probe;
        match self {
            KvBackend::Null => Ok(None),
            KvBackend::Local(local) => Ok(local.get(key)),
            #[cfg(feature = "redis")]
            KvBackend::Redis(redis) => redis.get(key, probe).await,
            KvBackend::Keep(keep) => keep.get(key).await,
        }
    }

    pub async fn exists(&self, key: &[u8], probe: &mut Probe) -> Result<bool, KvError> {
        let _ = &probe;
        match self {
            KvBackend::Null => Ok(false),
            KvBackend::Local(local) => Ok(local.exists(key)),
            #[cfg(feature = "redis")]
            KvBackend::Redis(redis) => redis.exists(key, probe).await,
            KvBackend::Keep(keep) => keep.exists(key).await,
        }
    }

    pub async fn set(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Option<u64>,
        probe: &mut Probe,
    ) -> Result<(), KvError> {
        let _ = &probe;
        match self {
            KvBackend::Null => Ok(()),
            KvBackend::Local(local) => {
                local.set(key, value, ttl);
                Ok(())
            }
            #[cfg(feature = "redis")]
            KvBackend::Redis(redis) => redis.set(key, value, ttl, probe).await,
            KvBackend::Keep(keep) => keep.set(key, value, ttl).await,
        }
    }

    pub async fn delete(&self, key: &[u8], probe: &mut Probe) -> Result<(), KvError> {
        let _ = &probe;
        match self {
            KvBackend::Null => Ok(()),
            KvBackend::Local(local) => {
                local.delete(key);
                Ok(())
            }
            #[cfg(feature = "redis")]
            KvBackend::Redis(redis) => redis.delete(key, probe).await,
            KvBackend::Keep(keep) => keep.delete(key).await,
        }
    }

    pub async fn unlock(&self, key: &[u8], probe: &mut Probe) -> Result<(), KvError> {
        match self {
            KvBackend::Keep(keep) => keep.unlock(key).await,
            _ => self.delete(key, probe).await,
        }
    }

    pub async fn incr(
        &self,
        key: &[u8],
        delta: i64,
        ttl: Option<u64>,
        probe: &mut Probe,
    ) -> Result<i64, KvError> {
        let _ = &probe;
        match self {
            KvBackend::Null => Ok(delta),
            KvBackend::Local(local) => Ok(local.incr(key, delta, ttl)),
            #[cfg(feature = "redis")]
            KvBackend::Redis(redis) => redis.incr(key, delta, ttl, probe).await,
            KvBackend::Keep(keep) => keep.incr(key, delta, ttl).await,
        }
    }

    pub async fn try_lock(&self, key: &[u8], ttl: u64, probe: &mut Probe) -> Result<bool, KvError> {
        let _ = &probe;
        match self {
            KvBackend::Null => Ok(true),
            KvBackend::Local(local) => Ok(local.try_lock(key, ttl)),
            #[cfg(feature = "redis")]
            KvBackend::Redis(redis) => redis.try_lock(key, ttl, probe).await,
            KvBackend::Keep(keep) => keep.try_lock(key, ttl).await,
        }
    }

    pub async fn renew(&self, key: &[u8], ttl: u64, probe: &mut Probe) -> Result<(), KvError> {
        let _ = &probe;
        match self {
            KvBackend::Null => Ok(()),
            KvBackend::Local(local) => {
                local.renew(key, ttl);
                Ok(())
            }
            #[cfg(feature = "redis")]
            KvBackend::Redis(redis) => redis.renew(key, ttl, probe).await,
            KvBackend::Keep(keep) => keep.renew(key, ttl).await,
        }
    }

    pub async fn try_lease(
        &self,
        key: &[u8],
        token: u64,
        ttl: u64,
        probe: &mut Probe,
    ) -> Result<bool, KvError> {
        let _ = &probe;
        match self {
            KvBackend::Null => Ok(true),
            KvBackend::Local(local) => Ok(local.try_lease(key, token, ttl)),
            #[cfg(feature = "redis")]
            KvBackend::Redis(redis) => redis.try_lease(key, token, ttl, probe).await,
            KvBackend::Keep(keep) => keep.try_lease(key, token, ttl).await,
        }
    }

    pub async fn renew_lease(
        &self,
        key: &[u8],
        token: u64,
        ttl: u64,
        probe: &mut Probe,
    ) -> Result<bool, KvError> {
        let _ = &probe;
        match self {
            KvBackend::Null => Ok(true),
            KvBackend::Local(local) => Ok(local.renew_lease(key, token, ttl)),
            #[cfg(feature = "redis")]
            KvBackend::Redis(redis) => redis.renew_lease(key, token, ttl, probe).await,
            KvBackend::Keep(keep) => keep.renew_lease(key, token, ttl).await,
        }
    }

    pub async fn release_lease(
        &self,
        key: &[u8],
        token: u64,
        probe: &mut Probe,
    ) -> Result<bool, KvError> {
        let _ = &probe;
        match self {
            KvBackend::Null => Ok(true),
            KvBackend::Local(local) => Ok(local.release_lease(key, token)),
            #[cfg(feature = "redis")]
            KvBackend::Redis(redis) => redis.release_lease(key, token, probe).await,
            KvBackend::Keep(keep) => keep.release_lease(key, token).await,
        }
    }

    pub async fn set_many(
        &self,
        items: &[(&[u8], &[u8])],
        ttl: Option<u64>,
        probe: &mut Probe,
    ) -> Result<(), KvError> {
        let _ = &probe;
        match self {
            KvBackend::Null => Ok(()),
            KvBackend::Local(local) => {
                for (key, value) in items {
                    local.set(key, value, ttl);
                }
                Ok(())
            }
            #[cfg(feature = "redis")]
            KvBackend::Redis(redis) => redis.set_many(items, ttl, probe).await,
            KvBackend::Keep(keep) => keep.set_many(items, ttl).await,
        }
    }

    pub async fn delete_prefix(&self, prefix: &[u8], probe: &mut Probe) -> Result<(), KvError> {
        let _ = &probe;
        match self {
            KvBackend::Null => Ok(()),
            KvBackend::Local(local) => {
                local.delete_prefix(prefix);
                Ok(())
            }
            #[cfg(feature = "redis")]
            KvBackend::Redis(redis) => redis.delete_prefix(prefix, probe).await,
            KvBackend::Keep(keep) => keep.delete_prefix(prefix).await,
        }
    }

    pub async fn flush(&self) -> Result<(), KvError> {
        match self {
            KvBackend::Null => Ok(()),
            KvBackend::Local(local) => {
                local.delete_prefix(&[]);
                Ok(())
            }
            #[cfg(feature = "redis")]
            KvBackend::Redis(redis) => redis.flush().await,
            KvBackend::Keep(keep) => keep.flush().await,
        }
    }
}
