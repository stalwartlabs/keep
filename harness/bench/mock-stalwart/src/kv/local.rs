/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    clock::{Clock, NANOS_PER_SEC},
    pattern::fingerprint,
};
use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard, PoisonError},
};

const SHARDS: usize = 64;

type Shard = HashMap<Box<[u8]>, Entry>;

pub struct LocalKv {
    clock: Clock,
    shards: Box<[Mutex<Shard>]>,
}

struct Entry {
    value: Stored,
    expires: Option<u64>,
}

enum Stored {
    Bytes(Box<[u8]>),
    Counter(i64),
}

impl LocalKv {
    pub fn new(clock: Clock) -> Self {
        LocalKv {
            clock,
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
        }
    }

    fn shard(&self, key: &[u8]) -> MutexGuard<'_, Shard> {
        let index = (fingerprint(key) >> 32) as usize % SHARDS;
        self.shards[index]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn deadline(&self, ttl: Option<u64>) -> Option<u64> {
        ttl.map(|ttl| self.clock.now() + ttl * NANOS_PER_SEC)
    }

    fn live<'a>(map: &'a mut Shard, key: &[u8], now: u64) -> Option<&'a mut Entry> {
        if map
            .get(key)
            .is_some_and(|entry| entry.expires.is_some_and(|at| at <= now))
        {
            map.remove(key);
        }
        map.get_mut(key)
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let now = self.clock.now();
        let mut map = self.shard(key);
        Self::live(&mut map, key, now).map(|entry| match &entry.value {
            Stored::Bytes(bytes) => bytes.to_vec(),
            Stored::Counter(value) => value.to_string().into_bytes(),
        })
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        let now = self.clock.now();
        Self::live(&mut self.shard(key), key, now).is_some()
    }

    pub fn set(&self, key: &[u8], value: &[u8], ttl: Option<u64>) {
        let expires = self.deadline(ttl);
        self.shard(key).insert(
            key.into(),
            Entry {
                value: Stored::Bytes(value.into()),
                expires,
            },
        );
    }

    pub fn delete(&self, key: &[u8]) {
        self.shard(key).remove(key);
    }

    pub fn incr(&self, key: &[u8], delta: i64, ttl: Option<u64>) -> i64 {
        let now = self.clock.now();
        let mut map = self.shard(key);
        match Self::live(&mut map, key, now) {
            Some(Entry {
                value: Stored::Counter(value),
                expires,
            }) => {
                *value += delta;
                if expires.is_none() {
                    *expires = ttl.map(|ttl| now + ttl * NANOS_PER_SEC);
                }
                *value
            }
            Some(entry) => {
                entry.value = Stored::Counter(delta);
                delta
            }
            None => {
                map.insert(
                    key.into(),
                    Entry {
                        value: Stored::Counter(delta),
                        expires: ttl.map(|ttl| now + ttl * NANOS_PER_SEC),
                    },
                );
                delta
            }
        }
    }

    pub fn try_lock(&self, key: &[u8], ttl: u64) -> bool {
        let now = self.clock.now();
        let mut map = self.shard(key);
        match Self::live(&mut map, key, now) {
            Some(_) => false,
            None => {
                map.insert(
                    key.into(),
                    Entry {
                        value: Stored::Bytes(Box::default()),
                        expires: Some(now + ttl * NANOS_PER_SEC),
                    },
                );
                true
            }
        }
    }

    pub fn renew(&self, key: &[u8], ttl: u64) {
        self.set(key, &[], Some(ttl));
    }

    pub fn try_lease(&self, key: &[u8], token: u64, ttl: u64) -> bool {
        let now = self.clock.now();
        let mut map = self.shard(key);
        match Self::live(&mut map, key, now) {
            Some(_) => false,
            None => {
                map.insert(
                    key.into(),
                    Entry {
                        value: Stored::Bytes(token_bytes(token)),
                        expires: Some(now + ttl * NANOS_PER_SEC),
                    },
                );
                true
            }
        }
    }

    pub fn renew_lease(&self, key: &[u8], token: u64, ttl: u64) -> bool {
        let now = self.clock.now();
        let mut map = self.shard(key);
        match Self::live(&mut map, key, now) {
            Some(entry) if entry.holds(token) => {
                entry.expires = Some(now + ttl * NANOS_PER_SEC);
                true
            }
            _ => false,
        }
    }

    pub fn release_lease(&self, key: &[u8], token: u64) -> bool {
        let now = self.clock.now();
        let mut map = self.shard(key);
        match Self::live(&mut map, key, now) {
            Some(entry) if entry.holds(token) => {
                map.remove(key);
                true
            }
            _ => false,
        }
    }

    pub fn delete_prefix(&self, prefix: &[u8]) {
        for shard in self.shards.iter() {
            shard
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .retain(|key, _| !key.starts_with(prefix));
        }
    }
}

impl Entry {
    fn holds(&self, token: u64) -> bool {
        match &self.value {
            Stored::Bytes(bytes) => **bytes == *itoa::Buffer::new().format(token).as_bytes(),
            Stored::Counter(value) => u64::try_from(*value) == Ok(token),
        }
    }
}

fn token_bytes(token: u64) -> Box<[u8]> {
    itoa::Buffer::new().format(token).as_bytes().into()
}
