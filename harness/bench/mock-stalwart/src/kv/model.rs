/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    clock::{NANOS_PER_MILLI, NANOS_PER_SEC},
    pattern::fingerprint,
};
use std::{
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
    sync::{
        Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
};

const SHARDS: usize = 64;
const SLACK_NS: u64 = 5 * NANOS_PER_MILLI;
const MIN_SWEEP: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unseen {
    Absent,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum IncrTtl {
    Refresh,
    OnCreate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValueTag {
    len: u32,
    fingerprint: u64,
}

impl ValueTag {
    pub fn of(bytes: &[u8]) -> Self {
        ValueTag {
            len: u32::try_from(bytes.len()).unwrap_or(u32::MAX),
            fingerprint: fingerprint(bytes),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Absent,
    Counter(i64),
    Bytes(ValueTag),
    Opaque,
    Lease(u64),
    Unknown,
}

#[derive(Clone, Copy, Debug)]
enum Expiry {
    Never,
    Between { earliest: u64, latest: u64 },
    Unknown,
}

#[derive(Clone, Copy, Debug)]
enum Current {
    Absent,
    Counter(i64),
    Bytes(ValueTag),
    Opaque,
    Lease(u64),
}

#[derive(Debug)]
struct Entry {
    state: State,
    expiry: Expiry,
    inflight: u32,
    contended: bool,
}

#[derive(Default)]
struct KeyHasher(u64);

impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(*byte);
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

struct Shard {
    entries: HashMap<u64, Entry, BuildHasherDefault<KeyHasher>>,
    sweep_at: usize,
}

pub struct Model {
    shards: Box<[Mutex<Shard>]>,
    unseen: Unseen,
    incr_ttl: IncrTtl,
    epoch: AtomicU64,
    purges: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
pub struct Ticket {
    key: u64,
    epoch: u64,
    during_purge: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum Effect<'a> {
    Get(Option<&'a [u8]>),
    Exists(bool),
    Set {
        value: ValueTag,
        ttl: Option<u64>,
    },
    Delete,
    Incr {
        delta: i64,
        ttl: Option<u64>,
        result: i64,
    },
    TryLock {
        ttl: u64,
        acquired: bool,
    },
    Renew {
        ttl: u64,
    },
    TryLease {
        token: u64,
        ttl: u64,
        acquired: bool,
    },
    RenewLease {
        token: u64,
        ttl: u64,
        renewed: bool,
    },
    ReleaseLease {
        token: u64,
        released: bool,
    },
    Failed {
        mutates: bool,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    Match,
    Unchecked,
    Mismatch(String),
}

impl Entry {
    fn unseen(unseen: Unseen) -> Self {
        Entry {
            state: match unseen {
                Unseen::Absent => State::Absent,
                Unseen::Unknown => State::Unknown,
            },
            expiry: Expiry::Never,
            inflight: 0,
            contended: false,
        }
    }

    fn current(&self, start: u64, end: u64) -> Option<Current> {
        let present = match self.state {
            State::Unknown => return None,
            State::Absent => return Some(Current::Absent),
            State::Counter(value) => Current::Counter(value),
            State::Bytes(tag) => Current::Bytes(tag),
            State::Opaque => Current::Opaque,
            State::Lease(token) => Current::Lease(token),
        };
        match self.expiry {
            Expiry::Never => Some(present),
            Expiry::Between { earliest, .. } if end + SLACK_NS < earliest => Some(present),
            Expiry::Between { latest, .. } if start > latest + SLACK_NS => Some(Current::Absent),
            Expiry::Between { .. } | Expiry::Unknown => None,
        }
    }

    fn expired_by(&self, now: u64) -> bool {
        match (self.state, self.expiry) {
            (State::Absent, _) => true,
            (_, Expiry::Between { latest, .. }) => now > latest + SLACK_NS,
            _ => false,
        }
    }

    fn forget(&mut self) {
        self.state = State::Unknown;
        self.expiry = Expiry::Unknown;
    }
}

fn expiry_for(start: u64, end: u64, ttl: u64) -> Expiry {
    Expiry::Between {
        earliest: start + ttl * NANOS_PER_SEC,
        latest: end + ttl * NANOS_PER_SEC,
    }
}

impl Effect<'_> {
    fn mutates(&self) -> bool {
        match self {
            Effect::Get(_) | Effect::Exists(_) => false,
            Effect::TryLock { acquired, .. } | Effect::TryLease { acquired, .. } => *acquired,
            Effect::RenewLease { renewed, .. } => *renewed,
            Effect::ReleaseLease { released, .. } => *released,
            Effect::Failed { mutates } => *mutates,
            Effect::Set { .. } | Effect::Delete | Effect::Incr { .. } | Effect::Renew { .. } => {
                true
            }
        }
    }
}

impl Model {
    pub fn new(unseen: Unseen, incr_ttl: IncrTtl) -> Self {
        Model {
            shards: (0..SHARDS)
                .map(|_| {
                    Mutex::new(Shard {
                        entries: HashMap::default(),
                        sweep_at: MIN_SWEEP,
                    })
                })
                .collect(),
            unseen,
            incr_ttl,
            epoch: AtomicU64::new(0),
            purges: AtomicU64::new(0),
        }
    }

    fn shard(&self, key: u64) -> MutexGuard<'_, Shard> {
        self.shards[(key >> 32) as usize % SHARDS]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub fn begin(&self, key: u64) -> Ticket {
        let during_purge = self.purges.load(Ordering::Acquire) > 0;
        let epoch = self.epoch.load(Ordering::Acquire);
        let mut shard = self.shard(key);
        let entry = shard
            .entries
            .entry(key)
            .or_insert_with(|| Entry::unseen(self.unseen));
        entry.inflight += 1;
        if entry.inflight > 1 {
            entry.contended = true;
        }
        Ticket {
            key,
            epoch,
            during_purge,
        }
    }

    pub fn complete(&self, ticket: Ticket, effect: Effect<'_>, start: u64, end: u64) -> Verdict {
        let overlapped_purge = ticket.during_purge
            || self.purges.load(Ordering::Acquire) > 0
            || self.epoch.load(Ordering::Acquire) != ticket.epoch;
        let mut shard = self.shard(ticket.key);
        let verdict = match shard.entries.get_mut(&ticket.key) {
            Some(entry) => {
                entry.inflight = entry.inflight.saturating_sub(1);
                let verdict = match entry.contended || overlapped_purge {
                    true => {
                        if effect.mutates() {
                            entry.forget();
                        }
                        Verdict::Unchecked
                    }
                    false => self.apply(entry, effect, start, end),
                };
                if entry.inflight == 0 {
                    entry.contended = false;
                }
                verdict
            }
            None => Verdict::Unchecked,
        };
        if shard.entries.len() >= shard.sweep_at {
            shard.sweep(end);
        }
        verdict
    }

    pub fn purge_begin(&self) {
        self.purges.fetch_add(1, Ordering::AcqRel);
        self.forget_all();
    }

    pub fn purge_end(&self) {
        self.forget_all();
        self.purges.fetch_sub(1, Ordering::AcqRel);
    }

    fn forget_all(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        for shard in self.shards.iter() {
            let mut shard = shard.lock().unwrap_or_else(PoisonError::into_inner);
            shard.entries.values_mut().for_each(Entry::forget);
        }
    }

    pub fn entries(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .entries
                    .len()
            })
            .sum()
    }

    fn apply(&self, entry: &mut Entry, effect: Effect<'_>, start: u64, end: u64) -> Verdict {
        let current = entry.current(start, end);
        match effect {
            Effect::Get(value) => {
                let verdict = match (current, value) {
                    (None, _) => Verdict::Unchecked,
                    (Some(Current::Absent), None) => Verdict::Match,
                    (Some(Current::Bytes(tag)), Some(bytes)) if tag == ValueTag::of(bytes) => {
                        Verdict::Match
                    }
                    (Some(Current::Counter(expected)), Some(bytes))
                        if parse_counter(bytes) == Some(expected) =>
                    {
                        Verdict::Match
                    }
                    (Some(Current::Opaque), Some(_)) => Verdict::Match,
                    (Some(Current::Lease(token)), Some(bytes))
                        if bytes == token.to_le_bytes() || parse_token(bytes) == Some(token) =>
                    {
                        Verdict::Match
                    }
                    (Some(expected), got) => Verdict::Mismatch(format!(
                        "get expected {expected:?}, got {:?}",
                        got.map(<[u8]>::len)
                    )),
                };
                if current.is_none() {
                    match value {
                        None => entry.state = State::Absent,
                        Some(bytes) => entry.state = State::Bytes(ValueTag::of(bytes)),
                    }
                }
                verdict
            }
            Effect::Exists(exists) => match current {
                None => {
                    if !exists {
                        entry.state = State::Absent;
                    }
                    Verdict::Unchecked
                }
                Some(Current::Absent) if !exists => Verdict::Match,
                Some(Current::Absent) => Verdict::Mismatch("exists on an absent key".into()),
                Some(_) if exists => Verdict::Match,
                Some(expected) => Verdict::Mismatch(format!("exists false, expected {expected:?}")),
            },
            Effect::Set { value, ttl } => {
                entry.state = State::Bytes(value);
                entry.expiry = match ttl {
                    Some(ttl) => expiry_for(start, end, ttl),
                    None => Expiry::Never,
                };
                Verdict::Match
            }
            Effect::Delete => {
                entry.state = State::Absent;
                entry.expiry = Expiry::Never;
                Verdict::Match
            }
            Effect::Incr { delta, ttl, result } => {
                let (verdict, created) = match current {
                    Some(Current::Absent) => (check(delta, result, "incr on absent"), true),
                    Some(Current::Counter(value)) => {
                        (check(value + delta, result, "incr on counter"), false)
                    }
                    Some(Current::Bytes(_) | Current::Opaque | Current::Lease(_)) | None => {
                        (Verdict::Unchecked, result == delta)
                    }
                };
                let had_ttl = !matches!(entry.expiry, Expiry::Never) || current.is_none();
                entry.state = State::Counter(result);
                entry.expiry = match (ttl, self.incr_ttl) {
                    (Some(ttl), IncrTtl::Refresh) => expiry_for(start, end, ttl),
                    (Some(ttl), IncrTtl::OnCreate) if created || !had_ttl => {
                        expiry_for(start, end, ttl)
                    }
                    (None, _) if created => Expiry::Never,
                    _ if current.is_none() => Expiry::Unknown,
                    _ => entry.expiry,
                };
                verdict
            }
            Effect::TryLock { ttl, acquired } => {
                let verdict = match current {
                    None => Verdict::Unchecked,
                    Some(Current::Absent) if acquired => Verdict::Match,
                    Some(Current::Absent) => Verdict::Mismatch("lock refused on a free key".into()),
                    Some(_) if !acquired => Verdict::Match,
                    Some(_) => Verdict::Mismatch("lock granted on a held key".into()),
                };
                if acquired {
                    entry.state = State::Opaque;
                    entry.expiry = expiry_for(start, end, ttl);
                }
                verdict
            }
            Effect::Renew { ttl } => {
                entry.state = State::Opaque;
                entry.expiry = expiry_for(start, end, ttl);
                Verdict::Match
            }
            Effect::TryLease {
                token,
                ttl,
                acquired,
            } => {
                let verdict = match current {
                    None => Verdict::Unchecked,
                    Some(Current::Absent) if acquired => Verdict::Match,
                    Some(Current::Absent) => {
                        Verdict::Mismatch("lease refused on a free key".into())
                    }
                    Some(Current::Lease(held)) if held == token => Verdict::Unchecked,
                    Some(_) if !acquired => Verdict::Match,
                    Some(_) => Verdict::Mismatch("lease granted on a held key".into()),
                };
                if acquired {
                    entry.state = State::Lease(token);
                    entry.expiry = expiry_for(start, end, ttl);
                }
                verdict
            }
            Effect::RenewLease {
                token,
                ttl,
                renewed,
            } => {
                let verdict = fenced(current, token, renewed, "renew_lease");
                if renewed {
                    entry.state = State::Lease(token);
                    entry.expiry = expiry_for(start, end, ttl);
                }
                verdict
            }
            Effect::ReleaseLease { token, released } => {
                let verdict = fenced(current, token, released, "release_lease");
                if released {
                    entry.state = State::Absent;
                    entry.expiry = Expiry::Never;
                }
                verdict
            }
            Effect::Failed { mutates } => {
                if mutates {
                    entry.forget();
                }
                Verdict::Unchecked
            }
        }
    }
}

impl Shard {
    fn sweep(&mut self, now: u64) {
        self.entries
            .retain(|_, entry| entry.inflight > 0 || !entry.expired_by(now));
        self.sweep_at = (self.entries.len() * 2).max(MIN_SWEEP);
    }
}

fn check(expected: i64, got: i64, what: &str) -> Verdict {
    match expected == got {
        true => Verdict::Match,
        false => Verdict::Mismatch(format!("{what}: expected {expected}, got {got}")),
    }
}

fn fenced(current: Option<Current>, token: u64, applied: bool, what: &str) -> Verdict {
    let expected = match current {
        Some(Current::Absent) => false,
        Some(Current::Lease(held)) => held == token,
        _ => return Verdict::Unchecked,
    };
    match expected == applied {
        true => Verdict::Match,
        false => Verdict::Mismatch(format!("{what}: expected {expected}, got {applied}")),
    }
}

fn parse_token(bytes: &[u8]) -> Option<u64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn parse_counter(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::{Effect, IncrTtl, Model, Unseen, ValueTag, Verdict};
    use crate::clock::NANOS_PER_SEC;

    // Sequential ops on one key are checked exactly, including expiry.
    #[test]
    fn sequential_semantics() {
        let model = Model::new(Unseen::Absent, IncrTtl::OnCreate);
        let mut at = 1_000 * NANOS_PER_SEC;
        let run = |effect: Effect<'_>, at: u64| {
            let ticket = model.begin(7);
            model.complete(ticket, effect, at, at + 1_000)
        };
        assert_eq!(
            run(
                Effect::Incr {
                    delta: 1,
                    ttl: Some(2),
                    result: 1
                },
                at
            ),
            Verdict::Match
        );
        at += NANOS_PER_SEC / 2;
        assert_eq!(
            run(
                Effect::Incr {
                    delta: 1,
                    ttl: Some(2),
                    result: 2
                },
                at
            ),
            Verdict::Match
        );
        assert!(matches!(
            run(
                Effect::Incr {
                    delta: 1,
                    ttl: Some(2),
                    result: 7
                },
                at
            ),
            Verdict::Mismatch(_)
        ));
        at += 3 * NANOS_PER_SEC;
        assert_eq!(run(Effect::Get(None), at), Verdict::Match);
        let value = ValueTag::of(b"hello");
        assert_eq!(run(Effect::Set { value, ttl: None }, at), Verdict::Match);
        assert_eq!(run(Effect::Get(Some(b"hello")), at), Verdict::Match);
        assert!(matches!(
            run(Effect::Get(Some(b"world")), at),
            Verdict::Mismatch(_)
        ));
        assert_eq!(
            run(
                Effect::TryLock {
                    ttl: 5,
                    acquired: false
                },
                at
            ),
            Verdict::Match
        );
        assert_eq!(run(Effect::Delete, at), Verdict::Match);
        assert_eq!(
            run(
                Effect::TryLock {
                    ttl: 5,
                    acquired: true
                },
                at
            ),
            Verdict::Match
        );
        assert!(matches!(
            run(
                Effect::TryLock {
                    ttl: 5,
                    acquired: true
                },
                at
            ),
            Verdict::Mismatch(_)
        ));
    }

    // Lease renew and release succeed only for the holder's token.
    #[test]
    fn lease_fencing() {
        let model = Model::new(Unseen::Absent, IncrTtl::Refresh);
        let at = 1_000 * NANOS_PER_SEC;
        let run = |effect: Effect<'_>| {
            let ticket = model.begin(11);
            model.complete(ticket, effect, at, at + 1_000)
        };
        let lease = |token, acquired| Effect::TryLease {
            token,
            ttl: 300,
            acquired,
        };
        let renew = |token, renewed| Effect::RenewLease {
            token,
            ttl: 300,
            renewed,
        };
        let release = |token, released| Effect::ReleaseLease { token, released };
        assert_eq!(run(renew(7, false)), Verdict::Match);
        assert!(matches!(run(release(7, true)), Verdict::Mismatch(_)));
        assert_eq!(run(lease(7, true)), Verdict::Match);
        assert_eq!(run(lease(8, false)), Verdict::Match);
        assert_eq!(run(Effect::Get(Some(b"7"))), Verdict::Match);
        assert_eq!(run(renew(8, false)), Verdict::Match);
        assert_eq!(run(renew(7, true)), Verdict::Match);
        assert_eq!(run(release(8, false)), Verdict::Match);
        assert_eq!(run(release(7, true)), Verdict::Match);
        assert_eq!(run(Effect::Get(None)), Verdict::Match);
        assert!(matches!(run(renew(8, true)), Verdict::Mismatch(_)));
    }

    // Overlapping ops on the same key are never checked and make the state unknown.
    #[test]
    fn concurrent_ops_are_unchecked() {
        let model = Model::new(Unseen::Absent, IncrTtl::Refresh);
        let first = model.begin(9);
        let second = model.begin(9);
        let effect = Effect::Incr {
            delta: 1,
            ttl: Some(60),
            result: 2,
        };
        assert_eq!(model.complete(first, effect, 0, 10), Verdict::Unchecked);
        assert_eq!(model.complete(second, effect, 0, 10), Verdict::Unchecked);
        let third = model.begin(9);
        let effect = Effect::Incr {
            delta: 1,
            ttl: Some(60),
            result: 3,
        };
        assert_eq!(model.complete(third, effect, 20, 30), Verdict::Unchecked);
        let fourth = model.begin(9);
        let effect = Effect::Incr {
            delta: 1,
            ttl: Some(60),
            result: 4,
        };
        assert_eq!(model.complete(fourth, effect, 40, 50), Verdict::Match);
    }

    // Ops that start or finish while a non-atomic prefix delete runs are never checked.
    #[test]
    fn ops_during_purge_are_unchecked() {
        let model = Model::new(Unseen::Absent, IncrTtl::OnCreate);
        let value = ValueTag::of(b"v");
        model.purge_begin();
        let set = model.begin(5);
        assert_eq!(
            model.complete(set, Effect::Set { value, ttl: None }, 0, 10),
            Verdict::Unchecked
        );
        let get = model.begin(5);
        assert_eq!(
            model.complete(get, Effect::Get(None), 20, 30),
            Verdict::Unchecked
        );
        let straddling = model.begin(5);
        model.purge_end();
        assert_eq!(
            model.complete(straddling, Effect::Get(None), 40, 50),
            Verdict::Unchecked
        );
        let set = model.begin(5);
        assert_eq!(
            model.complete(set, Effect::Set { value, ttl: None }, 60, 70),
            Verdict::Match
        );
        let get = model.begin(5);
        assert_eq!(
            model.complete(get, Effect::Get(Some(b"v")), 80, 90),
            Verdict::Match
        );
    }

    // Unknown keys are resynchronised from the first solo observation.
    #[test]
    fn unknown_keys_resync() {
        let model = Model::new(Unseen::Unknown, IncrTtl::Refresh);
        let ticket = model.begin(3);
        let effect = Effect::Incr {
            delta: 1,
            ttl: Some(60),
            result: 41,
        };
        assert_eq!(model.complete(ticket, effect, 0, 10), Verdict::Unchecked);
        let ticket = model.begin(3);
        let effect = Effect::Incr {
            delta: 1,
            ttl: Some(60),
            result: 42,
        };
        assert_eq!(model.complete(ticket, effect, 20, 30), Verdict::Match);
    }
}
