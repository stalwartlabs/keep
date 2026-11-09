/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

pub mod keys;
pub mod profile;
pub mod stalwart;
pub mod trace;
pub mod values;

use std::collections::VecDeque;
use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use stalwart_workload::pattern::mix64;

use self::keys::{KeyPicker, KeySizes, Popularity};
use self::values::ValueSize;
use crate::op::{OpKind, Request};

const HELD_CAP: usize = 65_536;
const NS_PER_SEC: u64 = 1_000_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ttl {
    None,
    Fixed(u32),
    Window(u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Preload {
    None,
    Set,
    Incr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeySource {
    Random,
    Held,
}

#[derive(Debug)]
pub struct Keyspace {
    pub name: Box<str>,
    pub tag: u8,
    pub count: u64,
    pub key_sizes: KeySizes,
    pub popularity: Popularity,
    pub ttl: Ttl,
    pub value: ValueSize,
    pub preload: Preload,
    pub preload_count: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct OpClass {
    pub kind: OpKind,
    pub keyspace: u8,
    pub threshold: u64,
    pub share: f64,
    pub delta: i64,
    pub key_source: KeySource,
    pub batch: u8,
}

#[derive(Debug)]
pub struct OpClasses {
    head: Box<[OpClass]>,
    last: OpClass,
}

impl OpClasses {
    pub fn new(mut classes: Vec<OpClass>) -> Option<Self> {
        let mut last = classes.pop()?;
        last.threshold = u64::MAX;
        Some(OpClasses {
            head: classes.into_boxed_slice(),
            last,
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = &OpClass> {
        self.head.iter().chain(std::iter::once(&self.last))
    }

    #[inline]
    fn pick(&self, r: u64) -> OpClass {
        *self
            .head
            .iter()
            .find(|class| r < class.threshold)
            .unwrap_or(&self.last)
    }
}

#[derive(Debug)]
pub struct Workload {
    name: Box<str>,
    description: Box<str>,
    keyspaces: Box<[Keyspace]>,
    classes: OpClasses,
}

impl Workload {
    pub fn new(
        name: String,
        description: String,
        keyspaces: Box<[Keyspace]>,
        classes: OpClasses,
    ) -> Self {
        Workload {
            name: name.into_boxed_str(),
            description: description.into_boxed_str(),
            keyspaces,
            classes,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn keyspaces(&self) -> &[Keyspace] {
        &self.keyspaces
    }

    pub fn classes(&self) -> &OpClasses {
        &self.classes
    }

    pub fn max_value(&self) -> usize {
        self.keyspaces
            .iter()
            .map(|ks| ks.value.max() as usize)
            .max()
            .unwrap_or(0)
    }

    pub fn kinds(&self) -> impl Iterator<Item = OpKind> + '_ {
        self.classes.iter().map(|class| class.kind)
    }

    pub fn retain_kinds(&mut self, keep: impl Fn(OpKind) -> bool) -> Option<Vec<OpKind>> {
        let mut dropped: Vec<OpKind> = Vec::new();
        let mut kept: Vec<OpClass> = Vec::new();
        for class in self.classes.iter() {
            if keep(class.kind) {
                kept.push(*class);
            } else if !dropped.contains(&class.kind) {
                dropped.push(class.kind);
            }
        }
        let total: f64 = kept.iter().map(|class| class.share).sum();
        let mut acc = 0.0;
        for class in &mut kept {
            class.share /= total;
            acc += class.share;
            class.threshold = values::threshold(acc);
        }
        self.classes = OpClasses::new(kept)?;
        Some(dropped)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct VirtualTime {
    pub origin_s: u64,
}

impl VirtualTime {
    #[inline]
    pub fn unix_secs(&self, now_ns: u64) -> u64 {
        self.origin_s + now_ns / NS_PER_SEC
    }
}

pub struct Generator {
    workload: Arc<Workload>,
    rng: SmallRng,
    pickers: Box<[KeyPicker]>,
    held: Box<[VecDeque<u64>]>,
    key: Vec<u8>,
    batch_key: Vec<u8>,
    token: itoa::Buffer,
    token_salt: u64,
    locks: u64,
    values: Arc<[u8]>,
    time: VirtualTime,
}

impl Generator {
    pub fn new(workload: Arc<Workload>, values: Arc<[u8]>, time: VirtualTime, seed: u64) -> Self {
        let pickers = workload
            .keyspaces()
            .iter()
            .map(|ks| KeyPicker::new(ks.popularity, ks.count))
            .collect();
        let held = workload
            .keyspaces()
            .iter()
            .map(|_| VecDeque::new())
            .collect();
        Generator {
            workload,
            rng: SmallRng::seed_from_u64(seed),
            pickers,
            held,
            key: Vec::with_capacity(keys::MAX_SYNTHETIC_KEY),
            batch_key: Vec::with_capacity(keys::MAX_SYNTHETIC_KEY),
            token: itoa::Buffer::new(),
            token_salt: mix64(seed),
            locks: 0,
            values,
            time,
        }
    }

    pub fn set_time(&mut self, time: VirtualTime) {
        self.time = time;
    }

    pub fn preload_counts(&self) -> Vec<u64> {
        self.workload
            .keyspaces()
            .iter()
            .map(|ks| match ks.preload {
                Preload::None => 0,
                Preload::Set | Preload::Incr => ks.preload_count,
            })
            .collect()
    }

    pub fn next(&mut self, now_ns: u64) -> Request<'_> {
        let Generator {
            workload,
            rng,
            pickers,
            held,
            key,
            batch_key,
            token,
            token_salt,
            locks,
            values,
            time,
        } = self;
        let class = workload.classes.pick(rng.random());
        let ks_index = class.keyspace as usize;
        let ks = &workload.keyspaces()[ks_index];
        let index = pick_index(&mut held[ks_index], &pickers[ks_index], rng, class);
        let (ttl, window) = ttl_and_window(ks.ttl, time, now_ns);
        let key_size = match class.kind {
            OpKind::DeletePrefix => 1,
            _ => ks.key_sizes.size_of(index),
        };
        keys::write_key(key, ks.tag, key_size, window, index);
        for offset in 1..u64::from(class.batch) {
            keys::write_key(
                batch_key,
                ks.tag,
                key_size,
                window,
                (index + offset) % ks.count,
            );
            key.extend_from_slice(batch_key);
        }
        let (value, token): (&[u8], u64) = if class.kind.carries_token() {
            let lease = mix64(index ^ *token_salt);
            (token.format(lease).as_bytes(), lease)
        } else if class.kind.carries_value() {
            let value = &values[..ks.value.sample(rng).min(values.len())];
            match class.kind {
                OpKind::TryLock => {
                    *locks += 1;
                    (value, mix64(*locks ^ *token_salt))
                }
                _ => (value, 0),
            }
        } else {
            (&[], 0)
        };
        Request {
            kind: class.kind,
            key,
            value,
            ttl,
            delta: class.delta,
            batch: class.batch,
            token,
        }
    }

    pub fn preload(&mut self, ks_index: usize, index: u64, now_ns: u64) -> Option<Request<'_>> {
        let Generator {
            workload,
            rng,
            key,
            values,
            time,
            ..
        } = self;
        let ks = workload.keyspaces().get(ks_index)?;
        let kind = match ks.preload {
            Preload::None => return None,
            Preload::Set => OpKind::Set,
            Preload::Incr => OpKind::Incr,
        };
        let (ttl, window) = ttl_and_window(ks.ttl, time, now_ns);
        keys::write_key(key, ks.tag, ks.key_sizes.size_of(index), window, index);
        let value_len = match kind {
            OpKind::Set => ks.value.sample(rng).min(values.len()),
            _ => 0,
        };
        Some(Request {
            kind,
            key,
            value: &values[..value_len],
            ttl,
            delta: 1,
            batch: 1,
            token: 0,
        })
    }
}

#[inline]
fn pick_index(
    held: &mut VecDeque<u64>,
    picker: &KeyPicker,
    rng: &mut SmallRng,
    class: OpClass,
) -> u64 {
    match (class.kind, class.key_source) {
        (OpKind::Unlock | OpKind::Delete | OpKind::ReleaseLease, KeySource::Held) => {
            if let Some(index) = held.pop_front() {
                return index;
            }
        }
        (_, KeySource::Held) => {
            if let Some(&index) = held.back() {
                return index;
            }
        }
        _ => {}
    }
    let index = picker.pick(rng);
    if matches!(class.kind, OpKind::TryLock | OpKind::TryLease) {
        if held.len() == HELD_CAP {
            held.pop_front();
        }
        held.push_back(index);
    }
    index
}

#[inline]
fn ttl_and_window(ttl: Ttl, time: &VirtualTime, now_ns: u64) -> (u32, Option<u64>) {
    match ttl {
        Ttl::None => (0, None),
        Ttl::Fixed(secs) => (secs, None),
        Ttl::Window(period) => {
            let now_s = time.unix_secs(now_ns);
            let period = u64::from(period);
            let remaining = period - now_s % period;
            (
                u32::try_from(remaining).unwrap_or(u32::MAX),
                Some(now_s / period),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    use crate::workload::profile::{Overrides, builtin, parse};

    fn generator(profile: &str) -> Generator {
        let workload = parse(
            builtin(profile).expect("builtin"),
            &Overrides {
                popularity: None,
                keys_scale: 1.0,
            },
        )
        .expect("valid");
        let values: Arc<[u8]> = values::value_pool(workload.max_value(), 7).into();
        Generator::new(
            Arc::new(workload),
            values,
            VirtualTime {
                origin_s: 1_700_000_000,
            },
            42,
        )
    }

    // Observed op frequencies follow the profile shares.
    #[test]
    fn op_frequencies_follow_shares() {
        let mut generator = generator("stalwart-mix");
        let mut counts = [0u64; OpKind::COUNT];
        let draws = 1_000_000;
        for _ in 0..draws {
            counts[generator.next(0).kind.index()] += 1;
        }
        let share = |kind: OpKind| counts[kind.index()] as f64 / draws as f64;
        assert!((share(OpKind::Incr) - 0.70).abs() < 0.005);
        assert!((share(OpKind::TryLock) - 0.08).abs() < 0.003);
        assert!((share(OpKind::Renew) - 0.01).abs() < 0.002);
    }

    // Window TTLs end at the window boundary and keys change across it.
    #[test]
    fn window_ttl_and_rollover() {
        let time = VirtualTime { origin_s: 120 };
        assert_eq!(ttl_and_window(Ttl::Window(60), &time, 0), (60, Some(2)));
        assert_eq!(
            ttl_and_window(Ttl::Window(60), &time, 59 * NS_PER_SEC),
            (1, Some(2))
        );
        assert_eq!(
            ttl_and_window(Ttl::Window(60), &time, 60 * NS_PER_SEC),
            (60, Some(3))
        );
    }

    // Values of set ops come from the pool with the configured size.
    #[test]
    fn set_values_sized() {
        let mut generator = generator("getset");
        for _ in 0..1000 {
            let req = generator.next(0);
            match req.kind {
                OpKind::Set => assert_eq!(req.value.len(), 32),
                _ => assert!(req.value.is_empty()),
            }
            assert_eq!(req.key.len(), 16);
        }
    }

    // The footprint profile preloads 10M distinct 13 B keys with 8 B values and a 1 day TTL, and its sets refresh that TTL.
    #[test]
    fn footprint_shape() {
        let mut generator = generator("footprint");
        let mut seen = HashSet::new();
        for index in [0, 1, 9_999_998, 9_999_999] {
            let req = generator.preload(0, index, 0).expect("preloads");
            assert_eq!(req.kind, OpKind::Set);
            assert_eq!((req.key.len(), req.value.len(), req.ttl), (13, 8, 86_400));
            assert!(seen.insert(req.key.to_vec()));
        }
        assert_eq!(generator.preload_counts().first(), Some(&10_000_000));
        for _ in 0..1000 {
            let req = generator.next(0);
            match req.kind {
                OpKind::Set => assert_eq!((req.value.len(), req.ttl), (8, 86_400)),
                kind => assert_eq!(kind, OpKind::Get),
            }
            assert_eq!(req.key.len(), 13);
        }
    }
}
