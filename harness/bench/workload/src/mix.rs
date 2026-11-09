/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    NANOS_PER_MILLI, NANOS_PER_SEC, WorkloadError,
    deployment::{CountSpec, Deployment},
    dist::{Dist, DistSpec, Popularity},
    pattern::{feistel32, fill, mix64},
};
use rand::{RngExt, rngs::SmallRng};
use serde::Deserialize;
use std::num::NonZeroU64;

const MIN_ID_LEN: u64 = 4;
const MAX_ID_LEN: u64 = 254;
const NODE_PREFIX_LEN: u8 = 2;
const SHORT_ID_INDEX_BITS: u32 = 23;
const LONG_ID_INDEX_BITS: u32 = 40;
pub const MAX_BATCH: u64 = 64;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileSpec {
    #[serde(default)]
    pub demand: Option<DemandSpec>,
    #[serde(rename = "family")]
    pub families: Vec<FamilySpec>,
    #[serde(rename = "action")]
    pub actions: Vec<ActionSpec>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DemandSpec {
    pub typical_ops: f64,
    pub peak_ops: f64,
    pub events: String,
    pub typical_events: f64,
    pub peak_events: f64,
    pub source: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FamilySpec {
    pub name: String,
    pub prefix: u8,
    pub id_len: DistSpec,
    #[serde(default)]
    pub window: Option<u64>,
    #[serde(default)]
    pub ttl: Option<u64>,
    pub population: CountSpec,
    #[serde(default = "default_popularity")]
    pub popularity: String,
    #[serde(default)]
    pub value: Option<DistSpec>,
    #[serde(default)]
    pub live: CountSpec,
    #[serde(default)]
    pub purgeable: bool,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ActionKindSpec {
    Rate,
    Counter,
    Get,
    Exists,
    Set,
    Delete,
    CheckSet,
    Lock,
    Lease,
    Token,
    Purge,
    SetMany,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionSpec {
    pub kind: ActionKindSpec,
    pub family: String,
    pub weight: u32,
    #[serde(default)]
    pub hold_ms: Option<DistSpec>,
    #[serde(default)]
    pub renews: u8,
    #[serde(default)]
    pub redeem_ms: Option<DistSpec>,
    #[serde(default)]
    pub batch: Option<DistSpec>,
}

fn default_popularity() -> String {
    "uniform".to_string()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyScope {
    Node,
    Cluster,
}

#[derive(Clone, Copy, Debug)]
pub enum Expiry {
    Never,
    Fixed(u64),
    Window(NonZeroU64),
}

#[derive(Clone, Copy, Debug)]
pub enum IdLayout {
    Hashed,
    NodePrefixed,
}

#[derive(Clone, Copy, Debug)]
pub struct IdLen {
    min: u8,
    max: u8,
}

#[derive(Debug)]
pub struct Family {
    pub name: Box<str>,
    pub prefix: u8,
    pub id_len: IdLen,
    pub expiry: Expiry,
    pub population: u64,
    pub popularity: Popularity,
    pub value: Dist,
    pub live: u64,
    pub layout: IdLayout,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreloadOp {
    Incr,
    Set,
}

#[derive(Clone, Copy, Debug)]
pub enum ActionKind {
    Rate,
    Counter,
    Get,
    Exists,
    Set,
    Delete,
    CheckSet,
    Lock { hold: Dist, renews: u8 },
    Lease { hold: Dist, renews: u8 },
    Token { redeem: Dist },
    Purge,
    SetMany { batch: Dist },
}

impl ActionKind {
    pub fn ops(self) -> f64 {
        match self {
            ActionKind::Lock { renews, .. } | ActionKind::Lease { renews, .. } => {
                2.0 + f64::from(renews)
            }
            ActionKind::Token { .. } => 3.0,
            ActionKind::CheckSet => 1.5,
            ActionKind::Rate
            | ActionKind::Counter
            | ActionKind::Get
            | ActionKind::Exists
            | ActionKind::Set
            | ActionKind::Delete
            | ActionKind::Purge
            | ActionKind::SetMany { .. } => 1.0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Action {
    pub kind: ActionKind,
    pub family: u16,
}

#[derive(Debug)]
pub struct Mix {
    pub families: Box<[Family]>,
    pub actions: Box<[Action]>,
    cumulative: Box<[u32]>,
}

#[derive(Clone, Copy, Debug)]
pub struct KeyRef {
    pub index: u64,
    pub window: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct KeyCodec {
    scope: KeyScope,
    salt: u64,
    node: u16,
}

impl Mix {
    pub fn from_spec(
        spec: ProfileSpec,
        scope: KeyScope,
        deployment: &Deployment,
    ) -> Result<Mix, WorkloadError> {
        let families = spec
            .families
            .into_iter()
            .map(|family| Family::from_spec(family, scope, deployment))
            .collect::<Result<Box<[_]>, _>>()?;
        let mut actions = Vec::with_capacity(spec.actions.len());
        let mut cumulative = Vec::with_capacity(spec.actions.len());
        let mut total = 0u32;
        for action in spec.actions {
            if action.weight == 0 {
                continue;
            }
            let family = families
                .iter()
                .position(|family| *family.name == *action.family)
                .ok_or_else(|| WorkloadError::invalid(&action.family, "unknown family"))?;
            let kind = action.resolve(&families[family])?;
            total = total
                .checked_add(action.weight)
                .ok_or_else(|| WorkloadError::invalid(&action.family, "weights overflow"))?;
            actions.push(Action {
                kind,
                family: u16::try_from(family)
                    .map_err(|_| WorkloadError::invalid(&action.family, "too many families"))?,
            });
            cumulative.push(total);
        }
        if actions.is_empty() {
            return Err(WorkloadError::invalid("profile", "no action has a weight"));
        }
        let mut used = vec![false; families.len()];
        for action in &actions {
            if let Some(used) = used.get_mut(usize::from(action.family)) {
                *used = true;
            }
        }
        let mut remap = vec![u16::MAX; families.len()];
        for (index, slot) in remap
            .iter_mut()
            .zip(&used)
            .filter_map(|(slot, &used)| used.then_some(slot))
            .enumerate()
        {
            *slot = u16::try_from(index).unwrap_or(u16::MAX);
        }
        let families: Box<[Family]> = families
            .into_vec()
            .into_iter()
            .zip(used)
            .filter_map(|(family, used)| used.then_some(family))
            .collect();
        for action in &mut actions {
            action.family = remap
                .get(usize::from(action.family))
                .copied()
                .unwrap_or(u16::MAX);
        }
        Ok(Mix {
            families,
            actions: actions.into_boxed_slice(),
            cumulative: cumulative.into_boxed_slice(),
        })
    }

    #[inline]
    pub fn pick(&self, rng: &mut SmallRng) -> u16 {
        let total = self.cumulative.last().copied().unwrap_or(1);
        let target = rng.random_range(0..total);
        let index = self.cumulative.partition_point(|&bound| bound <= target);
        index as u16
    }

    pub fn ops_per_action(&self) -> f64 {
        let total: u64 = self.cumulative.last().map_or(0, |&t| u64::from(t));
        let ops: f64 = self
            .weighted_actions()
            .map(|(action, weight)| f64::from(weight) * action.kind.ops())
            .sum();
        ops / total.max(1) as f64
    }

    pub fn live_keys(&self) -> u64 {
        self.families.iter().map(|family| family.live).sum()
    }

    pub fn weighted_actions(&self) -> impl Iterator<Item = (&Action, u32)> + '_ {
        let starts = std::iter::once(0).chain(self.cumulative.iter().copied());
        self.actions
            .iter()
            .zip(self.cumulative.iter().zip(starts))
            .map(|(action, (&end, start))| (action, end - start))
    }

    pub fn action(&self, index: u16) -> Option<(&Action, &Family)> {
        let action = self.actions.get(usize::from(index))?;
        Some((action, self.families.get(usize::from(action.family))?))
    }
}

impl ActionSpec {
    fn resolve(&self, family: &Family) -> Result<ActionKind, WorkloadError> {
        let needs_ttl = |kind: ActionKind| match family.expiry {
            Expiry::Never => Err(WorkloadError::invalid(&family.name, "action needs a ttl")),
            _ => Ok(kind),
        };
        match self.kind {
            ActionKindSpec::Rate => match family.expiry {
                Expiry::Window(_) => Ok(ActionKind::Rate),
                _ => Err(WorkloadError::invalid(&family.name, "rate needs a window")),
            },
            ActionKindSpec::Counter => Ok(ActionKind::Counter),
            ActionKindSpec::Get => Ok(ActionKind::Get),
            ActionKindSpec::Exists => Ok(ActionKind::Exists),
            ActionKindSpec::Set => Ok(ActionKind::Set),
            ActionKindSpec::Delete => Ok(ActionKind::Delete),
            ActionKindSpec::CheckSet => Ok(ActionKind::CheckSet),
            ActionKindSpec::Purge => match family.layout {
                IdLayout::NodePrefixed => Ok(ActionKind::Purge),
                IdLayout::Hashed => Err(WorkloadError::invalid(
                    &family.name,
                    "purge needs a purgeable family",
                )),
            },
            ActionKindSpec::Lock => needs_ttl(ActionKind::Lock {
                hold: scaled(self.hold_ms, NANOS_PER_MILLI, &family.name)?,
                renews: self.renews,
            }),
            ActionKindSpec::Lease => needs_ttl(ActionKind::Lease {
                hold: scaled(self.hold_ms, NANOS_PER_MILLI, &family.name)?,
                renews: self.renews,
            }),
            ActionKindSpec::SetMany => {
                let batch = Dist::try_from(self.batch.unwrap_or(DistSpec::Fixed(1)))
                    .map_err(|err| WorkloadError::invalid(&family.name, &err.to_string()))?;
                match batch {
                    Dist::Fixed(1..=MAX_BATCH) => Ok(ActionKind::SetMany { batch }),
                    Dist::Uniform { min, max } if min >= 1 && max <= MAX_BATCH => {
                        Ok(ActionKind::SetMany { batch })
                    }
                    _ => Err(WorkloadError::invalid(
                        &family.name,
                        "batch is fixed or uniform within 1 to 64",
                    )),
                }
            }
            ActionKindSpec::Token => needs_ttl(ActionKind::Token {
                redeem: scaled(self.redeem_ms, NANOS_PER_MILLI, &family.name)?,
            }),
        }
    }
}

fn scaled(spec: Option<DistSpec>, unit: u64, name: &str) -> Result<Dist, WorkloadError> {
    let dist = Dist::try_from(spec.unwrap_or(DistSpec::Fixed(0)))
        .map_err(|err| WorkloadError::invalid(name, &err.to_string()))?;
    Ok(match dist {
        Dist::Fixed(value) => Dist::Fixed(value * unit),
        Dist::Uniform { min, max } => Dist::Uniform {
            min: min * unit,
            max: max * unit,
        },
        Dist::LogUniform { ln_min, ln_max } => {
            let ln_unit = (unit as f64).ln();
            Dist::LogUniform {
                ln_min: ln_min + ln_unit,
                ln_max: ln_max + ln_unit,
            }
        }
        Dist::Exp { mean } => Dist::Exp {
            mean: mean * unit as f64,
        },
    })
}

impl Family {
    pub fn preload_op(&self) -> PreloadOp {
        match self.expiry {
            Expiry::Window(_) => PreloadOp::Incr,
            Expiry::Fixed(_) | Expiry::Never => PreloadOp::Set,
        }
    }

    fn from_spec(
        spec: FamilySpec,
        scope: KeyScope,
        deployment: &Deployment,
    ) -> Result<Family, WorkloadError> {
        let name = spec.name.as_str();
        let population = spec.population.resolve(deployment);
        let live = spec.live.resolve(deployment);
        let id_len = Dist::try_from(spec.id_len)
            .map_err(|err| WorkloadError::invalid(name, &err.to_string()))?;
        let (min, max) = match id_len {
            Dist::Fixed(len) => (len, len),
            Dist::Uniform { min, max } => (min, max),
            _ => return Err(WorkloadError::invalid(name, "id_len is fixed or uniform")),
        };
        let layout = match spec.purgeable {
            true => IdLayout::NodePrefixed,
            false => IdLayout::Hashed,
        };
        let floor = match layout {
            IdLayout::NodePrefixed => MIN_ID_LEN + u64::from(NODE_PREFIX_LEN),
            IdLayout::Hashed => MIN_ID_LEN,
        };
        if min < floor || max > MAX_ID_LEN {
            return Err(WorkloadError::invalid(name, "id_len out of range"));
        }
        let shortest_unique = min
            - match layout {
                IdLayout::NodePrefixed => u64::from(NODE_PREFIX_LEN),
                IdLayout::Hashed => 0,
            };
        let index_bits = match (shortest_unique < 8, scope) {
            (true, KeyScope::Node) => SHORT_ID_INDEX_BITS,
            (true, KeyScope::Cluster) => 32,
            (false, _) => LONG_ID_INDEX_BITS,
        };
        if population == 0 || population > 1u64 << index_bits {
            return Err(WorkloadError::invalid(
                name,
                "population is zero or too large for the id length",
            ));
        }
        let expiry = match (spec.window, spec.ttl) {
            (Some(period), None) => NonZeroU64::new(period)
                .map(Expiry::Window)
                .ok_or_else(|| WorkloadError::invalid(name, "window is zero"))?,
            (None, Some(ttl)) if ttl > 0 => Expiry::Fixed(ttl),
            (None, None) => Expiry::Never,
            _ => return Err(WorkloadError::invalid(name, "set window or ttl, not both")),
        };
        Ok(Family {
            popularity: Popularity::parse(&spec.popularity, population)
                .map_err(|err| WorkloadError::invalid(name, &err.to_string()))?,
            value: Dist::try_from(spec.value.unwrap_or(DistSpec::Fixed(0)))
                .map_err(|err| WorkloadError::invalid(name, &err.to_string()))?,
            name: spec.name.into_boxed_str(),
            prefix: spec.prefix,
            id_len: IdLen {
                min: min as u8,
                max: max as u8,
            },
            expiry,
            population,
            live: live.min(population),
            layout,
        })
    }

    #[inline]
    pub fn sample_index(&self, rng: &mut SmallRng) -> u64 {
        self.popularity.sample(rng, self.population)
    }

    pub fn key_ref(&self, index: u64, now_ns: u64) -> KeyRef {
        KeyRef {
            index,
            window: match self.expiry {
                Expiry::Window(period) => now_ns / NANOS_PER_SEC / period.get(),
                _ => 0,
            },
        }
    }

    pub fn neighbour(&self, key: KeyRef, offset: u64) -> KeyRef {
        KeyRef {
            index: (key.index + offset) % self.population,
            window: key.window,
        }
    }

    pub fn ttl_at(&self, key: KeyRef, now_ns: u64) -> Option<u64> {
        match self.expiry {
            Expiry::Never => None,
            Expiry::Fixed(ttl) => Some(ttl),
            Expiry::Window(period) => {
                let end = (key.window + 1) * period.get();
                Some(end.saturating_sub(now_ns / NANOS_PER_SEC).max(1))
            }
        }
    }

    pub fn write_key(&self, codec: &KeyCodec, key: KeyRef, out: &mut Vec<u8>) {
        out.clear();
        out.push(self.prefix);
        let len = self.id_len.of(key.index ^ codec.salt);
        let unique_len = match self.layout {
            IdLayout::NodePrefixed => {
                out.extend_from_slice(&codec.purge_tag().to_be_bytes());
                len - usize::from(NODE_PREFIX_LEN)
            }
            IdLayout::Hashed => len,
        };
        codec.write_unique(key.index, unique_len, out);
        if let Expiry::Window(_) = self.expiry {
            out.extend_from_slice(&key.window.to_be_bytes());
        }
    }

    pub fn write_purge_prefix(&self, codec: &KeyCodec, out: &mut Vec<u8>) {
        out.clear();
        out.push(self.prefix);
        out.extend_from_slice(&codec.purge_tag().to_be_bytes());
    }
}

impl IdLen {
    pub fn min(&self) -> u8 {
        self.min
    }

    pub fn max(&self) -> u8 {
        self.max
    }

    fn of(&self, seed: u64) -> usize {
        let span = u64::from(self.max - self.min) + 1;
        usize::from(self.min) + (mix64(seed) % span) as usize
    }
}

impl KeyCodec {
    pub fn new(scope: KeyScope, salt: u64, node: u16) -> Self {
        KeyCodec { scope, salt, node }
    }

    fn tag(&self) -> u64 {
        match self.scope {
            KeyScope::Node => u64::from(self.node) + 1,
            KeyScope::Cluster => 0,
        }
    }

    fn purge_tag(&self) -> u16 {
        match self.scope {
            KeyScope::Node => self.node,
            KeyScope::Cluster => 0,
        }
    }

    fn write_unique(&self, index: u64, len: usize, out: &mut Vec<u8>) {
        if len >= 8 {
            let unique = mix64(((self.tag() << LONG_ID_INDEX_BITS) | index) ^ self.salt);
            out.extend_from_slice(&unique.to_be_bytes());
            fill(out, unique, len - 8);
        } else {
            let tagged = match self.scope {
                KeyScope::Node => ((self.tag() << SHORT_ID_INDEX_BITS) | index) as u32,
                KeyScope::Cluster => index as u32,
            };
            let unique = feistel32(tagged, self.salt);
            out.extend_from_slice(&unique.to_be_bytes());
            fill(out, u64::from(unique), len - 4);
        }
    }

    pub fn scope_trace_key(&self, key: &mut [u8]) {
        let Some((class, rest)) = key.split_first_mut() else {
            return;
        };
        if self.scope == KeyScope::Cluster && is_shared_class(*class) {
            return;
        }
        let salt = mix64(self.salt ^ (u64::from(self.node) + 1)).to_le_bytes();
        rest.iter_mut()
            .zip(salt)
            .for_each(|(byte, salt)| *byte ^= salt);
    }
}

fn is_shared_class(class: u8) -> bool {
    matches!(class, 0x02..=0x0a | 0x0c | 0x0d) || (class >= 0x20 && class != b'{')
}

#[cfg(test)]
mod tests {
    use super::{KeyCodec, KeyRef, KeyScope, Mix, ProfileSpec};
    use std::collections::HashSet;

    fn mix(toml: &str, scope: KeyScope) -> Mix {
        Mix::from_spec(
            toml::from_str::<ProfileSpec>(toml).expect("toml"),
            scope,
            &crate::deployment::LAB,
        )
        .expect("mix")
    }

    const PROFILE: &str = r#"
        [[family]]
        name = "short"
        prefix = 8
        id_len = 4
        window = 60
        population = 100000

        [[family]]
        name = "long"
        prefix = 6
        id_len = { dist = "uniform", min = 20, max = 40 }
        ttl = 600
        population = 100000
        purgeable = true

        [[action]]
        kind = "rate"
        family = "short"
        weight = 3

        [[action]]
        kind = "lock"
        family = "long"
        weight = 1
        hold_ms = 5
    "#;

    // Keys of different nodes never collide in node scope, and lengths follow the family.
    #[test]
    fn node_scoped_keys_are_unique() {
        let mix = mix(PROFILE, KeyScope::Node);
        let mut seen = HashSet::new();
        let mut key = Vec::new();
        for node in [1u16, 2, 511] {
            let codec = KeyCodec::new(KeyScope::Node, 0xfeed, node);
            for family in mix.families.iter() {
                for index in 0..20_000 {
                    family.write_key(&codec, KeyRef { index, window: 7 }, &mut key);
                    assert_eq!(key[0], family.prefix);
                    assert!(seen.insert(key.clone()), "duplicate key");
                }
            }
        }
        let codec = KeyCodec::new(KeyScope::Node, 0xfeed, 3);
        mix.families[0].write_key(
            &codec,
            KeyRef {
                index: 1,
                window: 7,
            },
            &mut key,
        );
        assert_eq!(key.len(), 13);
    }

    // Per-node classes are always salted per node, shared limiter classes only in node scope.
    #[test]
    fn trace_keys_follow_the_fan_out_rule() {
        let original = [0x0e_u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let limiter = [0x08_u8, 1, 2, 3, 4];
        for (scope, salted_limiter) in [(KeyScope::Node, true), (KeyScope::Cluster, false)] {
            let first = KeyCodec::new(scope, 5, 1);
            let second = KeyCodec::new(scope, 5, 2);
            let (mut a, mut b) = (original, original);
            first.scope_trace_key(&mut a);
            second.scope_trace_key(&mut b);
            assert_ne!(a, b);
            assert!(a[0] == 0x0e && a[9..] == original[9..]);
            let mut c = limiter;
            first.scope_trace_key(&mut c);
            assert_eq!(c != limiter, salted_limiter);
        }
    }

    // The action picker follows the configured weights.
    #[test]
    fn pick_follows_weights() {
        use rand::{SeedableRng, rngs::SmallRng};
        let mix = mix(PROFILE, KeyScope::Cluster);
        let mut rng = SmallRng::seed_from_u64(1);
        let mut counts = [0u32; 2];
        for _ in 0..40_000 {
            counts[usize::from(mix.pick(&mut rng))] += 1;
        }
        let ratio = f64::from(counts[0]) / f64::from(counts[1]);
        assert!((2.7..3.3).contains(&ratio), "{ratio}");
    }
}
