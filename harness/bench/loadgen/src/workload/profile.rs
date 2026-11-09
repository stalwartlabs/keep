/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::borrow::Cow;
use std::fmt;

use serde::Deserialize;

use super::keys::{self, KeySizes, MAX_SYNTHETIC_KEY, Popularity};
use super::values::{ValueSize, threshold};
use super::{KeySource, Keyspace, OpClass, OpClasses, Preload, Ttl, Workload};
use crate::op::OpKind;

const STALWART_MIX: &str = include_str!("../../profiles/stalwart-mix.toml");
pub const STALWART_PREFIX: &str = "stalwart:";
const GETSET: &str = include_str!("../../profiles/getset.toml");
const FOOTPRINT: &str = include_str!("../../profiles/footprint.toml");
const MAX_KEYSPACES: usize = keys::ALPHABET.len();

#[derive(Debug)]
pub struct ProfileError(String);

impl fmt::Display for ProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ProfileError {}

impl ProfileError {
    pub fn new(msg: impl Into<String>) -> Self {
        ProfileError(msg.into())
    }
}

fn err<T>(msg: impl Into<String>) -> Result<T, ProfileError> {
    Err(ProfileError(msg.into()))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProfile {
    name: String,
    #[serde(default)]
    description: String,
    keyspace: Vec<RawKeyspace>,
    op: Vec<RawOp>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawKeyspace {
    name: String,
    key_size: RawSizes,
    keys: u64,
    #[serde(default)]
    popularity: Option<String>,
    #[serde(default)]
    ttl: Option<String>,
    #[serde(default)]
    value_size: Option<RawSizes>,
    #[serde(default)]
    preload: Option<String>,
    #[serde(default)]
    preload_keys: Option<u64>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawSizes {
    Fixed(u32),
    Weighted(Vec<(u32, f64)>),
    Spec(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOp {
    kind: String,
    keyspace: String,
    weight: f64,
    #[serde(default)]
    delta: Option<i64>,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    batch: Option<u8>,
}

pub fn builtin(name: &str) -> Option<&'static str> {
    hashify::map!(
        name.as_bytes(),
        &'static str,
        "stalwart-mix" => STALWART_MIX,
        "getset" => GETSET,
        "footprint" => FOOTPRINT
    )
    .copied()
}

pub fn builtin_text(name: &str) -> Option<Result<Cow<'static, str>, ProfileError>> {
    match name.strip_prefix(STALWART_PREFIX) {
        Some(profile) => Some(super::stalwart::render(profile).map(Cow::Owned)),
        None => builtin(name).map(|text| Ok(Cow::Borrowed(text))),
    }
}

pub struct Overrides {
    pub popularity: Option<Popularity>,
    pub keys_scale: f64,
}

pub fn parse(text: &str, overrides: &Overrides) -> Result<Workload, ProfileError> {
    let raw: RawProfile =
        toml::from_str(text).map_err(|e| ProfileError(format!("invalid profile: {e}")))?;
    if raw.keyspace.len() > MAX_KEYSPACES {
        return err(format!("at most {MAX_KEYSPACES} keyspaces are supported"));
    }
    let keyspaces = raw
        .keyspace
        .iter()
        .zip(keys::ALPHABET)
        .map(|(ks, &tag)| keyspace(ks, tag, overrides))
        .collect::<Result<Box<[_]>, _>>()?;
    let classes = classes(&raw.op, &raw.keyspace)?;
    Ok(Workload::new(raw.name, raw.description, keyspaces, classes))
}

fn keyspace(raw: &RawKeyspace, tag: u8, overrides: &Overrides) -> Result<Keyspace, ProfileError> {
    let name = raw.name.as_str();
    let count = ((raw.keys as f64) * overrides.keys_scale).round().max(1.0) as u64;
    let ttl = match raw.ttl.as_deref() {
        None | Some("none") => Ttl::None,
        Some(spec) => match spec.strip_prefix("window:") {
            Some(period) => Ttl::Window(parse_num(name, "ttl window", period)?),
            None => Ttl::Fixed(parse_num(name, "ttl", spec)?),
        },
    };
    if matches!(ttl, Ttl::Window(0)) {
        return err(format!("keyspace {name}: window must be at least 1 s"));
    }
    let windowed = matches!(ttl, Ttl::Window(_));
    let sizes: Vec<(u8, f64)> = match &raw.key_size {
        RawSizes::Fixed(size) => vec![(key_len(name, *size)?, 1.0)],
        RawSizes::Weighted(list) => list
            .iter()
            .map(|&(size, weight)| Ok((key_len(name, size)?, weight)))
            .collect::<Result<_, ProfileError>>()?,
        RawSizes::Spec(spec) => {
            return err(format!("keyspace {name}: unsupported key_size {spec:?}"));
        }
    };
    if sizes.is_empty() || sizes.iter().any(|&(_, w)| !positive(w)) {
        return err(format!(
            "keyspace {name}: key_size weights must be positive"
        ));
    }
    let key_sizes = KeySizes::new(count, &sizes);
    if let Some(size) = key_sizes
        .sizes()
        .find(|&size| keys::capacity(keys::index_chars(size as usize, windowed)) < count)
    {
        return err(format!(
            "keyspace {name}: {count} keys do not fit in {size} byte keys{}",
            if windowed { " with a window index" } else { "" }
        ));
    }
    let popularity = match overrides.popularity {
        Some(popularity) => popularity,
        None => parse_popularity(name, raw.popularity.as_deref().unwrap_or("uniform"))?,
    };
    let value = match &raw.value_size {
        None => ValueSize::Fixed(0),
        Some(RawSizes::Fixed(size)) => ValueSize::Fixed(*size),
        Some(RawSizes::Weighted(list)) => {
            if list.is_empty() || list.iter().any(|&(_, w)| !positive(w)) {
                return err(format!(
                    "keyspace {name}: value_size weights must be positive"
                ));
            }
            ValueSize::choice(list)
        }
        Some(RawSizes::Spec(spec)) => parse_log_uniform(name, spec)?,
    };
    let preload = match raw.preload.as_deref() {
        None => Preload::None,
        Some(spec) => hashify::map!(
            spec.as_bytes(),
            Preload,
            "none" => Preload::None,
            "set" => Preload::Set,
            "incr" => Preload::Incr,
        )
        .copied()
        .ok_or_else(|| ProfileError(format!("keyspace {name}: unknown preload {spec:?}")))?,
    };
    let preload_count = raw
        .preload_keys
        .map_or(count, |keys| {
            ((keys as f64) * overrides.keys_scale).round().max(1.0) as u64
        })
        .min(count);
    Ok(Keyspace {
        name: name.into(),
        tag,
        count,
        key_sizes,
        popularity,
        ttl,
        value,
        preload,
        preload_count,
    })
}

const MAX_BATCH: u8 = 64;

fn classes(raw: &[RawOp], keyspaces: &[RawKeyspace]) -> Result<OpClasses, ProfileError> {
    let total: f64 = raw.iter().map(|op| op.weight).sum();
    if raw.is_empty()
        || !positive(total)
        || raw
            .iter()
            .any(|op| !(positive(op.weight) || op.weight == 0.0))
    {
        return err("op weights must be non-negative with a positive sum");
    }
    let mut acc = 0.0;
    let classes = raw
        .iter()
        .filter(|op| op.weight > 0.0)
        .map(|op| {
            let kind = OpKind::parse(op.kind.as_bytes())
                .ok_or_else(|| ProfileError(format!("unknown op kind {:?}", op.kind)))?;
            let keyspace = keyspaces
                .iter()
                .position(|ks| ks.name == op.keyspace)
                .ok_or_else(|| ProfileError(format!("unknown keyspace {:?}", op.keyspace)))?;
            let key_source = match op.key.as_deref() {
                None => KeySource::Random,
                Some(spec) => hashify::map!(
                    spec.as_bytes(),
                    KeySource,
                    "random" => KeySource::Random,
                    "held" => KeySource::Held,
                )
                .copied()
                .ok_or_else(|| ProfileError(format!("unknown key source {spec:?}")))?,
            };
            let batch = match (kind, op.batch) {
                (OpKind::SetMany, Some(batch @ 1..=MAX_BATCH)) => batch,
                (OpKind::SetMany, None) => 1,
                (OpKind::SetMany, Some(batch)) => {
                    return err(format!("set_many batch {batch} outside 1..={MAX_BATCH}"));
                }
                (_, None) => 1,
                (_, Some(_)) => {
                    return err(format!("batch is only valid for set_many, not {}", op.kind));
                }
            };
            acc += op.weight;
            Ok(OpClass {
                batch,
                kind,
                keyspace: u8::try_from(keyspace).unwrap_or(u8::MAX),
                threshold: threshold(acc / total),
                share: op.weight / total,
                delta: op.delta.unwrap_or(1),
                key_source,
            })
        })
        .collect::<Result<Vec<_>, ProfileError>>()?;
    OpClasses::new(classes).ok_or_else(|| ProfileError(String::from("no op has a positive weight")))
}

fn positive(weight: f64) -> bool {
    weight.is_finite() && weight > 0.0
}

fn key_len(keyspace: &str, size: u32) -> Result<u8, ProfileError> {
    match u8::try_from(size) {
        Ok(size) if (2..=MAX_SYNTHETIC_KEY as u32).contains(&u32::from(size)) => Ok(size),
        _ => err(format!(
            "keyspace {keyspace}: key size {size} outside 2..={MAX_SYNTHETIC_KEY}"
        )),
    }
}

fn parse_num<T: std::str::FromStr>(keyspace: &str, what: &str, s: &str) -> Result<T, ProfileError> {
    s.trim()
        .parse()
        .map_err(|_| ProfileError(format!("keyspace {keyspace}: invalid {what} {s:?}")))
}

pub fn parse_popularity(keyspace: &str, spec: &str) -> Result<Popularity, ProfileError> {
    match spec.split_once(':') {
        None if spec == "uniform" => Ok(Popularity::Uniform),
        Some(("zipf", s)) => {
            let s: f64 = parse_num(keyspace, "zipf exponent", s)?;
            if s > 0.0 {
                Ok(Popularity::Zipf(s))
            } else {
                err(format!(
                    "keyspace {keyspace}: zipf exponent must be positive"
                ))
            }
        }
        _ => err(format!("keyspace {keyspace}: unknown popularity {spec:?}")),
    }
}

fn parse_log_uniform(keyspace: &str, spec: &str) -> Result<ValueSize, ProfileError> {
    let mut parts = spec.splitn(3, ':');
    match (parts.next(), parts.next(), parts.next()) {
        (Some("log-uniform"), Some(min), Some(max)) => {
            let min: u32 = parse_num(keyspace, "value_size", min)?;
            let max: u32 = parse_num(keyspace, "value_size", max)?;
            if min == 0 || min > max {
                return err(format!(
                    "keyspace {keyspace}: log-uniform needs 0 < min <= max"
                ));
            }
            Ok(ValueSize::log_uniform(min, max))
        }
        _ => err(format!("keyspace {keyspace}: unknown value_size {spec:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_overrides() -> Overrides {
        Overrides {
            popularity: None,
            keys_scale: 1.0,
        }
    }

    // Built-in profiles parse and their op shares sum to one.
    #[test]
    fn builtins_parse() {
        for name in ["stalwart-mix", "getset", "footprint"] {
            let text = builtin(name).expect("builtin exists");
            let workload = parse(text, &no_overrides()).expect("valid profile");
            let total: f64 = workload.classes().iter().map(|c| c.share).sum();
            assert!((total - 1.0).abs() < 1e-9, "{name}: {total}");
        }
    }

    // The stalwart-mix shares match its documented op mix.
    #[test]
    fn stalwart_mix_shares() {
        let workload = parse(STALWART_MIX, &no_overrides()).expect("valid profile");
        let share = |kinds: &[OpKind]| -> f64 {
            workload
                .classes()
                .iter()
                .filter(|c| kinds.contains(&c.kind))
                .map(|c| c.share)
                .sum()
        };
        let close = |a: f64, b: f64| (a - b).abs() < 1e-6;
        assert!(close(share(&[OpKind::Incr]), 0.70));
        assert!(close(share(&[OpKind::Get, OpKind::Exists]), 0.10));
        assert!(close(share(&[OpKind::TryLock]), 0.08));
        assert!(close(share(&[OpKind::Unlock, OpKind::Delete]), 0.08));
        assert!(close(share(&[OpKind::Set]), 0.03));
        assert!(close(share(&[OpKind::Renew]), 0.01));
    }

    // Too many keys for the key size is rejected at load time.
    #[test]
    fn rejects_overfull_keyspace() {
        let text = r#"
            name = "x"
            [[keyspace]]
            name = "a"
            key_size = 6
            keys = 100000
            ttl = "window:60"
            [[op]]
            kind = "get"
            keyspace = "a"
            weight = 1
        "#;
        assert!(parse(text, &no_overrides()).is_err());
    }
}
