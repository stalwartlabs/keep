/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt::Write as _;

use stalwart_workload::deployment;
use stalwart_workload::dist::{Dist, Popularity};
use stalwart_workload::mix::{ActionKind, Expiry, Family, KeyScope, Mix, PreloadOp, ProfileSpec};
use stalwart_workload::profiles;

use super::keys::MAX_SYNTHETIC_KEY;
use super::profile::ProfileError;

const SIZE_POINTS: u64 = 8;
const WINDOW_BYTES: u64 = 8;
const MIN_KEY: u64 = 2;
const MIN_MISS_RATIO: f64 = 0.05;

pub fn render(name: &str) -> Result<String, ProfileError> {
    let (profile, tier) = name.split_once('@').unwrap_or((name, "lab"));
    let deployment = deployment::builtin(tier).ok_or_else(|| {
        ProfileError::new(format!(
            "unknown deployment {tier:?}; built-in: {}",
            deployment::DEPLOYMENTS.join(", ")
        ))
    })?;
    let text = profiles::builtin(profile).ok_or_else(|| {
        ProfileError::new(format!(
            "unknown Stalwart profile {profile:?}; built-in: {}",
            profiles::KV_PROFILES.join(", ")
        ))
    })?;
    let spec: ProfileSpec = toml::from_str(text)
        .map_err(|e| ProfileError::new(format!("stalwart profile {profile}: {e}")))?;
    let mix = Mix::from_spec(spec, KeyScope::Cluster, deployment)
        .map_err(|e| ProfileError::new(format!("stalwart profile {profile}: {e}")))?;
    let mut out = String::with_capacity(8 << 10);
    let _ = writeln!(out, "name = \"stalwart:{profile}@{tier}\"");
    let _ = writeln!(
        out,
        "description = \"Stalwart {profile} profile at deployment {tier} from harness/bench/workload, flattened to single ops\""
    );
    for family in mix.families.iter() {
        keyspace(&mut out, family);
    }
    for (action, weight) in mix.weighted_actions() {
        let Some(family) = mix.families.get(usize::from(action.family)) else {
            continue;
        };
        let weight = f64::from(weight);
        let name = &*family.name;
        match action.kind {
            ActionKind::Rate | ActionKind::Counter => op(&mut out, "incr", name, weight, false),
            ActionKind::Get => op(&mut out, "get", name, weight, false),
            ActionKind::Exists => op(&mut out, "exists", name, weight, false),
            ActionKind::Set => op(&mut out, "set", name, weight, false),
            ActionKind::Delete => op(&mut out, "delete", name, weight, false),
            ActionKind::Purge => op(&mut out, "delete_prefix", name, weight, false),
            ActionKind::CheckSet => {
                op(&mut out, "exists", name, weight, false);
                op(&mut out, "set", name, weight * miss_ratio(family), false);
            }
            ActionKind::SetMany { batch } => set_many(&mut out, name, weight, batch),
            ActionKind::Lease { renews, .. } => {
                op(&mut out, "try_lease", name, weight, false);
                if renews > 0 {
                    op(
                        &mut out,
                        "renew_lease",
                        name,
                        weight * f64::from(renews),
                        true,
                    );
                }
                op(&mut out, "release_lease", name, weight, true);
            }
            ActionKind::Lock { renews, .. } => {
                op(&mut out, "try_lock", name, weight, false);
                if renews > 0 {
                    op(&mut out, "renew", name, weight * f64::from(renews), true);
                }
                op(&mut out, "unlock", name, weight, true);
            }
            ActionKind::Token { .. } => {
                op(&mut out, "set", name, weight, false);
                op(&mut out, "get", name, weight, false);
                op(&mut out, "delete", name, weight, false);
            }
        }
    }
    Ok(out)
}

fn keyspace(out: &mut String, family: &Family) {
    let window = match family.expiry {
        Expiry::Window(_) => WINDOW_BYTES,
        Expiry::Fixed(_) | Expiry::Never => 0,
    };
    let key =
        |id_len: u8| (1 + u64::from(id_len) + window).clamp(MIN_KEY, MAX_SYNTHETIC_KEY as u64);
    let _ = write!(
        out,
        "\n[[keyspace]]\nname = \"{}\"\nkeys = {}\nkey_size = ",
        family.name, family.population
    );
    let (min, max) = (family.id_len.min(), family.id_len.max());
    match min == max {
        true => {
            let _ = writeln!(out, "{}", key(min));
        }
        false => {
            let span = u64::from(max - min);
            let points = (span + 1).min(SIZE_POINTS);
            out.push('[');
            for i in 0..points {
                let len = u64::from(min) + span * i / (points - 1).max(1);
                let len = u8::try_from(len).unwrap_or(max);
                let _ = write!(out, "{}[{}, 1.0]", if i > 0 { ", " } else { "" }, key(len));
            }
            out.push_str("]\n");
        }
    }
    match family.popularity {
        Popularity::Uniform => out.push_str("popularity = \"uniform\"\n"),
        Popularity::Zipf { exponent, .. } => {
            let _ = writeln!(out, "popularity = \"zipf:{exponent}\"");
        }
    }
    match family.expiry {
        Expiry::Window(period) => {
            let _ = writeln!(out, "ttl = \"window:{period}\"");
        }
        Expiry::Fixed(ttl) => {
            let _ = writeln!(out, "ttl = \"{ttl}\"");
        }
        Expiry::Never => {}
    }
    value_size(out, family.value);
    if family.live > 0 {
        let kind = match family.preload_op() {
            PreloadOp::Incr => "incr",
            PreloadOp::Set => "set",
        };
        let _ = writeln!(out, "preload = \"{kind}\"\npreload_keys = {}", family.live);
    }
}

fn value_size(out: &mut String, value: Dist) {
    match value {
        Dist::Fixed(size) => {
            let _ = writeln!(out, "value_size = {size}");
        }
        Dist::Uniform { min, max } => {
            let span = max - min;
            let points = (span + 1).min(SIZE_POINTS);
            out.push_str("value_size = [");
            for i in 0..points {
                let size = min + span * i / (points - 1).max(1);
                let _ = write!(out, "{}[{size}, 1.0]", if i > 0 { ", " } else { "" });
            }
            out.push_str("]\n");
        }
        Dist::LogUniform { ln_min, ln_max } => {
            let _ = writeln!(
                out,
                "value_size = \"log-uniform:{}:{}\"",
                ln_min.exp().round().max(1.0) as u64,
                ln_max.exp().round().max(1.0) as u64
            );
        }
        Dist::Exp { mean } => {
            out.push_str("value_size = [");
            for i in 0..SIZE_POINTS {
                let quantile = (i as f64 + 0.5) / SIZE_POINTS as f64;
                let size = (-mean * (1.0 - quantile).ln()).round() as u64;
                let _ = write!(out, "{}[{size}, 1.0]", if i > 0 { ", " } else { "" });
            }
            out.push_str("]\n");
        }
    }
}

fn op(out: &mut String, kind: &str, keyspace: &str, weight: f64, held: bool) {
    let _ = write!(
        out,
        "\n[[op]]\nkind = \"{kind}\"\nkeyspace = \"{keyspace}\"\nweight = {weight}\n"
    );
    if held {
        out.push_str("key = \"held\"\n");
    }
}

fn set_many(out: &mut String, keyspace: &str, weight: f64, batch: Dist) {
    let (min, max) = match batch {
        Dist::Fixed(size) => (size, size),
        Dist::Uniform { min, max } => (min, max),
        Dist::LogUniform { .. } | Dist::Exp { .. } => return,
    };
    let share = weight / (max - min + 1) as f64;
    for size in min..=max {
        op(out, "set_many", keyspace, share, false);
        let _ = writeln!(out, "batch = {size}");
    }
}

fn miss_ratio(family: &Family) -> f64 {
    let live = family.live as f64 / family.population.max(1) as f64;
    (1.0 - live).clamp(MIN_MISS_RATIO, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op::OpKind;
    use crate::workload::profile::{Overrides, parse};

    // Every built-in Stalwart profile renders to a loadgen profile that parses and keeps lock pairs balanced.
    #[test]
    fn builtin_profiles_render_and_parse() {
        for name in profiles::KV_PROFILES {
            let text = render(name).expect("renders");
            let workload = parse(
                &text,
                &Overrides {
                    popularity: None,
                    keys_scale: 1.0,
                },
            )
            .unwrap_or_else(|e| panic!("{name}: {e}\n{text}"));
            let share = |kind: OpKind| -> f64 {
                workload
                    .classes()
                    .iter()
                    .filter(|class| class.kind == kind)
                    .map(|class| class.share)
                    .sum()
            };
            assert!((share(OpKind::TryLock) - share(OpKind::Unlock)).abs() < 1e-9);
            assert!((share(OpKind::TryLease) - share(OpKind::ReleaseLease)).abs() < 1e-9);
            let total: f64 = workload.classes().iter().map(|class| class.share).sum();
            assert!((total - 1.0).abs() < 1e-9);
        }
    }
}
