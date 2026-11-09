/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use stalwart_workload::deployment::{self, DEPLOYMENTS};
use stalwart_workload::dist::Dist;
use stalwart_workload::mix::{Expiry, KeyScope, Mix, ProfileSpec};
use stalwart_workload::profiles::{self, KV_PROFILES};

const STORED_WORD: f64 = 8.0;

fn mean(dist: Dist) -> f64 {
    match dist {
        Dist::Fixed(value) => value as f64,
        Dist::Uniform { min, max } => (min + max) as f64 / 2.0,
        Dist::LogUniform { ln_min, ln_max } => (ln_max.exp() - ln_min.exp()) / (ln_max - ln_min),
        Dist::Exp { mean } => mean,
    }
}

fn main() {
    println!(
        "| profile | tier | ops per action | typical ops/s per node | peak ops/s per node | cluster peak ops/s | live keys | live key bytes | live value bytes |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for name in KV_PROFILES {
        let text = profiles::builtin(name).expect("builtin profile");
        for tier in DEPLOYMENTS {
            let deployment = deployment::builtin(tier).expect("tier");
            let spec: ProfileSpec = toml::from_str(text).expect("profile toml");
            let demand = spec.demand.clone().expect("demand");
            let mix = Mix::from_spec(spec, KeyScope::Cluster, deployment).expect("profile");
            let (keys, values) = mix.families.iter().fold((0.0, 0.0), |(k, v), family| {
                let window = match family.expiry {
                    Expiry::Window(_) => 8.0,
                    _ => 0.0,
                };
                let key = 1.0
                    + (f64::from(family.id_len.min()) + f64::from(family.id_len.max())) / 2.0
                    + window;
                let live = family.live as f64;
                (
                    k + live * key,
                    v + live * mean(family.value).max(STORED_WORD),
                )
            });
            println!(
                "| {name} | {tier} | {:.2} | {:.0} | {:.0} | {:.0} | {} | {:.0} | {:.0} |",
                mix.ops_per_action(),
                demand.typical_ops,
                demand.peak_ops,
                demand.peak_ops * deployment.nodes as f64,
                mix.live_keys(),
                keys,
                values
            );
        }
    }
}
