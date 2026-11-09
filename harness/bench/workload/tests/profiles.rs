/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use stalwart_workload::deployment::{self, DEPLOYMENTS};
use stalwart_workload::events::{EventMix, EventMixSpec};
use stalwart_workload::mix::{KeyScope, Mix, ProfileSpec};
use stalwart_workload::profiles::{self, EVENT_PROFILES, KV_PROFILES};

// Every KV profile validates at every deployment tier in both key scopes and names an event mix that exists.
#[test]
fn kv_profiles_validate_at_every_tier() {
    for name in KV_PROFILES {
        let text = profiles::builtin(name).expect("builtin");
        for tier in DEPLOYMENTS {
            let deployment = deployment::builtin(tier).expect("tier");
            for scope in [KeyScope::Node, KeyScope::Cluster] {
                let spec: ProfileSpec = toml::from_str(text).expect("toml");
                let demand = spec.demand.clone().expect("demand");
                assert!(
                    profiles::builtin(&demand.events).is_some(),
                    "{name}: {}",
                    demand.events
                );
                assert!(demand.peak_ops >= demand.typical_ops);
                Mix::from_spec(spec, scope, deployment)
                    .unwrap_or_else(|e| panic!("{name}@{tier}: {e}"));
            }
        }
    }
}

// Every event mix parses.
#[test]
fn event_profiles_parse() {
    for name in EVENT_PROFILES {
        let spec: EventMixSpec =
            toml::from_str(profiles::builtin(name).expect("builtin")).expect("toml");
        EventMix::from_spec(spec).unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}

// The mixed profile reproduces the measured class shares it was derived from.
#[test]
fn mixed_matches_measured_shares() {
    let spec: ProfileSpec = toml::from_str(profiles::MIXED).expect("toml");
    let mix = Mix::from_spec(spec, KeyScope::Cluster, &deployment::LAB).expect("mix");
    let ops = |family: &str| -> f64 {
        mix.weighted_actions()
            .filter(|(action, _)| &*mix.families[usize::from(action.family)].name == family)
            .map(|(action, weight)| f64::from(weight) * action.kind.ops())
            .sum()
    };
    let total: f64 = mix
        .weighted_actions()
        .map(|(action, weight)| f64::from(weight) * action.kind.ops())
        .sum();
    let share = |family: &str| ops(family) / total;
    assert!((share("imap-rate") - 0.362).abs() < 0.01);
    assert!((share("queue-lock") - 0.298).abs() < 0.01);
    assert!((share("index-lock") - 0.075).abs() < 0.01);
}
