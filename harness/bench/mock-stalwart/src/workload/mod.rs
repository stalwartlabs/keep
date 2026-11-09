/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

pub mod trace;

pub use stalwart_workload::{WorkloadError, deployment, dist, events, mix, profiles};

use deployment::Deployment;
use events::{EventMix, EventMixSpec};
use mix::{KeyScope, Mix, ProfileSpec};
use std::{fs, path::Path};
use trace::{EventTrace, KvTrace, TraceNodes};

#[derive(Debug)]
pub enum Workload {
    Mix(Mix),
    Trace(KvTrace),
}

#[derive(Debug)]
pub enum EventSource {
    Mix(EventMix),
    Trace(EventTrace),
}

pub enum Source<'a> {
    Builtin(&'static str),
    File(&'a Path),
    Trace(&'a Path),
}

impl<'a> Source<'a> {
    pub fn parse(spec: &'a str) -> Source<'a> {
        match (profiles::builtin(spec), spec.strip_prefix("trace:")) {
            (Some(text), _) => Source::Builtin(text),
            (None, Some(path)) => Source::Trace(Path::new(path)),
            (None, None) => Source::File(Path::new(spec)),
        }
    }
}

fn read_toml<T: serde::de::DeserializeOwned>(source: &Source<'_>) -> Result<T, WorkloadError> {
    let (text, label) = match source {
        Source::Builtin(text) => ((*text).to_string(), "builtin".to_string()),
        Source::File(path) | Source::Trace(path) => (
            fs::read_to_string(path).map_err(|err| WorkloadError::io(path, err))?,
            path.display().to_string(),
        ),
    };
    toml::from_str(&text).map_err(|source| WorkloadError::Toml {
        path: label.into_boxed_str(),
        source,
    })
}

impl Workload {
    pub fn load(
        spec: &str,
        scope: KeyScope,
        deployment: &Deployment,
        nodes: TraceNodes,
    ) -> Result<Workload, WorkloadError> {
        match Source::parse(spec) {
            Source::Trace(path) => KvTrace::load(path, nodes).map(Workload::Trace),
            source => Mix::from_spec(read_toml::<ProfileSpec>(&source)?, scope, deployment)
                .map(Workload::Mix),
        }
    }
}

impl EventSource {
    pub fn load(spec: &str, nodes: TraceNodes) -> Result<EventSource, WorkloadError> {
        match Source::parse(spec) {
            Source::Trace(path) => EventTrace::load(path, nodes).map(EventSource::Trace),
            source => {
                EventMix::from_spec(read_toml::<EventMixSpec>(&source)?).map(EventSource::Mix)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EventSource, Workload, deployment, mix::KeyScope, profiles, trace::TraceNodes};

    // Every built-in profile parses and validates in both key scopes.
    #[test]
    fn builtin_profiles_are_valid() {
        for name in profiles::KV_PROFILES {
            for scope in [KeyScope::Node, KeyScope::Cluster] {
                Workload::load(name, scope, &deployment::LAB, TraceNodes::All).expect("profile");
            }
        }
        for name in profiles::EVENT_PROFILES {
            EventSource::load(name, TraceNodes::All).expect("event mix");
        }
    }
}
