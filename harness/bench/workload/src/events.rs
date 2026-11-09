/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    WorkloadError,
    dist::{Dist, DistSpec},
};
use rand::{RngExt, rngs::SmallRng};
use serde::Deserialize;

pub const MAX_TAG: u8 = 15;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventMixSpec {
    #[serde(rename = "event")]
    pub events: Vec<EventSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventSpec {
    pub name: String,
    pub tag: u8,
    pub weight: u32,
    pub size: DistSpec,
    #[serde(default = "one")]
    pub accounts: u32,
}

fn one() -> u32 {
    1
}

#[derive(Debug)]
pub struct EventClass {
    pub tag: u8,
    pub size: Dist,
    pub accounts: u32,
}

#[derive(Debug)]
pub struct EventMix {
    pub classes: Box<[EventClass]>,
    cumulative: Box<[u32]>,
}

#[derive(Clone, Copy, Debug)]
pub struct SampledEvent {
    pub tag: u8,
    pub size: u32,
    pub account: u32,
}

impl EventMix {
    pub fn from_spec(spec: EventMixSpec) -> Result<EventMix, WorkloadError> {
        let mut classes = Vec::with_capacity(spec.events.len());
        let mut cumulative = Vec::with_capacity(spec.events.len());
        let mut total = 0u32;
        for event in spec.events.into_iter().filter(|event| event.weight > 0) {
            if event.tag > MAX_TAG || event.accounts == 0 {
                return Err(WorkloadError::invalid(
                    &event.name,
                    "tag above 15 or zero accounts",
                ));
            }
            total = total
                .checked_add(event.weight)
                .ok_or_else(|| WorkloadError::invalid(&event.name, "weights overflow"))?;
            cumulative.push(total);
            classes.push(EventClass {
                size: Dist::try_from(event.size)
                    .map_err(|err| WorkloadError::invalid(&event.name, &err.to_string()))?,
                tag: event.tag,
                accounts: event.accounts,
            });
        }
        if classes.is_empty() {
            return Err(WorkloadError::invalid("event mix", "no event has a weight"));
        }
        Ok(EventMix {
            classes: classes.into_boxed_slice(),
            cumulative: cumulative.into_boxed_slice(),
        })
    }

    #[inline]
    pub fn sample(&self, rng: &mut SmallRng) -> SampledEvent {
        let total = self.cumulative.last().copied().unwrap_or(1);
        let target = rng.random_range(0..total);
        let index = self.cumulative.partition_point(|&bound| bound <= target);
        let class = &self.classes[index.min(self.classes.len() - 1)];
        SampledEvent {
            tag: class.tag,
            size: u32::try_from(class.size.sample(rng).max(1)).unwrap_or(u32::MAX),
            account: rng.random_range(0..class.accounts),
        }
    }
}
