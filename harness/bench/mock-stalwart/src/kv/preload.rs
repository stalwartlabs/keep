/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::{
    KvBackend, Probe,
    model::{Effect, Model, ValueTag},
    stats::KvCounters,
};
use crate::{
    clock::Clock,
    pattern::{fill, fingerprint, mix64},
    stats::bump,
    workload::mix::{KeyCodec, Mix, PreloadOp},
};
use futures::{StreamExt, stream};
use rand::{SeedableRng, rngs::SmallRng};

const CONCURRENCY: usize = 256;
const PRELOAD_DELTA: i64 = 1;

pub async fn preload(
    backend: &KvBackend,
    mix: &Mix,
    codec: &KeyCodec,
    model: Option<&Model>,
    clock: Clock,
    scale: f64,
    counters: &KvCounters,
) -> u64 {
    let mut total = 0;
    for family in mix.families.iter() {
        let live = (family.live as f64 * scale) as u64;
        if live == 0 {
            continue;
        }
        let ttl = family.ttl_at(family.key_ref(0, clock.now()), clock.now());
        let op = family.preload_op();
        stream::iter(0..live)
            .for_each_concurrent(CONCURRENCY, |index| async move {
                let mut rng = SmallRng::seed_from_u64(mix64(index ^ u64::from(family.prefix)));
                let mut key = Vec::with_capacity(64);
                let key_ref = family.key_ref(index, clock.now());
                family.write_key(codec, key_ref, &mut key);
                let mut value = Vec::new();
                if op == PreloadOp::Set {
                    fill(
                        &mut value,
                        mix64(index),
                        family.value.sample(&mut rng) as usize,
                    );
                }
                let ticket = model.map(|model| (model, model.begin(fingerprint(&key))));
                let mut probe = Probe::default();
                let start = clock.now();
                let result = match op {
                    PreloadOp::Incr => backend
                        .incr(&key, PRELOAD_DELTA, ttl, &mut probe)
                        .await
                        .map(|result| Effect::Incr {
                            delta: PRELOAD_DELTA,
                            ttl,
                            result,
                        }),
                    PreloadOp::Set => {
                        backend
                            .set(&key, &value, ttl, &mut probe)
                            .await
                            .map(|()| Effect::Set {
                                value: ValueTag::of(&value),
                                ttl,
                            })
                    }
                };
                let end = clock.now();
                let stored = result.is_ok();
                if let Some((model, ticket)) = ticket {
                    let effect = result.unwrap_or(Effect::Failed { mutates: true });
                    model.complete(ticket, effect, start, end);
                }
                match stored {
                    true => bump(&counters.preload_keys),
                    false => bump(&counters.preload_errors),
                }
            })
            .await;
        total += live;
    }
    total
}
