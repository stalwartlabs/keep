/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::num::NonZeroU16;

use keep_proto::{ClusterSecret, Fingerprint, KeyHasher, ServerInfo};

const COLLECT_ATTEMPTS_PER_CONNECTION: usize = 8;

#[derive(Clone, Debug)]
pub struct ShardMap {
    hasher: KeyHasher,
    shards: NonZeroU16,
    fingerprint: Option<Fingerprint>,
}

#[derive(Clone, Copy, Debug)]
pub struct ShardCollect {
    shards: u16,
    fingerprint: Option<Fingerprint>,
}

impl ShardCollect {
    pub fn new(map: &ShardMap) -> Self {
        ShardCollect {
            shards: map.shards.get(),
            fingerprint: map.fingerprint,
        }
    }

    pub fn shard_of(&self, info: ServerInfo) -> Result<usize, String> {
        let shard = info
            .shard
            .ok_or("the Hello reply names no shard (TLV 2 missing)")?;
        if shard.count().get() != self.shards {
            return Err(format!(
                "the server has {} shards, --shard-collect says {}",
                shard.count(),
                self.shards
            ));
        }
        if let Some(expected) = self.fingerprint
            && info.placement != Some(expected)
        {
            return Err(format!(
                "placement fingerprint {:?} differs from ours {expected:?} (secret or hash mismatch)",
                info.placement
            ));
        }
        Ok(usize::from(shard.shard()))
    }

    pub fn collect<S>(
        &self,
        per_shard: usize,
        mut open: impl FnMut() -> Result<(S, ServerInfo), String>,
    ) -> Result<(Vec<S>, usize), String> {
        let shards = usize::from(self.shards);
        let wanted = shards * per_shard;
        let mut buckets: Vec<Vec<S>> = (0..shards).map(|_| Vec::with_capacity(per_shard)).collect();
        let mut kept = 0;
        let mut attempts = 0;
        while kept < wanted {
            if attempts >= wanted.max(1) * COLLECT_ATTEMPTS_PER_CONNECTION {
                return Err(format!(
                    "shard collection kept {kept} of {wanted} connections after {attempts} connects (cap: {COLLECT_ATTEMPTS_PER_CONNECTION} per connection, PROTOCOL 9.3)"
                ));
            }
            attempts += 1;
            let (stream, info) = open()?;
            let shard = self.shard_of(info)?;
            if let Some(bucket) = buckets
                .get_mut(shard)
                .filter(|bucket| bucket.len() < per_shard)
            {
                bucket.push(stream);
                kept += 1;
            }
        }
        Ok((buckets.into_iter().flatten().collect(), attempts))
    }
}

impl ShardMap {
    pub fn keep(secret: &ClusterSecret, shards: u16) -> Option<Self> {
        Some(ShardMap {
            hasher: secret.hasher(),
            shards: NonZeroU16::new(shards)?,
            fingerprint: Some(secret.fingerprint()),
        })
    }

    #[inline]
    pub fn node_of(&self, key: &[u8]) -> usize {
        usize::from(self.hasher.hash(key).shard(self.shards))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keep_proto::ShardInfo;

    const K1: u64 = 0x6b65_6570_6c61_6231;

    fn map(k0: u64, shards: u16) -> ShardMap {
        ShardMap::keep(&ClusterSecret::new(k0, K1), shards).expect("shards")
    }

    fn info(shard: u16, count: u16, placement: Option<Fingerprint>) -> ServerInfo {
        ServerInfo {
            shard: ShardInfo::new(shard, count),
            placement,
            ..ServerInfo::default()
        }
    }

    // Collecting keeps the first connections per shard in shard order and drops the surplus.
    #[test]
    fn collect_orders_by_shard() {
        let collect = ShardCollect::new(&map(1, 3));
        let fingerprint = map(1, 3).fingerprint;
        let mut landed = [2u16, 2, 0, 2, 1, 0, 1].into_iter().enumerate();
        let (streams, attempts) = collect
            .collect(2, || {
                let (id, shard) = landed.next().ok_or("ran out")?;
                Ok((id, info(shard, 3, fingerprint)))
            })
            .expect("collects");
        assert_eq!(streams, vec![2, 5, 4, 6, 0, 1]);
        assert_eq!(attempts, 7);
    }

    // A shard count, missing shard or fingerprint different from ours fails the run.
    #[test]
    fn collect_rejects_mismatches() {
        let ours = map(1, 3);
        let collect = ShardCollect::new(&ours);
        let good = ours.fingerprint;
        let theirs = map(2, 3).fingerprint;
        assert!(collect.shard_of(info(1, 3, good)).is_ok());
        assert!(collect.shard_of(info(1, 4, good)).is_err());
        assert!(collect.shard_of(info(1, 3, theirs)).is_err());
        assert!(collect.shard_of(info(1, 3, None)).is_err());
        assert!(collect.shard_of(ServerInfo::default()).is_err());
        let stuck = collect.collect(1, || Ok(((), info(0, 3, good))));
        assert!(stuck.is_err());
    }

    // Every shard owns a fair share of keys, and the secret changes the owners.
    #[test]
    fn keys_spread_and_depend_on_secret() {
        let (a, b) = (map(1, 8), map(2, 8));
        let mut owned = [0u32; 8];
        let mut moved = 0;
        for index in 0u32..8192 {
            let key = index.to_le_bytes();
            owned[a.node_of(&key)] += 1;
            moved += usize::from(a.node_of(&key) != b.node_of(&key));
        }
        assert!(owned.iter().all(|&n| n > 850), "{owned:?}");
        assert!(moved > 6500, "{moved}");
        assert!(ShardMap::keep(&ClusterSecret::new(1, 2), 0).is_none());
        let secret = ClusterSecret::new(7, K1);
        let keep = map(7, 5);
        for index in 0u32..256 {
            let key = index.to_le_bytes();
            let shards = NonZeroU16::new(5).expect("nonzero");
            assert_eq!(
                keep.node_of(&key),
                usize::from(secret.hasher().hash(&key).shard(shards))
            );
        }
    }
}
