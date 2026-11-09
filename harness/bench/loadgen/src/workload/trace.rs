/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use stalwart_workload::pattern::mix64;

use crate::op::{OpKind, Request};

pub const HEADER: &str = "t_ns\tnode\top\tkey_hex\tvalue_len\tttl_s\tdelta";
const MAX_VALUE: u32 = 8 << 20;

#[derive(Debug)]
pub struct TraceError(String);

impl fmt::Display for TraceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TraceError {}

#[derive(Clone, Copy, Debug)]
struct TraceOp {
    t_ns: u64,
    key_off: u32,
    key_len: u32,
    value_len: u32,
    ttl: u32,
    delta: i64,
    affinity: u32,
    kind: OpKind,
}

const _: () = assert!(size_of::<TraceOp>() <= 40);

#[derive(Default)]
pub struct TraceShard {
    ops: Vec<TraceOp>,
    keys: Vec<u8>,
    pos: usize,
    loops: u64,
}

pub struct OpShape {
    pub kind: OpKind,
    pub key_len: usize,
    pub value_len: usize,
    pub ttl: u32,
}

pub struct TracedOp<'a> {
    pub t_ns: u64,
    pub affinity: u32,
    pub request: Request<'a>,
}

impl TraceShard {
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn next<'a>(
        &'a mut self,
        values: &'a [u8],
        span_ns: u64,
        repeat: bool,
    ) -> Option<TracedOp<'a>> {
        if self.pos == self.ops.len() {
            if !repeat || self.ops.is_empty() {
                return None;
            }
            self.pos = 0;
            self.loops += 1;
        }
        let op = self.ops[self.pos];
        self.pos += 1;
        let start = op.key_off as usize;
        let key = self.keys.get(start..start + op.key_len as usize)?;
        let value_len = (op.value_len as usize).min(values.len());
        Some(TracedOp {
            t_ns: op.t_ns + self.loops * span_ns,
            affinity: op.affinity,
            request: Request {
                kind: op.kind,
                key,
                value: &values[..value_len],
                ttl: op.ttl,
                delta: op.delta,
                batch: 1,
                token: mix64(op.t_ns ^ (self.loops << 48)),
            },
        })
    }

    pub fn peek_t_ns(&self, span_ns: u64, repeat: bool) -> Option<u64> {
        match self.ops.get(self.pos) {
            Some(op) => Some(op.t_ns + self.loops * span_ns),
            None if repeat => self
                .ops
                .first()
                .map(|op| op.t_ns + (self.loops + 1) * span_ns),
            None => None,
        }
    }
}

pub struct Trace {
    pub shards: Vec<TraceShard>,
    pub records: u64,
    pub span_ns: u64,
    pub max_value: u32,
    pub kinds: Vec<OpKind>,
}

impl Trace {
    pub fn retain(&mut self, keep: impl Fn(&OpShape) -> bool) -> u64 {
        let mut dropped = 0;
        for shard in &mut self.shards {
            let before = shard.ops.len();
            shard.ops.retain(|op| {
                keep(&OpShape {
                    kind: op.kind,
                    key_len: op.key_len as usize,
                    value_len: op.value_len as usize,
                    ttl: op.ttl,
                })
            });
            dropped += (before - shard.ops.len()) as u64;
        }
        self.records -= dropped;
        let mut kinds: Vec<OpKind> = Vec::new();
        for op in self.shards.iter().flat_map(|shard| shard.ops.iter()) {
            if !kinds.contains(&op.kind) {
                kinds.push(op.kind);
            }
        }
        self.kinds = kinds;
        dropped
    }
}

#[derive(Clone, Debug, Default)]
pub struct TraceOptions {
    pub node: Option<u16>,
    pub salt: Option<Salt>,
}

#[derive(Clone, Debug)]
pub struct Salt {
    pub value: u64,
    pub prefixes: Vec<u8>,
}

impl Salt {
    fn apply(&self, key: &mut [u8]) {
        let Some((first, rest)) = key.split_first_mut() else {
            return;
        };
        if !self.prefixes.is_empty() && !self.prefixes.contains(first) {
            return;
        }
        rest.iter_mut()
            .zip(self.value.to_le_bytes())
            .for_each(|(b, s)| *b ^= s);
    }
}

pub fn load(path: &Path, shards: usize, options: &TraceOptions) -> Result<Trace, TraceError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| TraceError(format!("cannot read trace {}: {e}", path.display())))?;
    parse(&text, shards, options)
}

pub fn parse(text: &str, shard_count: usize, options: &TraceOptions) -> Result<Trace, TraceError> {
    let shard_count = shard_count.max(1);
    let mut lines = text
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.is_empty() && !line.starts_with('#'));
    match lines.next() {
        Some((_, header)) if header.trim_end() == HEADER => {}
        _ => {
            return Err(TraceError(format!(
                "trace must start with the header {HEADER:?}"
            )));
        }
    }
    let mut shards: Vec<TraceShard> = (0..shard_count).map(|_| TraceShard::default()).collect();
    let mut first_ns = None;
    let mut last_ns = 0;
    let mut records = 0;
    let mut max_value = 0;
    let mut kinds: Vec<OpKind> = Vec::new();
    let mut key = Vec::with_capacity(256);
    for (line_no, line) in lines {
        let fail = |what: &str| TraceError(format!("trace line {}: {what}", line_no + 1));
        let mut fields = line.split('\t');
        let mut field = |name: &str| {
            fields
                .next()
                .ok_or_else(|| fail(&format!("missing {name}")))
        };
        let t_ns: u64 = field("t_ns")?.parse().map_err(|_| fail("invalid t_ns"))?;
        let rec_node: u16 = field("node")?.parse().map_err(|_| fail("invalid node"))?;
        let kind = OpKind::parse(field("op")?.as_bytes()).ok_or_else(|| fail("unknown op"))?;
        let key_hex = field("key_hex")?;
        let value_len: u32 = field("value_len")?
            .parse()
            .map_err(|_| fail("invalid value_len"))?;
        let ttl: u32 = field("ttl_s")?.parse().map_err(|_| fail("invalid ttl_s"))?;
        let delta: i64 = field("delta")?
            .trim_end()
            .parse()
            .map_err(|_| fail("invalid delta"))?;
        if options.node.is_some_and(|wanted| wanted != rec_node) {
            continue;
        }
        if value_len > MAX_VALUE {
            return Err(fail("value_len above 8 MiB"));
        }
        key.clear();
        decode_hex(key_hex.as_bytes(), &mut key).ok_or_else(|| fail("invalid key_hex"))?;
        if let Some(salt) = &options.salt {
            salt.apply(&mut key);
        }
        let hash = fnv1a(&key);
        let shard = &mut shards[(hash % shard_count as u64) as usize];
        let key_off = u32::try_from(shard.keys.len()).map_err(|_| fail("key arena over 4 GiB"))?;
        shard.keys.extend_from_slice(&key);
        let key_len = u32::try_from(key.len()).map_err(|_| fail("key over 4 GiB"))?;
        shard.ops.push(TraceOp {
            t_ns,
            key_off,
            key_len,
            value_len,
            ttl,
            delta,
            affinity: (hash >> 32) as u32,
            kind,
        });
        first_ns = Some(first_ns.map_or(t_ns, |first: u64| first.min(t_ns)));
        last_ns = last_ns.max(t_ns);
        records += 1;
        max_value = max_value.max(value_len);
        if !kinds.contains(&kind) {
            kinds.push(kind);
        }
    }
    let first_ns = first_ns.ok_or_else(|| TraceError(String::from("trace has no records")))?;
    for shard in &mut shards {
        shard.ops.iter_mut().for_each(|op| op.t_ns -= first_ns);
        shard.ops.sort_by_key(|op| op.t_ns);
    }
    let span = last_ns - first_ns;
    let mean_gap = span / records.max(1);
    Ok(Trace {
        shards,
        records,
        span_ns: span + mean_gap.max(1),
        max_value,
        kinds,
    })
}

pub fn shared_values(max_value: u32) -> Arc<[u8]> {
    super::values::value_pool(max_value as usize, 0x5eed).into()
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &b| {
        (hash ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    })
}

fn decode_hex(hex: &[u8], out: &mut Vec<u8>) -> Option<()> {
    let (pairs, rest) = hex.as_chunks::<2>();
    if !rest.is_empty() {
        return None;
    }
    for &[hi, lo] in pairs {
        out.push((nibble(hi)? << 4) | nibble(lo)?);
    }
    Some(())
}

fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "# comment\nt_ns\tnode\top\tkey_hex\tvalue_len\tttl_s\tdelta\n\
        1000\t0\tincr\t08aabb\t0\t60\t1\n\
        2000\t1\ttry_lock\t15ccdd\t8\t600\t0\n\
        3000\t0\tunlock\t15ccdd\t0\t0\t0\n";

    // Records are parsed, rebased to the first timestamp and kept in time order per shard.
    #[test]
    fn parses_records() {
        let trace = parse(SAMPLE, 1, &TraceOptions::default()).expect("valid trace");
        assert_eq!(trace.records, 3);
        let mut shard = trace.shards.into_iter().next().expect("one shard");
        let values = [b'x'; 16];
        let first = shard.next(&values, 0, false).expect("first");
        assert_eq!(first.t_ns, 0);
        assert_eq!(first.request.kind, OpKind::Incr);
        assert_eq!(first.request.key, &[0x08, 0xaa, 0xbb]);
        assert_eq!(first.request.ttl, 60);
        let second = shard.next(&values, 0, false).expect("second");
        assert_eq!(second.request.value.len(), 8);
    }

    // Same key always lands on the same shard, and the node filter applies.
    #[test]
    fn key_affinity_and_node_filter() {
        let trace = parse(SAMPLE, 4, &TraceOptions::default()).expect("valid trace");
        let lock_shards = trace
            .shards
            .iter()
            .filter(|s| s.ops.iter().any(|op| op.kind == OpKind::TryLock))
            .count();
        assert_eq!(lock_shards, 1);
        let unlock_with_lock = trace.shards.iter().any(|s| {
            s.ops.iter().any(|op| op.kind == OpKind::TryLock)
                && s.ops.iter().any(|op| op.kind == OpKind::Unlock)
        });
        assert!(unlock_with_lock);
        let node0 = parse(
            SAMPLE,
            1,
            &TraceOptions {
                node: Some(0),
                salt: None,
            },
        )
        .expect("valid trace");
        assert_eq!(node0.records, 2);
    }

    // A wrong header or a bad hex key is rejected.
    #[test]
    fn rejects_bad_input() {
        assert!(parse("t_ns\top\n1\tget\n", 1, &TraceOptions::default()).is_err());
        let bad = format!("{HEADER}\n1\t0\tget\tzz\t0\t0\t0\n");
        assert!(parse(&bad, 1, &TraceOptions::default()).is_err());
    }

    // The salt changes bytes 1..=8 of keys whose prefix is listed, and only those.
    #[test]
    fn salt_applies_to_listed_prefixes() {
        let options = TraceOptions {
            node: None,
            salt: Some(Salt {
                value: 0xff,
                prefixes: vec![0x15],
            }),
        };
        let mut trace = parse(SAMPLE, 1, &options).expect("valid trace");
        let shard = trace.shards.first_mut().expect("one shard");
        let values = [0u8; 8];
        let first = shard.next(&values, 0, false).expect("first");
        assert_eq!(first.request.key, &[0x08, 0xaa, 0xbb]);
        let second = shard.next(&values, 0, false).expect("second");
        assert_eq!(second.request.key, &[0x15, 0x33, 0xdd]);
    }
}
