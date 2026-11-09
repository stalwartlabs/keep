/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::WorkloadError;
use super::events::MAX_TAG;
use crate::kv::op::KvOp;
use std::{fs, path::Path};

const MAX_KEY_LEN: usize = u16::MAX as usize;
const MAX_FIELDS: usize = 16;
const ZSTD_EXTENSION: &str = "zst";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceNodes {
    All,
    Only(u16),
}

#[derive(Clone, Copy, Debug)]
pub struct TraceRecord {
    pub at_ns: u64,
    pub op: KvOp,
    key_offset: u32,
    key_len: u16,
    pub value_len: u32,
    pub ttl: u32,
    pub delta: i64,
}

#[derive(Debug)]
pub struct KvTrace {
    records: Box<[TraceRecord]>,
    keys: Box<[u8]>,
    pub span_ns: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct EventRecord {
    pub at_ns: u64,
    pub tag: u8,
    pub bytes: u32,
    pub account: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct MessageRecord {
    pub at_ns: u64,
    pub bytes: u32,
    pub count: u32,
}

#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct PublishSummary {
    pub messages: u64,
    pub events: u64,
    pub bytes: u64,
}

#[derive(Debug)]
pub struct EventTrace {
    pub events: Box<[EventRecord]>,
    pub messages: Box<[MessageRecord]>,
    pub published: PublishSummary,
    pub span_ns: u64,
    pub message_span_ns: u64,
}

#[derive(Clone, Copy)]
enum TraceKind {
    Event,
    Publish,
}

struct Columns<const N: usize> {
    index: [usize; N],
    width: usize,
}

fn records(text: &str) -> impl Iterator<Item = (usize, &str)> {
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.is_empty() && !line.starts_with('#'))
}

impl<const N: usize> Columns<N> {
    fn locate<'a>(
        lines: &mut impl Iterator<Item = (usize, &'a str)>,
        names: [&str; N],
        path: &Path,
    ) -> Result<Columns<N>, WorkloadError> {
        let (line_no, header) = lines
            .next()
            .ok_or_else(|| WorkloadError::trace(path, 1, "trace has no header"))?;
        let fields = header
            .trim_end_matches('\r')
            .split('\t')
            .collect::<Vec<_>>();
        let mut index = [0; N];
        for (slot, name) in index.iter_mut().zip(names) {
            *slot = fields
                .iter()
                .position(|field| *field == name)
                .ok_or_else(|| {
                    WorkloadError::trace(path, line_no + 1, &format!("missing column {name}"))
                })?;
        }
        if fields.len() > MAX_FIELDS {
            return Err(WorkloadError::trace(path, line_no + 1, "too many columns"));
        }
        Ok(Columns {
            index,
            width: fields.len(),
        })
    }

    fn split<'a>(
        &self,
        line: &'a str,
        path: &Path,
        line_no: usize,
    ) -> Result<[&'a str; N], WorkloadError> {
        let mut fields = [""; MAX_FIELDS];
        let mut count = 0;
        for field in line.trim_end_matches('\r').split('\t') {
            match fields.get_mut(count) {
                Some(slot) => *slot = field,
                None => return Err(WorkloadError::trace(path, line_no, "too many fields")),
            }
            count += 1;
        }
        if count != self.width {
            return Err(WorkloadError::trace(
                path,
                line_no,
                "wrong number of fields",
            ));
        }
        Ok(self.index.map(|index| fields[index]))
    }
}

fn number<T: std::str::FromStr>(
    field: &str,
    path: &Path,
    line_no: usize,
    name: &str,
) -> Result<T, WorkloadError> {
    field
        .parse()
        .map_err(|_| WorkloadError::trace(path, line_no, &format!("invalid {name}")))
}

fn read_text(path: &Path) -> Result<String, WorkloadError> {
    let bytes = fs::read(path).map_err(|err| WorkloadError::io(path, err))?;
    let bytes = match path.extension().is_some_and(|ext| ext == ZSTD_EXTENSION) {
        true => zstd::decode_all(bytes.as_slice()).map_err(|err| WorkloadError::io(path, err))?,
        false => bytes,
    };
    String::from_utf8(bytes).map_err(|_| WorkloadError::trace(path, 0, "trace is not UTF-8"))
}

fn rebase(times: impl Iterator<Item = u64> + Clone) -> (u64, u64) {
    let first = times.clone().min().unwrap_or_default();
    let last = times.max().unwrap_or_default();
    (first, last - first)
}

fn event_tag(field: &str) -> Option<u8> {
    hashify::map!(field.as_bytes(), u8,
        "state_change" => 0,
        "calendar_alert" => 1,
        "email_push" => 2,
        "push_server_update" => 3,
        "registry_insert" => 4,
        "registry_delete" => 5,
        "registry_reload" => 6,
        "cache_invalidate" => 7,
        "cache_invalidate_all" => 8,
        "cache_invalidate_negative" => 9,
        "mta_queue_running" => 10,
        "mta_queue_paused" => 11,
        "queue_refresh" => 12,
        "task_queue_refresh" => 13,
        "index_queue_refresh" => 14,
        "viewer_state_change" => 15,
    )
    .copied()
    .or_else(|| field.parse().ok().filter(|tag| *tag <= MAX_TAG))
}

impl KvTrace {
    pub fn load(path: &Path, nodes: TraceNodes) -> Result<KvTrace, WorkloadError> {
        Self::parse(&read_text(path)?, path, nodes)
    }

    pub fn parse(text: &str, path: &Path, nodes: TraceNodes) -> Result<KvTrace, WorkloadError> {
        let mut lines = records(text);
        let columns = Columns::locate(
            &mut lines,
            [
                "t_ns",
                "node",
                "op",
                "key_hex",
                "value_len",
                "ttl_s",
                "delta",
            ],
            path,
        )?;
        let mut records = Vec::new();
        let mut keys = Vec::new();
        for (index, line) in lines {
            let line_no = index + 1;
            let [at, node, op, key, value_len, ttl, delta] = columns.split(line, path, line_no)?;
            if let TraceNodes::Only(only) = nodes
                && number::<u16>(node, path, line_no, "node")? != only
            {
                continue;
            }
            let op = KvOp::parse(op.as_bytes())
                .ok_or_else(|| WorkloadError::trace(path, line_no, "unknown op"))?;
            let key_offset = u32::try_from(keys.len())
                .map_err(|_| WorkloadError::trace(path, line_no, "trace keys exceed 4 GiB"))?;
            let key_len = decode_hex(key, &mut keys)
                .ok_or_else(|| WorkloadError::trace(path, line_no, "invalid key_hex"))?;
            records.push(TraceRecord {
                at_ns: number(at, path, line_no, "t_ns")?,
                op,
                key_offset,
                key_len,
                value_len: number(value_len, path, line_no, "value_len")?,
                ttl: number(ttl, path, line_no, "ttl_s")?,
                delta: match delta {
                    "" => 0,
                    delta => number(delta, path, line_no, "delta")?,
                },
            });
        }
        records.sort_by_key(|record| record.at_ns);
        let (first, span_ns) = rebase(records.iter().map(|record| record.at_ns));
        records.iter_mut().for_each(|record| record.at_ns -= first);
        Ok(KvTrace {
            records: records.into_boxed_slice(),
            keys: keys.into_boxed_slice(),
            span_ns,
        })
    }

    #[cfg(test)]
    pub fn count(&self) -> usize {
        self.records.len()
    }

    pub fn record(&self, index: usize) -> Option<(&TraceRecord, &[u8])> {
        let record = self.records.get(index)?;
        let start = record.key_offset as usize;
        let key = self.keys.get(start..start + usize::from(record.key_len))?;
        Some((record, key))
    }

    pub fn at_ns(&self, index: usize) -> Option<u64> {
        self.records.get(index).map(|record| record.at_ns)
    }
}

impl EventTrace {
    pub fn load(path: &Path, nodes: TraceNodes) -> Result<EventTrace, WorkloadError> {
        Self::parse(&read_text(path)?, path, nodes)
    }

    pub fn parse(text: &str, path: &Path, nodes: TraceNodes) -> Result<EventTrace, WorkloadError> {
        let mut lines = records(text);
        let columns = Columns::locate(
            &mut lines,
            ["t_ns", "node", "kind", "tag", "bytes", "account", "count"],
            path,
        )?;
        let mut events = Vec::new();
        let mut messages = Vec::new();
        let mut published = PublishSummary::default();
        for (index, line) in lines {
            let line_no = index + 1;
            let [at, node, kind, tag, bytes, account, count] =
                columns.split(line, path, line_no)?;
            if let TraceNodes::Only(only) = nodes
                && number::<u16>(node, path, line_no, "node")? != only
            {
                continue;
            }
            let at_ns: u64 = number(at, path, line_no, "t_ns")?;
            let bytes: u32 = number(bytes, path, line_no, "bytes")?;
            let kind = hashify::map!(kind.as_bytes(), TraceKind,
                "event" => TraceKind::Event,
                "publish" => TraceKind::Publish,
            )
            .copied()
            .ok_or_else(|| WorkloadError::trace(path, line_no, "unknown kind"))?;
            match kind {
                TraceKind::Event => events.push(EventRecord {
                    at_ns,
                    tag: event_tag(tag)
                        .ok_or_else(|| WorkloadError::trace(path, line_no, "unknown event tag"))?,
                    bytes,
                    account: match account {
                        "" => 0,
                        account => number(account, path, line_no, "account")?,
                    },
                }),
                TraceKind::Publish => {
                    let count: u32 = number(count, path, line_no, "count")?;
                    published.messages += 1;
                    published.events += u64::from(count);
                    published.bytes += u64::from(bytes);
                    messages.push(MessageRecord {
                        at_ns,
                        bytes,
                        count: count.max(1),
                    });
                }
            }
        }
        events.sort_by_key(|event| event.at_ns);
        messages.sort_by_key(|message| message.at_ns);
        let (first, span_ns) = rebase(events.iter().map(|event| event.at_ns));
        events.iter_mut().for_each(|event| event.at_ns -= first);
        let (first, message_span_ns) = rebase(messages.iter().map(|message| message.at_ns));
        messages
            .iter_mut()
            .for_each(|message| message.at_ns -= first);
        Ok(EventTrace {
            events: events.into_boxed_slice(),
            messages: messages.into_boxed_slice(),
            published,
            span_ns,
            message_span_ns,
        })
    }
}

fn decode_hex(hex: &str, out: &mut Vec<u8>) -> Option<u16> {
    let (pairs, rest) = hex.as_bytes().as_chunks::<2>();
    if !rest.is_empty() || pairs.len() > MAX_KEY_LEN {
        return None;
    }
    out.reserve(pairs.len());
    for [high, low] in pairs {
        out.push((nibble(*high)? << 4) | nibble(*low)?);
    }
    u16::try_from(pairs.len()).ok()
}

fn nibble(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        b'A'..=b'F' => Some(digit - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{EventTrace, KvTrace, TraceNodes};
    use crate::kv::op::KvOp;
    use rand::{RngExt, SeedableRng, rngs::SmallRng};
    use std::path::Path;

    const KV: &str = "# stalwart-kv-trace v1 profile=mixed window=peak node=1\n\
        t_ns\tnode\top\tkey_hex\tvalue_len\tttl_s\tdelta\n\
        200\t1\tincr\t0801020304\t0\t60\t1\n\
        100\t2\tget\t0aff\t0\t0\t0\n\
        150\t1\ttry_lock\t0e0102\t8\t600\t0\n";

    const EVENTS: &str = "# stalwart-stream-trace v1\n\
        t_ns\tnode\tkind\ttag\tbytes\taccount\tcount\n\
        47449112\t1\tevent\tqueue_refresh\t1\t0\t1\n\
        47478676\t1\tpublish\t-\t3\t0\t1\n\
        47400000\t1\tevent\tcache_invalidate\t61\t10\t1\n";

    // Comments are skipped, records filtered by node, sorted, rebased and keys decoded.
    #[test]
    fn parses_kv_trace() {
        let trace = KvTrace::parse(KV, Path::new("t"), TraceNodes::Only(1)).expect("parse");
        assert_eq!(trace.count(), 2);
        let (first, key) = trace.record(0).expect("record");
        assert_eq!(
            (first.at_ns, first.op, key),
            (0, KvOp::TryLock, &[0x0e, 1, 2][..])
        );
        let (second, key) = trace.record(1).expect("record");
        assert_eq!(
            (second.at_ns, second.op, second.delta, key.len()),
            (50, KvOp::Incr, 1, 5)
        );
        let all = KvTrace::parse(KV, Path::new("t"), TraceNodes::All).expect("parse");
        assert_eq!(all.count(), 3);
    }

    // Event tags are read by name, publish rows become replayable messages.
    #[test]
    fn parses_event_trace() {
        let trace = EventTrace::parse(EVENTS, Path::new("e"), TraceNodes::All).expect("parse");
        assert_eq!(trace.events.len(), 2);
        assert_eq!((trace.events[0].at_ns, trace.events[0].tag), (0, 7));
        assert_eq!((trace.events[1].tag, trace.events[1].bytes), (12, 1));
        assert_eq!((trace.published.messages, trace.published.events), (1, 1));
        assert_eq!(trace.messages[0].bytes, 3);
    }

    // Random and mutated input never panics the parsers.
    #[test]
    fn random_input_is_rejected_cleanly() {
        let mut rng = SmallRng::seed_from_u64(3);
        let alphabet = b"0123456789abcdefg\t\n#-xtnsodeplkyhvaugcr_";
        for round in 0..20_000 {
            let mut text = match round % 2 {
                0 => KV.to_string(),
                _ => EVENTS.to_string(),
            };
            for _ in 0..rng.random_range(0..40) {
                let at = rng.random_range(0..=text.len());
                if text.is_char_boundary(at) {
                    text.insert(at, alphabet[rng.random_range(0..alphabet.len())] as char);
                }
            }
            let _ = KvTrace::parse(&text, Path::new("f"), TraceNodes::All);
            let _ = EventTrace::parse(&text, Path::new("f"), TraceNodes::All);
        }
    }
}
