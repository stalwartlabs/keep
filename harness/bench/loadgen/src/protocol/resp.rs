/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::{ProtocolError, Reply, parse_i64, parse_u64};
use crate::op::{OpKind, Outcome, Request};

pub const INCR_EXPIRE_SCRIPT: &str = "redis.call('INCRBY', KEYS[1], ARGV[1])
                 redis.call('EXPIRE', KEYS[1], ARGV[2])
                 return redis.call('GET', KEYS[1])";
pub const LEASE_RENEW_SCRIPT: &str = "if redis.call('GET', KEYS[1]) == ARGV[1] then
                    return redis.call('EXPIRE', KEYS[1], ARGV[2])
                 end
                 return 0";
pub const LEASE_RELEASE_SCRIPT: &str = "if redis.call('GET', KEYS[1]) == ARGV[1] then
                    return redis.call('DEL', KEYS[1])
                 end
                 return 0";
pub const LEASE_PROBE_KEY: &[u8] = b"loadgen:lease-probe";
pub const SCAN_COUNT: &[u8] = b"100";
const MAX_DEPTH: u8 = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RespMapping {
    Stalwart,
    Native,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseRenew {
    Script,
    SetIfEq,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseRelease {
    Script,
    DelIfEq,
    DelexIfEq,
}

impl LeaseRenew {
    pub fn name(self) -> &'static str {
        match self {
            LeaseRenew::Script => "lua",
            LeaseRenew::SetIfEq => "set-ifeq",
        }
    }
}

impl LeaseRelease {
    pub fn name(self) -> &'static str {
        match self {
            LeaseRelease::Script => "lua",
            LeaseRelease::DelIfEq => "delifeq",
            LeaseRelease::DelexIfEq => "delex-ifeq",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Resp {
    pub mapping: RespMapping,
    pub script_sha: Box<[u8]>,
    pub lease_renew_sha: Box<[u8]>,
    pub lease_release_sha: Box<[u8]>,
    pub lease_renew: LeaseRenew,
    pub lease_release: LeaseRelease,
}

impl Resp {
    #[inline]
    pub fn encode(&self, req: &Request<'_>, out: &mut Vec<u8>) -> u8 {
        let key = req.key;
        match req.kind {
            OpKind::Get => {
                out.extend_from_slice(b"*2\r\n$3\r\nGET\r\n");
                bulk(out, key);
            }
            OpKind::Exists => {
                out.extend_from_slice(b"*2\r\n$6\r\nEXISTS\r\n");
                bulk(out, key);
            }
            OpKind::Set if req.ttl == 0 => {
                out.extend_from_slice(b"*3\r\n$3\r\nSET\r\n");
                bulk(out, key);
                bulk(out, req.value);
            }
            OpKind::Set if self.mapping == RespMapping::Stalwart => {
                out.extend_from_slice(b"*4\r\n$5\r\nSETEX\r\n");
                bulk(out, key);
                bulk_u64(out, u64::from(req.ttl));
                bulk(out, req.value);
            }
            OpKind::Set => set_ex(out, key, req.value, req.ttl),
            OpKind::Incr if req.ttl == 0 => incrby(out, key, req.delta),
            OpKind::Incr if self.mapping == RespMapping::Stalwart => {
                out.extend_from_slice(b"*6\r\n$7\r\nEVALSHA\r\n");
                bulk(out, &self.script_sha);
                out.extend_from_slice(b"$1\r\n1\r\n");
                bulk(out, key);
                bulk_i64(out, req.delta);
                bulk_u64(out, u64::from(req.ttl));
            }
            OpKind::Incr => {
                incrby(out, key, req.delta);
                out.extend_from_slice(b"*4\r\n$6\r\nEXPIRE\r\n");
                bulk(out, key);
                bulk_u64(out, u64::from(req.ttl));
                out.extend_from_slice(b"$2\r\nNX\r\n");
                return 2;
            }
            OpKind::RenewLease if self.lease_renew == LeaseRenew::SetIfEq => match req.ttl {
                0 => {
                    out.extend_from_slice(b"*5\r\n$3\r\nSET\r\n");
                    bulk(out, key);
                    bulk(out, req.value);
                    out.extend_from_slice(b"$4\r\nIFEQ\r\n");
                    bulk(out, req.value);
                }
                ttl => {
                    out.extend_from_slice(b"*7\r\n$3\r\nSET\r\n");
                    bulk(out, key);
                    bulk(out, req.value);
                    out.extend_from_slice(b"$4\r\nIFEQ\r\n");
                    bulk(out, req.value);
                    out.extend_from_slice(b"$2\r\nEX\r\n");
                    bulk_u64(out, u64::from(ttl));
                }
            },
            OpKind::RenewLease => {
                out.extend_from_slice(b"*6\r\n$7\r\nEVALSHA\r\n");
                bulk(out, &self.lease_renew_sha);
                out.extend_from_slice(b"$1\r\n1\r\n");
                bulk(out, key);
                bulk(out, req.value);
                bulk_u64(out, u64::from(req.ttl));
            }
            OpKind::ReleaseLease => match self.lease_release {
                LeaseRelease::Script => {
                    out.extend_from_slice(b"*5\r\n$7\r\nEVALSHA\r\n");
                    bulk(out, &self.lease_release_sha);
                    out.extend_from_slice(b"$1\r\n1\r\n");
                    bulk(out, key);
                    bulk(out, req.value);
                }
                LeaseRelease::DelIfEq => {
                    out.extend_from_slice(b"*3\r\n$7\r\nDELIFEQ\r\n");
                    bulk(out, key);
                    bulk(out, req.value);
                }
                LeaseRelease::DelexIfEq => {
                    out.extend_from_slice(b"*4\r\n$5\r\nDELEX\r\n");
                    bulk(out, key);
                    out.extend_from_slice(b"$4\r\nIFEQ\r\n");
                    bulk(out, req.value);
                }
            },
            OpKind::SetMany => {
                let size = key.len() / usize::from(req.batch.max(1));
                for key in key.chunks_exact(size.max(1)) {
                    match req.ttl {
                        0 => {
                            out.extend_from_slice(b"*3\r\n$3\r\nSET\r\n");
                            bulk(out, key);
                            bulk(out, req.value);
                        }
                        ttl => set_ex(out, key, req.value, ttl),
                    }
                }
                return req.batch.max(1);
            }
            OpKind::TryLock | OpKind::TryLease if req.ttl == 0 => {
                out.extend_from_slice(b"*4\r\n$3\r\nSET\r\n");
                bulk(out, key);
                bulk(out, req.value);
                out.extend_from_slice(b"$2\r\nNX\r\n");
            }
            OpKind::TryLock | OpKind::TryLease => {
                out.extend_from_slice(b"*6\r\n$3\r\nSET\r\n");
                bulk(out, key);
                bulk(out, req.value);
                out.extend_from_slice(b"$2\r\nNX\r\n$2\r\nEX\r\n");
                bulk_u64(out, u64::from(req.ttl));
            }
            OpKind::Unlock | OpKind::Delete => {
                out.extend_from_slice(b"*2\r\n$3\r\nDEL\r\n");
                bulk(out, key);
            }
            OpKind::Renew if req.ttl == 0 => {
                out.extend_from_slice(b"*2\r\n$7\r\nPERSIST\r\n");
                bulk(out, key);
            }
            OpKind::Renew if self.mapping == RespMapping::Stalwart => {
                set_ex(out, key, req.value, req.ttl);
            }
            OpKind::Renew => {
                out.extend_from_slice(b"*3\r\n$6\r\nEXPIRE\r\n");
                bulk(out, key);
                bulk_u64(out, u64::from(req.ttl));
            }
            OpKind::DeletePrefix => scan(out, b"0", key),
        }
        1
    }

    #[inline]
    pub fn decode(
        &self,
        buf: &[u8],
        kind: OpKind,
        last_part: bool,
    ) -> Result<Option<Reply>, ProtocolError> {
        let Some((value, len)) = parse_value(buf, 0)? else {
            return Ok(None);
        };
        if kind == OpKind::DeletePrefix && last_part {
            return scan_reply(buf, value, len).map(Some);
        }
        Ok(Some(Reply {
            len,
            outcome: classify(kind, value),
            id: 0,
            cursor: None,
        }))
    }
}

#[inline]
fn classify(kind: OpKind, value: RespValue) -> Outcome {
    match value {
        RespValue::Error => Outcome::Error,
        RespValue::Nil => Outcome::Miss,
        RespValue::Int(0)
            if matches!(
                kind,
                OpKind::Exists
                    | OpKind::Unlock
                    | OpKind::Delete
                    | OpKind::Renew
                    | OpKind::RenewLease
                    | OpKind::ReleaseLease
            ) =>
        {
            Outcome::Miss
        }
        _ => Outcome::Ok,
    }
}

fn scan_reply(buf: &[u8], value: RespValue, len: usize) -> Result<Reply, ProtocolError> {
    let RespValue::Array { count: 2, start } = value else {
        return Ok(Reply {
            len,
            outcome: match value {
                RespValue::Error => Outcome::Error,
                _ => Outcome::Miss,
            },
            id: 0,
            cursor: None,
        });
    };
    let elems = buf.get(start..len).ok_or(ProtocolError::Malformed)?;
    let Some((
        RespValue::Bulk {
            start,
            len: cursor_len,
        },
        cursor_end,
    )) = parse_value(elems, 1)?
    else {
        return Err(ProtocolError::Malformed);
    };
    let cursor = elems
        .get(start..start + cursor_len)
        .and_then(parse_u64)
        .ok_or(ProtocolError::Malformed)?;
    let keys_start = len - elems.len() + cursor_end;
    Ok(Reply {
        len,
        outcome: Outcome::Ok,
        id: 0,
        cursor: Some((cursor, keys_start)),
    })
}

pub fn encode_scan_step(
    reply: &[u8],
    keys_start: usize,
    cursor: u64,
    prefix: &[u8],
    out: &mut Vec<u8>,
) -> Result<u8, ProtocolError> {
    let keys = reply.get(keys_start..).ok_or(ProtocolError::Malformed)?;
    let Some((RespValue::Array { count, start }, keys_len)) = parse_value(keys, 1)? else {
        return Err(ProtocolError::Malformed);
    };
    let mut parts = 0;
    if count > 0 {
        let raw = keys.get(start..keys_len).ok_or(ProtocolError::Malformed)?;
        array_header(out, count + 1);
        out.extend_from_slice(b"$3\r\nDEL\r\n");
        out.extend_from_slice(raw);
        parts += 1;
    }
    if cursor != 0 {
        let mut digits = itoa::Buffer::new();
        scan(out, digits.format(cursor).as_bytes(), prefix);
        parts += 1;
    }
    Ok(parts)
}

fn scan(out: &mut Vec<u8>, cursor: &[u8], prefix: &[u8]) {
    out.extend_from_slice(b"*6\r\n$4\r\nSCAN\r\n");
    bulk(out, cursor);
    out.extend_from_slice(b"$5\r\nMATCH\r\n");
    let escaped = prefix
        .iter()
        .filter(|b| matches!(b, b'*' | b'?' | b'[' | b']' | b'\\'))
        .count();
    let mut digits = itoa::Buffer::new();
    out.push(b'$');
    out.extend_from_slice(digits.format(prefix.len() + escaped + 1).as_bytes());
    out.extend_from_slice(b"\r\n");
    for &b in prefix {
        if matches!(b, b'*' | b'?' | b'[' | b']' | b'\\') {
            out.push(b'\\');
        }
        out.push(b);
    }
    out.extend_from_slice(b"*\r\n$5\r\nCOUNT\r\n");
    bulk(out, SCAN_COUNT);
}

fn incrby(out: &mut Vec<u8>, key: &[u8], delta: i64) {
    out.extend_from_slice(b"*3\r\n$6\r\nINCRBY\r\n");
    bulk(out, key);
    bulk_i64(out, delta);
}

fn set_ex(out: &mut Vec<u8>, key: &[u8], value: &[u8], ttl: u32) {
    out.extend_from_slice(b"*5\r\n$3\r\nSET\r\n");
    bulk(out, key);
    bulk(out, value);
    out.extend_from_slice(b"$2\r\nEX\r\n");
    bulk_u64(out, u64::from(ttl));
}

pub fn array_header(out: &mut Vec<u8>, count: usize) {
    let mut digits = itoa::Buffer::new();
    out.push(b'*');
    out.extend_from_slice(digits.format(count).as_bytes());
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn bulk(out: &mut Vec<u8>, bytes: &[u8]) {
    let mut digits = itoa::Buffer::new();
    out.push(b'$');
    out.extend_from_slice(digits.format(bytes.len()).as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(bytes);
    out.extend_from_slice(b"\r\n");
}

#[inline]
fn bulk_u64(out: &mut Vec<u8>, value: u64) {
    let mut digits = itoa::Buffer::new();
    bulk(out, digits.format(value).as_bytes());
}

#[inline]
fn bulk_i64(out: &mut Vec<u8>, value: i64) {
    let mut digits = itoa::Buffer::new();
    bulk(out, digits.format(value).as_bytes());
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RespValue {
    Simple,
    Error,
    Int(i64),
    Bulk { start: usize, len: usize },
    Nil,
    Array { count: usize, start: usize },
}

#[inline]
pub fn parse_value(buf: &[u8], depth: u8) -> Result<Option<(RespValue, usize)>, ProtocolError> {
    let Some((&tag, rest)) = buf.split_first() else {
        return Ok(None);
    };
    let Some(end) = memchr::memchr(b'\n', rest) else {
        return Ok(None);
    };
    let line = rest
        .get(..end)
        .and_then(|line| line.strip_suffix(b"\r"))
        .ok_or(ProtocolError::Malformed)?;
    let header = end + 2;
    match tag {
        b'+' => Ok(Some((RespValue::Simple, header))),
        b'-' => Ok(Some((RespValue::Error, header))),
        b':' => parse_i64(line)
            .map(|v| Some((RespValue::Int(v), header)))
            .ok_or(ProtocolError::Malformed),
        b'$' => match parse_i64(line).ok_or(ProtocolError::Malformed)? {
            -1 => Ok(Some((RespValue::Nil, header))),
            len => {
                let len = usize::try_from(len).map_err(|_| ProtocolError::Malformed)?;
                let total = header + len + 2;
                match buf.get(header + len..total) {
                    Some(b"\r\n") => Ok(Some((RespValue::Bulk { start: header, len }, total))),
                    Some(_) => Err(ProtocolError::Malformed),
                    None => Ok(None),
                }
            }
        },
        b'*' => match parse_i64(line).ok_or(ProtocolError::Malformed)? {
            -1 => Ok(Some((RespValue::Nil, header))),
            count => {
                if depth >= MAX_DEPTH {
                    return Err(ProtocolError::Malformed);
                }
                let count = usize::try_from(count).map_err(|_| ProtocolError::Malformed)?;
                let mut pos = header;
                for _ in 0..count {
                    let elem = buf.get(pos..).ok_or(ProtocolError::Malformed)?;
                    match parse_value(elem, depth + 1)? {
                        Some((_, len)) => pos += len,
                        None => return Ok(None),
                    }
                }
                Ok(Some((
                    RespValue::Array {
                        count,
                        start: header,
                    },
                    pos,
                )))
            }
        },
        b'_' => Ok(Some((RespValue::Nil, header))),
        _ => Err(ProtocolError::Malformed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(kind: OpKind, ttl: u32) -> Request<'static> {
        Request {
            kind,
            key: b"k1",
            value: b"vv",
            ttl,
            delta: 3,
            batch: 1,
            token: 42,
        }
    }

    fn resp(mapping: RespMapping) -> Resp {
        Resp {
            mapping,
            script_sha: b"0123456789012345678901234567890123456789".to_vec().into(),
            lease_renew_sha: b"1111111111111111111111111111111111111111".to_vec().into(),
            lease_release_sha: b"2222222222222222222222222222222222222222".to_vec().into(),
            lease_renew: LeaseRenew::Script,
            lease_release: LeaseRelease::Script,
        }
    }

    // Stalwart's mapping uses EVALSHA for incr with TTL, SETEX and SET NX EX, as in crates/store.
    #[test]
    fn stalwart_mapping() {
        let r = resp(RespMapping::Stalwart);
        let mut out = Vec::new();
        assert_eq!(r.encode(&req(OpKind::Incr, 60), &mut out), 1);
        assert_eq!(
            out,
            b"*6\r\n$7\r\nEVALSHA\r\n$40\r\n0123456789012345678901234567890123456789\r\n$1\r\n1\r\n$2\r\nk1\r\n$1\r\n3\r\n$2\r\n60\r\n"
        );
        out.clear();
        r.encode(&req(OpKind::Set, 5), &mut out);
        assert_eq!(
            out,
            b"*4\r\n$5\r\nSETEX\r\n$2\r\nk1\r\n$1\r\n5\r\n$2\r\nvv\r\n"
        );
        out.clear();
        r.encode(&req(OpKind::Renew, 600), &mut out);
        assert_eq!(
            out,
            b"*5\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nvv\r\n$2\r\nEX\r\n$3\r\n600\r\n"
        );
    }

    // Leases use Stalwart's SET NX EX and its two Lua scripts; set_many pipelines one SET EX per key.
    #[test]
    fn lease_and_set_many() {
        let r = resp(RespMapping::Stalwart);
        let lease = |kind| Request {
            kind,
            key: b"k1",
            value: b"42",
            ttl: 300,
            delta: 0,
            batch: 1,
            token: 42,
        };
        let mut out = Vec::new();
        r.encode(&lease(OpKind::TryLease), &mut out);
        assert_eq!(
            out,
            b"*6\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\n42\r\n$2\r\nNX\r\n$2\r\nEX\r\n$3\r\n300\r\n"
        );
        out.clear();
        r.encode(&lease(OpKind::RenewLease), &mut out);
        assert_eq!(
            out,
            b"*6\r\n$7\r\nEVALSHA\r\n$40\r\n1111111111111111111111111111111111111111\r\n$1\r\n1\r\n$2\r\nk1\r\n$2\r\n42\r\n$3\r\n300\r\n"
        );
        out.clear();
        r.encode(&lease(OpKind::ReleaseLease), &mut out);
        assert_eq!(
            out,
            b"*5\r\n$7\r\nEVALSHA\r\n$40\r\n2222222222222222222222222222222222222222\r\n$1\r\n1\r\n$2\r\nk1\r\n$2\r\n42\r\n"
        );
        let renewed0 = r
            .decode(b":0\r\n", OpKind::RenewLease, true)
            .expect("ok")
            .expect("complete");
        assert_eq!(renewed0.outcome, Outcome::Miss);
        out.clear();
        let many = Request {
            kind: OpKind::SetMany,
            key: b"k1k2",
            value: b"v",
            ttl: 9,
            delta: 0,
            batch: 2,
            token: 42,
        };
        assert_eq!(r.encode(&many, &mut out), 2);
        assert_eq!(
            out,
            b"*5\r\n$3\r\nSET\r\n$2\r\nk1\r\n$1\r\nv\r\n$2\r\nEX\r\n$1\r\n9\r\n*5\r\n$3\r\nSET\r\n$2\r\nk2\r\n$1\r\nv\r\n$2\r\nEX\r\n$1\r\n9\r\n"
        );
    }

    // The native mapping pipelines INCRBY and EXPIRE NX as two replies.
    #[test]
    fn native_mapping() {
        let r = resp(RespMapping::Native);
        let mut out = Vec::new();
        assert_eq!(r.encode(&req(OpKind::Incr, 60), &mut out), 2);
        assert_eq!(
            out,
            b"*3\r\n$6\r\nINCRBY\r\n$2\r\nk1\r\n$1\r\n3\r\n*4\r\n$6\r\nEXPIRE\r\n$2\r\nk1\r\n$2\r\n60\r\n$2\r\nNX\r\n"
        );
        out.clear();
        r.encode(&req(OpKind::TryLock, 600), &mut out);
        assert_eq!(
            out,
            b"*6\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nvv\r\n$2\r\nNX\r\n$2\r\nEX\r\n$3\r\n600\r\n"
        );
    }

    // Native lease ops use SET IFEQ and DELIFEQ or DELEX IFEQ where the server has them.
    #[test]
    fn native_lease_commands() {
        let lease = |kind, ttl| Request {
            kind,
            key: b"k1",
            value: b"42",
            ttl,
            delta: 0,
            batch: 1,
            token: 42,
        };
        let mut r = resp(RespMapping::Native);
        r.lease_renew = LeaseRenew::SetIfEq;
        r.lease_release = LeaseRelease::DelIfEq;
        let mut out = Vec::new();
        assert_eq!(r.encode(&lease(OpKind::RenewLease, 300), &mut out), 1);
        assert_eq!(
            out,
            b"*7\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\n42\r\n$4\r\nIFEQ\r\n$2\r\n42\r\n$2\r\nEX\r\n$3\r\n300\r\n"
        );
        out.clear();
        r.encode(&lease(OpKind::RenewLease, 0), &mut out);
        assert_eq!(
            out,
            b"*5\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\n42\r\n$4\r\nIFEQ\r\n$2\r\n42\r\n"
        );
        out.clear();
        r.encode(&lease(OpKind::ReleaseLease, 0), &mut out);
        assert_eq!(out, b"*3\r\n$7\r\nDELIFEQ\r\n$2\r\nk1\r\n$2\r\n42\r\n");
        out.clear();
        r.lease_release = LeaseRelease::DelexIfEq;
        r.encode(&lease(OpKind::ReleaseLease, 0), &mut out);
        assert_eq!(
            out,
            b"*4\r\n$5\r\nDELEX\r\n$2\r\nk1\r\n$4\r\nIFEQ\r\n$2\r\n42\r\n"
        );
        let renew_nil = r
            .decode(b"$-1\r\n", OpKind::RenewLease, true)
            .expect("ok")
            .expect("complete");
        assert_eq!(renew_nil.outcome, Outcome::Miss);
        let renew_ok = r
            .decode(b"+OK\r\n", OpKind::RenewLease, true)
            .expect("ok")
            .expect("complete");
        assert_eq!(renew_ok.outcome, Outcome::Ok);
        let release0 = r
            .decode(b":0\r\n", OpKind::ReleaseLease, true)
            .expect("ok")
            .expect("complete");
        assert_eq!(release0.outcome, Outcome::Miss);
    }

    // Replies are framed incrementally and classified per op.
    #[test]
    fn decode_replies() {
        let r = resp(RespMapping::Native);
        let full = b"$5\r\nhello\r\n";
        for cut in 0..full.len() {
            assert!(
                r.decode(&full[..cut], OpKind::Get, true)
                    .expect("ok")
                    .is_none()
            );
        }
        let reply = r
            .decode(full, OpKind::Get, true)
            .expect("ok")
            .expect("complete");
        assert_eq!((reply.len, reply.outcome), (full.len(), Outcome::Ok));
        let miss = r
            .decode(b"$-1\r\n", OpKind::Get, true)
            .expect("ok")
            .expect("complete");
        assert_eq!(miss.outcome, Outcome::Miss);
        let not_locked = r
            .decode(b"$-1\r\n", OpKind::TryLock, true)
            .expect("ok")
            .expect("complete");
        assert_eq!(not_locked.outcome, Outcome::Miss);
        let del0 = r
            .decode(b":0\r\n", OpKind::Unlock, true)
            .expect("ok")
            .expect("complete");
        assert_eq!(del0.outcome, Outcome::Miss);
        let expire_nx = r
            .decode(b":0\r\n", OpKind::Incr, true)
            .expect("ok")
            .expect("complete");
        assert_eq!(expire_nx.outcome, Outcome::Ok);
        let error = r
            .decode(b"-ERR x\r\n", OpKind::Get, true)
            .expect("ok")
            .expect("complete");
        assert_eq!(error.outcome, Outcome::Error);
        assert!(r.decode(b"$3\r\nabcde", OpKind::Get, true).is_err());
    }

    // A SCAN reply yields the cursor and a DEL of the returned keys plus the next SCAN.
    #[test]
    fn scan_continuation() {
        let r = resp(RespMapping::Stalwart);
        let reply = b"*2\r\n$2\r\n17\r\n*2\r\n$2\r\na1\r\n$2\r\na2\r\n";
        let decoded = r
            .decode(reply, OpKind::DeletePrefix, true)
            .expect("ok")
            .expect("complete");
        assert_eq!(decoded.len, reply.len());
        let (cursor, keys_start) = decoded.cursor.expect("scan cursor");
        assert_eq!(cursor, 17);
        let mut out = Vec::new();
        let parts = encode_scan_step(reply, keys_start, cursor, b"a*", &mut out).expect("ok");
        assert_eq!(parts, 2);
        assert_eq!(
            out,
            b"*3\r\n$3\r\nDEL\r\n$2\r\na1\r\n$2\r\na2\r\n*6\r\n$4\r\nSCAN\r\n$2\r\n17\r\n$5\r\nMATCH\r\n$4\r\na\\**\r\n$5\r\nCOUNT\r\n$3\r\n100\r\n"
        );
    }
}
