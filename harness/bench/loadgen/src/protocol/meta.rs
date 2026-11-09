/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use super::{ProtocolError, Reply, parse_u64};
use crate::op::{OpKind, Outcome, Request};

pub const MAX_KEY: usize = 250;

#[derive(Clone, Copy)]
enum Code {
    Stored,
    Value,
    Negative,
    Noop,
    Failure,
}

#[inline]
pub fn encode(req: &Request<'_>, out: &mut Vec<u8>) -> u8 {
    let mut digits = itoa::Buffer::new();
    match req.kind {
        OpKind::Get => {
            command(out, b"mg ", req.key);
            out.extend_from_slice(b" v\r\n");
        }
        OpKind::Exists => {
            command(out, b"mg ", req.key);
            out.extend_from_slice(b"\r\n");
        }
        OpKind::Set | OpKind::TryLock => {
            command(out, b"ms ", req.key);
            out.push(b' ');
            out.extend_from_slice(digits.format(req.value.len()).as_bytes());
            if req.ttl > 0 {
                out.extend_from_slice(b" T");
                out.extend_from_slice(digits.format(req.ttl).as_bytes());
            }
            if req.kind == OpKind::TryLock {
                out.extend_from_slice(b" ME");
            }
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(req.value);
            out.extend_from_slice(b"\r\n");
        }
        OpKind::Incr => {
            command(out, b"ma ", req.key);
            out.extend_from_slice(b" N");
            out.extend_from_slice(digits.format(req.ttl).as_bytes());
            let magnitude = req.delta.unsigned_abs();
            if req.delta < 0 {
                out.extend_from_slice(b" MD J0 D");
            } else {
                out.extend_from_slice(b" J");
                out.extend_from_slice(digits.format(magnitude).as_bytes());
                out.extend_from_slice(b" D");
            }
            out.extend_from_slice(digits.format(magnitude).as_bytes());
            out.extend_from_slice(b" v\r\n");
        }
        OpKind::Unlock | OpKind::Delete => {
            command(out, b"md ", req.key);
            out.extend_from_slice(b"\r\n");
        }
        OpKind::Renew => {
            command(out, b"mg ", req.key);
            out.extend_from_slice(b" T");
            out.extend_from_slice(digits.format(req.ttl).as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        OpKind::DeletePrefix
        | OpKind::TryLease
        | OpKind::RenewLease
        | OpKind::ReleaseLease
        | OpKind::SetMany => {
            out.extend_from_slice(b"mn\r\n");
        }
    }
    1
}

#[inline]
fn command(out: &mut Vec<u8>, verb: &[u8], key: &[u8]) {
    out.extend_from_slice(verb);
    if key.len() <= MAX_KEY && key.iter().all(|&b| b.is_ascii_graphic()) {
        out.extend_from_slice(key);
    } else {
        encode_base64_key(out, key);
    }
}

#[cold]
#[inline(never)]
fn encode_base64_key(out: &mut Vec<u8>, key: &[u8]) {
    let start = out.len();
    out.resize(start + base64::encoded_len(key.len(), true).unwrap_or(0), 0);
    let written = STANDARD
        .encode_slice(key, out.get_mut(start..).unwrap_or_default())
        .unwrap_or(0);
    out.truncate(start + written);
    out.extend_from_slice(b" b");
}

#[inline]
pub fn decode(buf: &[u8]) -> Result<Option<Reply>, ProtocolError> {
    let Some(end) = memchr::memchr(b'\n', buf) else {
        return Ok(None);
    };
    let line = buf
        .get(..end)
        .and_then(|line| line.strip_suffix(b"\r"))
        .ok_or(ProtocolError::Malformed)?;
    let header = end + 1;
    let mut parts = line.splitn(2, |&b| b == b' ');
    let token = parts.next().unwrap_or_default();
    let rest = parts.next().unwrap_or_default();
    let code = hashify::map!(
        token,
        Code,
        "HD" => Code::Stored,
        "VA" => Code::Value,
        "EN" => Code::Negative,
        "NF" => Code::Negative,
        "NS" => Code::Negative,
        "EX" => Code::Negative,
        "MN" => Code::Noop,
        "ERROR" => Code::Failure,
        "CLIENT_ERROR" => Code::Failure,
        "SERVER_ERROR" => Code::Failure,
    )
    .copied()
    .ok_or(ProtocolError::Malformed)?;
    let (len, outcome) = match code {
        Code::Stored | Code::Noop => (header, Outcome::Ok),
        Code::Negative => (header, Outcome::Miss),
        Code::Failure => (header, Outcome::Error),
        Code::Value => {
            let size = rest.split(|&b| b == b' ').next().unwrap_or_default();
            let size = parse_u64(size)
                .and_then(|n| usize::try_from(n).ok())
                .ok_or(ProtocolError::Malformed)?;
            let total = header + size + 2;
            match buf.get(header + size..total) {
                Some(b"\r\n") => (total, Outcome::Ok),
                Some(_) => return Err(ProtocolError::Malformed),
                None => return Ok(None),
            }
        }
    };
    Ok(Some(Reply {
        len,
        outcome,
        id: 0,
        cursor: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(kind: OpKind, key: &[u8], ttl: u32, delta: i64) -> Vec<u8> {
        let mut out = Vec::new();
        encode(
            &Request {
                kind,
                key,
                value: b"vv",
                ttl,
                delta,
                batch: 1,
                token: 42,
            },
            &mut out,
        );
        out
    }

    // Each op maps to the meta command documented in memcached doc/protocol.txt.
    #[test]
    fn encodes_ops() {
        assert_eq!(encoded(OpKind::Get, b"k", 0, 0), b"mg k v\r\n");
        assert_eq!(encoded(OpKind::Exists, b"k", 0, 0), b"mg k\r\n");
        assert_eq!(encoded(OpKind::Set, b"k", 60, 0), b"ms k 2 T60\r\nvv\r\n");
        assert_eq!(
            encoded(OpKind::TryLock, b"k", 600, 0),
            b"ms k 2 T600 ME\r\nvv\r\n"
        );
        assert_eq!(encoded(OpKind::Incr, b"k", 60, 1), b"ma k N60 J1 D1 v\r\n");
        assert_eq!(
            encoded(OpKind::Incr, b"k", 60, -2),
            b"ma k N60 MD J0 D2 v\r\n"
        );
        assert_eq!(encoded(OpKind::Unlock, b"k", 0, 0), b"md k\r\n");
        assert_eq!(encoded(OpKind::Renew, b"k", 30, 0), b"mg k T30\r\n");
    }

    // Binary keys travel base64-encoded with the b flag.
    #[test]
    fn binary_keys_use_base64() {
        assert_eq!(
            encoded(OpKind::Get, &[0x08, 0xff], 0, 0),
            b"mg CP8= b v\r\n"
        );
    }

    // Replies are framed incrementally and classified.
    #[test]
    fn decodes_replies() {
        let full = b"VA 3 t\r\nabc\r\n";
        for cut in 0..full.len() {
            assert!(decode(&full[..cut]).expect("ok").is_none());
        }
        let value = decode(full).expect("ok").expect("complete");
        assert_eq!((value.len, value.outcome), (full.len(), Outcome::Ok));
        assert_eq!(
            decode(b"EN\r\n").expect("ok").expect("complete").outcome,
            Outcome::Miss
        );
        assert_eq!(
            decode(b"NS\r\n").expect("ok").expect("complete").outcome,
            Outcome::Miss
        );
        assert_eq!(
            decode(b"SERVER_ERROR out of memory\r\n")
                .expect("ok")
                .expect("complete")
                .outcome,
            Outcome::Error
        );
        assert!(decode(b"XX\r\n").is_err());
    }
}
