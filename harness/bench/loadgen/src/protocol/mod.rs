/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

pub mod cluster;
pub mod keep;
pub mod meta;
pub mod resp;
pub mod shard;

use std::fmt;

use self::resp::Resp;
use crate::op::{OpKind, Outcome, Request};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    Malformed,
    UnexpectedId,
    UnexpectedReply,
    Unsupported(OpKind),
    Invalid(OpKind),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ProtocolError::Malformed => "malformed reply",
            ProtocolError::UnexpectedId => "reply with an unknown request id",
            ProtocolError::UnexpectedReply => "reply without a request in flight",
            ProtocolError::Unsupported(kind) => {
                return write!(f, "{} has no form in this protocol", kind.name());
            }
            ProtocolError::Invalid(kind) => {
                return write!(
                    f,
                    "{} request out of the protocol's limits (key length or TTL)",
                    kind.name()
                );
            }
        })
    }
}

impl std::error::Error for ProtocolError {}

#[derive(Clone, Copy, Debug)]
pub struct Reply {
    pub len: usize,
    pub outcome: Outcome,
    pub id: u32,
    pub cursor: Option<(u64, usize)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Matching {
    Fifo,
    Id,
}

#[derive(Clone, Debug)]
pub enum Protocol {
    Resp(Resp),
    Meta,
    Keep,
}

impl Protocol {
    pub fn matching(&self) -> Matching {
        match self {
            Protocol::Resp(_) | Protocol::Meta => Matching::Fifo,
            Protocol::Keep => Matching::Id,
        }
    }

    pub fn supports(&self, kind: OpKind, routed: bool) -> bool {
        match (self, kind) {
            (_, OpKind::DeletePrefix | OpKind::SetMany) if routed => false,
            (
                Protocol::Meta,
                OpKind::DeletePrefix
                | OpKind::TryLease
                | OpKind::RenewLease
                | OpKind::ReleaseLease
                | OpKind::SetMany,
            ) => false,
            (Protocol::Keep, _) => true,
            _ => true,
        }
    }

    #[inline]
    pub fn encode(
        &self,
        req: &Request<'_>,
        id: u32,
        out: &mut Vec<u8>,
    ) -> Result<u8, ProtocolError> {
        match self {
            Protocol::Resp(resp) => Ok(resp.encode(req, out)),
            Protocol::Meta => Ok(meta::encode(req, out)),
            Protocol::Keep => keep::encode(req, id, out),
        }
    }

    #[inline]
    pub fn decode(
        &self,
        buf: &[u8],
        expect: Option<(OpKind, bool)>,
    ) -> Result<Option<Reply>, ProtocolError> {
        match self {
            Protocol::Resp(resp) => {
                let (kind, last_part) = expect.ok_or(ProtocolError::UnexpectedReply)?;
                resp.decode(buf, kind, last_part)
            }
            Protocol::Meta => {
                expect.ok_or(ProtocolError::UnexpectedReply)?;
                meta::decode(buf)
            }
            Protocol::Keep => keep::decode(buf),
        }
    }
}

#[inline]
pub fn parse_u64(digits: &[u8]) -> Option<u64> {
    if digits.is_empty() || digits.len() > 20 {
        return None;
    }
    digits.iter().try_fold(0u64, |acc, &d| {
        d.is_ascii_digit()
            .then(|| acc.checked_mul(10)?.checked_add(u64::from(d - b'0')))?
    })
}

#[inline]
pub fn parse_i64(text: &[u8]) -> Option<i64> {
    match text.split_first() {
        Some((b'-', digits)) => i64::try_from(parse_u64(digits)?).ok().map(|v| -v),
        _ => i64::try_from(parse_u64(text)?).ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integer parsing rejects junk and overflow.
    #[test]
    fn integers() {
        assert_eq!(parse_i64(b"-12"), Some(-12));
        assert_eq!(parse_i64(b"0"), Some(0));
        assert_eq!(parse_u64(b"18446744073709551615"), Some(u64::MAX));
        assert_eq!(parse_u64(b"18446744073709551616"), None);
        assert_eq!(parse_i64(b"1a"), None);
        assert_eq!(parse_i64(b""), None);
        assert_eq!(parse_i64(b"-"), None);
    }
}
