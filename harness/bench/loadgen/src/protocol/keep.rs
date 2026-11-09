/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use keep_proto::{
    Command, Decoded as FrameDecoded, Frame, Key, LockToken, ReplyMode, Request as KeepRequest,
    ServerFrame, SetMode, Status as KeepStatus, Ttl, TtlReport, Value,
};

use super::{ProtocolError, Reply};
use crate::op::{OpKind, Outcome, Request};

#[inline]
pub fn encode(req: &Request<'_>, id: u32, out: &mut Vec<u8>) -> Result<u8, ProtocolError> {
    let invalid = || ProtocolError::Invalid(req.kind);
    let required_ttl = || Ttl::from_secs(req.ttl).ok_or_else(invalid);
    let token = LockToken::from_u64(req.token);
    if req.kind == OpKind::SetMany {
        let batch = req.batch.max(1);
        let size = req.key.len() / usize::from(batch);
        let value = Value::new(req.value).ok_or_else(invalid)?;
        for key in req.key.chunks_exact(size.max(1)) {
            let command = Command::Set {
                key: Key::new(key).ok_or_else(invalid)?,
                value,
                ttl: Ttl::from_secs(req.ttl),
                mode: SetMode::Always,
            };
            put(command, id, out);
        }
        return Ok(batch);
    }
    let key = Key::new(req.key).ok_or_else(invalid)?;
    let command = match req.kind {
        OpKind::Get => Command::Get {
            key,
            ttl: TtlReport::Omit,
        },
        OpKind::Exists => Command::Exists { key },
        OpKind::Set => Command::Set {
            key,
            value: Value::new(req.value).ok_or_else(invalid)?,
            ttl: Ttl::from_secs(req.ttl),
            mode: SetMode::Always,
        },
        OpKind::Incr => Command::Incr {
            key,
            delta: req.delta,
            ttl: Ttl::from_secs(req.ttl),
        },
        OpKind::TryLock | OpKind::TryLease => Command::TryLock {
            key,
            ttl: required_ttl()?,
            token,
        },
        OpKind::Renew => Command::Renew {
            key,
            ttl: required_ttl()?,
            token: None,
        },
        OpKind::RenewLease => Command::Renew {
            key,
            ttl: required_ttl()?,
            token: Some(token),
        },
        OpKind::ReleaseLease => Command::Unlock { key, token },
        OpKind::Unlock | OpKind::Delete => Command::Delete { key },
        OpKind::DeletePrefix => Command::DeletePrefix { prefix: key },
        OpKind::SetMany => return Err(invalid()),
    };
    put(command, id, out);
    Ok(1)
}

#[inline]
fn put(command: Command<'_>, id: u32, out: &mut Vec<u8>) {
    KeepRequest {
        id,
        reply: ReplyMode::Always,
        command,
    }
    .encode(out);
}

#[inline]
pub fn decode(buf: &[u8]) -> Result<Option<Reply>, ProtocolError> {
    let mut skipped = 0;
    loop {
        let rest = buf.get(skipped..).ok_or(ProtocolError::Malformed)?;
        let (frame, len) = match Frame::decode(rest).map_err(|_| ProtocolError::Malformed)? {
            FrameDecoded::Complete { frame, len } => (frame, len),
            FrameDecoded::Incomplete { .. } => return Ok(None),
        };
        match ServerFrame::parse(&frame).map_err(|_| ProtocolError::Malformed)? {
            ServerFrame::Response(response) => {
                return Ok(Some(Reply {
                    len: skipped + len,
                    outcome: outcome(response.status),
                    id: response.id,
                    cursor: None,
                }));
            }
            ServerFrame::GoAway { .. } => return Err(ProtocolError::UnexpectedReply),
            ServerFrame::Event(_)
            | ServerFrame::Heartbeat(_)
            | ServerFrame::End(_)
            | ServerFrame::TopologyChanged { .. } => skipped += len,
        }
    }
}

#[inline]
fn outcome(status: KeepStatus) -> Outcome {
    match status {
        KeepStatus::Ok => Outcome::Ok,
        KeepStatus::NotFound | KeepStatus::Conflict => Outcome::Miss,
        _ => Outcome::Error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keep_proto::{Op as KeepOp, Response};

    fn request(kind: OpKind, ttl: u32, batch: u8, key: &'static [u8]) -> Request<'static> {
        Request {
            kind,
            key,
            value: b"vv",
            ttl,
            delta: 3,
            batch,
            token: 0x0102_0304_0506_0708,
        }
    }

    fn parse(buf: &[u8]) -> Vec<(u32, KeepOp, Command<'_>)> {
        Frame::split_all(buf)
            .map(|frame| {
                let frame = frame.expect("framed");
                let request = KeepRequest::parse(&frame).expect("valid request");
                (request.id, request.command.op(), request.command)
            })
            .collect()
    }

    // Stalwart's calls map onto version 1 ops as in PROTOCOL.md 11.
    #[test]
    fn stalwart_mapping() {
        let token = LockToken::from_u64(0x0102_0304_0506_0708);
        let cases = [
            (OpKind::TryLock, KeepOp::TryLock),
            (OpKind::TryLease, KeepOp::TryLock),
            (OpKind::Renew, KeepOp::Renew),
            (OpKind::RenewLease, KeepOp::Renew),
            (OpKind::ReleaseLease, KeepOp::Unlock),
            (OpKind::Unlock, KeepOp::Delete),
            (OpKind::Delete, KeepOp::Delete),
            (OpKind::Get, KeepOp::Get),
            (OpKind::Incr, KeepOp::Incr),
        ];
        for (kind, op) in cases {
            let mut out = Vec::new();
            assert_eq!(encode(&request(kind, 300, 1, b"k1"), 7, &mut out), Ok(1));
            let frames = parse(&out);
            let [(id, got, command)] = frames.as_slice() else {
                panic!("one frame for {kind:?}");
            };
            assert_eq!((*id, *got), (7, op), "{kind:?}");
            match (kind, command) {
                (OpKind::TryLock | OpKind::TryLease, Command::TryLock { token: t, ttl, .. }) => {
                    assert_eq!((*t, ttl.secs()), (token, 300));
                }
                (OpKind::Renew, Command::Renew { token: t, .. }) => assert_eq!(*t, None),
                (OpKind::RenewLease, Command::Renew { token: t, .. }) => {
                    assert_eq!(*t, Some(token));
                }
                (OpKind::ReleaseLease, Command::Unlock { token: t, .. }) => {
                    assert_eq!(*t, token);
                }
                _ => {}
            }
        }
    }

    // set_many writes one Set frame per key under one id.
    #[test]
    fn set_many_pipelines() {
        let mut out = Vec::new();
        assert_eq!(
            encode(&request(OpKind::SetMany, 9, 3, b"k1k2k3"), 5, &mut out),
            Ok(3)
        );
        let frames = parse(&out);
        assert_eq!(frames.len(), 3);
        for ((id, op, command), key) in frames.iter().zip([b"k1", b"k2", b"k3"]) {
            assert_eq!((*id, *op), (5, KeepOp::Set));
            let Command::Set { key: got, ttl, .. } = command else {
                panic!("set expected");
            };
            assert_eq!((got.as_bytes(), ttl.map(Ttl::secs)), (&key[..], Some(9)));
        }
    }

    // Locks need a TTL and keys must fit a frame.
    #[test]
    fn rejects_invalid_requests() {
        for kind in [OpKind::TryLock, OpKind::Renew, OpKind::TryLease] {
            assert_eq!(
                encode(&request(kind, 0, 1, b"k"), 1, &mut Vec::new()),
                Err(ProtocolError::Invalid(kind))
            );
        }
        assert!(encode(&request(OpKind::Get, 0, 1, b""), 1, &mut Vec::new()).is_err());
    }

    // Replies are matched by id, framed incrementally, and pushes before them are skipped.
    #[test]
    fn decode_replies() {
        let mut reply = Vec::new();
        keep_proto::Event {
            seq: 1,
            topic: keep_proto::Topic::from(3),
            payload: Value::EMPTY,
        }
        .encode(&mut reply);
        Response::encode_status(&mut reply, 41, KeepStatus::Conflict);
        for cut in 0..reply.len() {
            assert!(decode(&reply[..cut]).expect("ok").is_none());
        }
        let decoded = decode(&reply).expect("ok").expect("complete");
        assert_eq!(
            (decoded.id, decoded.outcome, decoded.len),
            (41, Outcome::Miss, reply.len())
        );
        let mut error = Vec::new();
        Response::encode_status(&mut error, 2, KeepStatus::Busy);
        let decoded = decode(&error).expect("ok").expect("complete");
        assert_eq!(decoded.outcome, Outcome::Error);
    }
}
