/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use keep_proto::{
    Command, Decoded as KeepDecoded, Frame, PROTOCOL_VERSION, ReplyMode, Request as KeepRequest,
    Response, Status as KeepStatus, Value,
};

use crate::protocol::ProtocolError;
use crate::protocol::resp::{RespValue, parse_value};

const FAKE_SHA: &[u8] = b"$40\r\n0000000000000000000000000000000000000000\r\n";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerProtocol {
    Resp,
    Meta,
    Keep,
}

pub struct Framer {
    protocol: ServerProtocol,
    cluster_slots: Box<[u8]>,
}

impl Framer {
    pub fn new(protocol: ServerProtocol, cluster_slots: Box<[u8]>) -> Self {
        Framer {
            protocol,
            cluster_slots,
        }
    }

    #[inline]
    pub fn answer(
        &mut self,
        buf: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<Option<usize>, ProtocolError> {
        match self.protocol {
            ServerProtocol::Resp => answer_resp(buf, out, &self.cluster_slots),
            ServerProtocol::Meta => answer_meta(buf, out),
            ServerProtocol::Keep => answer_keep(buf, out),
        }
    }
}

#[inline]
fn answer_resp(
    buf: &[u8],
    out: &mut Vec<u8>,
    cluster_slots: &[u8],
) -> Result<Option<usize>, ProtocolError> {
    let Some((value, len)) = parse_value(buf, 0)? else {
        return Ok(None);
    };
    let RespValue::Array { count, start } = value else {
        return Err(ProtocolError::Malformed);
    };
    let args = buf.get(start..len).ok_or(ProtocolError::Malformed)?;
    let command = match parse_value(args, 1)? {
        Some((RespValue::Bulk { start, len }, _)) if count > 0 => args
            .get(start..start + len)
            .ok_or(ProtocolError::Malformed)?,
        _ => return Err(ProtocolError::Malformed),
    };
    let reply: &[u8] = hashify::fnc_map_ignore_case!(command,
        "GET" => b"$1\r\n1\r\n",
        "EVALSHA" => b"$1\r\n1\r\n",
        "EVAL" => b"$1\r\n1\r\n",
        "EXISTS" => b":1\r\n",
        "DEL" => b":1\r\n",
        "DELIFEQ" => b":1\r\n",
        "DELEX" => b":1\r\n",
        "UNLINK" => b":1\r\n",
        "EXPIRE" => b":1\r\n",
        "PEXPIRE" => b":1\r\n",
        "PERSIST" => b":1\r\n",
        "INCR" => b":1\r\n",
        "INCRBY" => b":1\r\n",
        "SET" => b"+OK\r\n",
        "SETEX" => b"+OK\r\n",
        "PSETEX" => b"+OK\r\n",
        "AUTH" => b"+OK\r\n",
        "SELECT" => b"+OK\r\n",
        "PING" => b"+PONG\r\n",
        "SCAN" => b"*2\r\n$1\r\n0\r\n*0\r\n",
        "SCRIPT" => FAKE_SHA,
        "CLUSTER" => cluster_slots,
        _ => b"-ERR unknown command\r\n"
    );
    out.extend_from_slice(reply);
    Ok(Some(len))
}

#[inline]
fn answer_meta(buf: &[u8], out: &mut Vec<u8>) -> Result<Option<usize>, ProtocolError> {
    let Some(end) = memchr::memchr(b'\n', buf) else {
        return Ok(None);
    };
    let line = buf
        .get(..end)
        .and_then(|line| line.strip_suffix(b"\r"))
        .ok_or(ProtocolError::Malformed)?;
    let mut tokens = line.split(|&b| b == b' ').filter(|t| !t.is_empty());
    let verb = tokens.next().unwrap_or_default();
    let header = end + 1;
    let consumed = hashify::fnc_map!(verb,
        "mg" => {
            value_or_stored(tokens, out);
            header
        },
        "ma" => {
            value_or_stored(tokens, out);
            header
        },
        "ms" => {
            let _key = tokens.next();
            let size = tokens
                .next()
                .and_then(crate::protocol::parse_u64)
                .and_then(|n| usize::try_from(n).ok())
                .ok_or(ProtocolError::Malformed)?;
            let total = header + size + 2;
            match buf.get(header + size..total) {
                Some(b"\r\n") => {}
                Some(_) => return Err(ProtocolError::Malformed),
                None => return Ok(None),
            }
            out.extend_from_slice(b"HD\r\n");
            total
        },
        "md" => {
            out.extend_from_slice(b"HD\r\n");
            header
        },
        "mn" => {
            out.extend_from_slice(b"MN\r\n");
            header
        },
        _ => {
            out.extend_from_slice(b"ERROR\r\n");
            header
        }
    );
    Ok(Some(consumed))
}

#[inline]
fn value_or_stored<'a>(tokens: impl Iterator<Item = &'a [u8]>, out: &mut Vec<u8>) {
    let mut flags = tokens.skip(1);
    out.extend_from_slice(if flags.any(|t| t == b"v") {
        b"VA 1\r\n1\r\n"
    } else {
        b"HD\r\n"
    });
}

#[inline]
fn answer_keep(buf: &[u8], out: &mut Vec<u8>) -> Result<Option<usize>, ProtocolError> {
    let (frame, len) = match Frame::decode(buf).map_err(|_| ProtocolError::Malformed)? {
        KeepDecoded::Complete { frame, len } => (frame, len),
        KeepDecoded::Incomplete { .. } => return Ok(None),
    };
    let Ok(request) = KeepRequest::parse(&frame) else {
        Response::encode_status(out, frame.id, KeepStatus::BadRequest);
        return Ok(Some(len));
    };
    match request.command {
        Command::Get { .. } => {
            Response::encode_value(out, request.id, Value::new(b"1").unwrap_or_default(), 0)
        }
        Command::Incr { .. } => Response::encode_counter(out, request.id, 1),
        Command::Hello { .. } => {
            Response::encode_value(out, request.id, Value::EMPTY, PROTOCOL_VERSION)
        }
        _ if request.reply == ReplyMode::ErrorsOnly => {}
        _ => Response::encode_status(out, request.id, KeepStatus::Ok),
    }
    Ok(Some(len))
}

pub fn cluster_slots_reply(nodes: &[(String, u16)]) -> Box<[u8]> {
    let slots = crate::protocol::cluster::SLOTS;
    let n = nodes.len().max(1);
    let mut out = Vec::new();
    crate::protocol::resp::array_header(&mut out, nodes.len());
    let mut digits = itoa::Buffer::new();
    for (i, (host, port)) in nodes.iter().enumerate() {
        let start = slots * i / n;
        let end = slots * (i + 1) / n - 1;
        out.extend_from_slice(b"*3\r\n:");
        out.extend_from_slice(digits.format(start).as_bytes());
        out.extend_from_slice(b"\r\n:");
        out.extend_from_slice(digits.format(end).as_bytes());
        out.extend_from_slice(b"\r\n*2\r\n");
        crate::protocol::resp::bulk(&mut out, host.as_bytes());
        out.push(b':');
        out.extend_from_slice(digits.format(*port).as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.into_boxed_slice()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op::{OpKind, Outcome as ClientOutcome, Request};
    use crate::protocol::Protocol;
    use crate::protocol::resp::{LeaseRelease, LeaseRenew, Resp, RespMapping};
    use crate::setup;

    fn requests() -> Vec<Request<'static>> {
        OpKind::ALL
            .iter()
            .filter(|&&kind| kind != OpKind::DeletePrefix)
            .map(|&kind| Request {
                kind,
                key: b"key:1",
                value: if kind.carries_value() {
                    b"12345678"
                } else {
                    b""
                },
                ttl: 60,
                delta: 1,
                batch: 1,
                token: 42,
            })
            .collect()
    }

    fn round_trip(protocol: &Protocol, server: ServerProtocol) {
        let mut framer = Framer::new(server, Box::default());
        for (i, req) in requests().iter().enumerate() {
            if !protocol.supports(req.kind, false) {
                continue;
            }
            let mut wire = Vec::new();
            let parts = protocol.encode(req, i as u32, &mut wire).expect("encodes");
            let mut replies = Vec::new();
            let mut pos = 0;
            while pos < wire.len() {
                pos += framer
                    .answer(&wire[pos..], &mut replies)
                    .expect("valid request")
                    .expect("complete request");
            }
            let mut rpos = 0;
            for part in 0..parts {
                let reply = protocol
                    .decode(&replies[rpos..], Some((req.kind, part + 1 == parts)))
                    .expect("valid reply")
                    .expect("complete reply");
                assert_ne!(reply.outcome, ClientOutcome::Error, "{:?}", req.kind);
                rpos += reply.len;
            }
            assert_eq!(rpos, replies.len());
        }
    }

    // Every client encoding is framed and answered by the null server, and the answers decode.
    #[test]
    fn client_and_null_server_agree() {
        for mapping in [RespMapping::Stalwart, RespMapping::Native] {
            let protocol = Protocol::Resp(Resp {
                mapping,
                script_sha: FAKE_SHA[5..45].to_vec().into(),
                lease_renew_sha: FAKE_SHA[5..45].to_vec().into(),
                lease_release_sha: FAKE_SHA[5..45].to_vec().into(),
                lease_renew: LeaseRenew::Script,
                lease_release: LeaseRelease::Script,
            });
            round_trip(&protocol, ServerProtocol::Resp);
        }
        for (lease_renew, lease_release) in [
            (LeaseRenew::SetIfEq, LeaseRelease::DelIfEq),
            (LeaseRenew::SetIfEq, LeaseRelease::DelexIfEq),
        ] {
            let protocol = Protocol::Resp(Resp {
                mapping: RespMapping::Native,
                script_sha: FAKE_SHA[5..45].to_vec().into(),
                lease_renew_sha: FAKE_SHA[5..45].to_vec().into(),
                lease_release_sha: FAKE_SHA[5..45].to_vec().into(),
                lease_renew,
                lease_release,
            });
            round_trip(&protocol, ServerProtocol::Resp);
        }
        round_trip(&Protocol::Meta, ServerProtocol::Meta);
        round_trip(&Protocol::Keep, ServerProtocol::Keep);
    }

    // The fake CLUSTER SLOTS reply parses into a full slot map.
    #[test]
    fn cluster_slots_parse() {
        let reply = cluster_slots_reply(&[
            (String::from("10.0.0.1"), 7000),
            (String::from("10.0.0.2"), 7001),
        ]);
        let (value, len) = setup::parse_owned(&reply).expect("ok").expect("complete");
        assert_eq!(len, reply.len());
        let setup::Value::Array(entries) = value else {
            panic!("array expected");
        };
        assert_eq!(entries.len(), 2);
    }
}
