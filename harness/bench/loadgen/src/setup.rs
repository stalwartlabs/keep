/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt;
use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use keep_proto::{
    ClientInfo, ClusterSecret, Command, Decoded as KeepDecoded, Frame, Nonce, Nonces,
    PROTOCOL_VERSION, ReplyMode, Request, Response, ServerFrame, ServerInfo, Side, SoftwareName,
    Status, Value as KeepValue,
};

use crate::protocol::ProtocolError;
use crate::protocol::cluster::SlotMap;
use crate::protocol::resp::{self, LeaseRelease, LeaseRenew, RespValue};
use crate::tls::Stream;

const SETUP_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_SETUP_REPLY: usize = 16 << 20;
const CLIENT_NAME: &str = "loadgen";

#[derive(Debug)]
pub enum SetupError {
    Io(String, io::Error),
    Protocol(String, ProtocolError),
    Server(String, String),
    Invalid(String),
    Tls(String),
}

impl fmt::Display for SetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetupError::Io(what, e) => write!(f, "{what}: {e}"),
            SetupError::Protocol(what, e) => write!(f, "{what}: {e}"),
            SetupError::Server(what, msg) => write!(f, "{what}: server replied {msg}"),
            SetupError::Invalid(msg) => f.write_str(msg),
            SetupError::Tls(msg) => write!(f, "tls: {msg}"),
        }
    }
}

impl std::error::Error for SetupError {}

pub fn connect(addr: &str) -> Result<TcpStream, SetupError> {
    let io_err = |e| SetupError::Io(format!("connect {addr}"), e);
    let sock_addr = addr
        .to_socket_addrs()
        .map_err(io_err)?
        .next()
        .ok_or_else(|| SetupError::Invalid(format!("{addr} does not resolve")))?;
    let stream = TcpStream::connect_timeout(&sock_addr, SETUP_TIMEOUT).map_err(io_err)?;
    stream.set_nodelay(true).map_err(io_err)?;
    stream
        .set_read_timeout(Some(SETUP_TIMEOUT))
        .map_err(io_err)?;
    stream
        .set_write_timeout(Some(SETUP_TIMEOUT))
        .map_err(io_err)?;
    Ok(stream)
}

pub fn clear_timeouts(stream: &TcpStream) -> io::Result<()> {
    stream.set_read_timeout(None)?;
    stream.set_write_timeout(None)
}

pub fn exchange<T, S: Read + Write>(
    stream: &mut S,
    what: &str,
    request: &[u8],
    mut parse: impl FnMut(&[u8]) -> Result<Option<(T, usize)>, ProtocolError>,
) -> Result<T, SetupError> {
    stream
        .write_all(request)
        .map_err(|e| SetupError::Io(what.into(), e))?;
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    loop {
        if let Some((value, _)) = parse(&buf).map_err(|e| SetupError::Protocol(what.into(), e))? {
            return Ok(value);
        }
        if buf.len() > MAX_SETUP_REPLY {
            return Err(SetupError::Invalid(format!("{what}: reply above 16 MiB")));
        }
        let n = stream
            .read(&mut chunk)
            .map_err(|e| SetupError::Io(what.into(), e))?;
        if n == 0 {
            return Err(SetupError::Invalid(format!("{what}: connection closed")));
        }
        buf.extend_from_slice(chunk.get(..n).unwrap_or_default());
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Int(i64),
    Bulk(Vec<u8>),
    Nil,
    Array(Vec<Value>),
}

pub fn parse_owned(buf: &[u8]) -> Result<Option<(Value, usize)>, ProtocolError> {
    parse_owned_at(buf, 0)
}

fn parse_owned_at(buf: &[u8], depth: u8) -> Result<Option<(Value, usize)>, ProtocolError> {
    let Some((value, len)) = resp::parse_value(buf, depth)? else {
        return Ok(None);
    };
    let line = || {
        buf.get(1..len.saturating_sub(2))
            .map(<[u8]>::to_vec)
            .unwrap_or_default()
    };
    let owned = match value {
        RespValue::Simple => Value::Simple(line()),
        RespValue::Error => Value::Error(line()),
        RespValue::Int(v) => Value::Int(v),
        RespValue::Nil => Value::Nil,
        RespValue::Bulk { start, len } => Value::Bulk(
            buf.get(start..start + len)
                .ok_or(ProtocolError::Malformed)?
                .to_vec(),
        ),
        RespValue::Array { count, start } => {
            let mut pos = start;
            let mut items = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                let rest = buf.get(pos..).ok_or(ProtocolError::Malformed)?;
                let (item, item_len) =
                    parse_owned_at(rest, depth + 1)?.ok_or(ProtocolError::Malformed)?;
                pos += item_len;
                items.push(item);
            }
            Value::Array(items)
        }
    };
    Ok(Some((owned, len)))
}

pub fn resp_command(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    resp::array_header(&mut out, args.len());
    args.iter().for_each(|arg| resp::bulk(&mut out, arg));
    out
}

pub fn resp_call(stream: &mut TcpStream, what: &str, args: &[&[u8]]) -> Result<Value, SetupError> {
    match exchange(stream, what, &resp_command(args), parse_owned)? {
        Value::Error(msg) => Err(SetupError::Server(
            what.into(),
            String::from_utf8_lossy(&msg).into_owned(),
        )),
        value => Ok(value),
    }
}

pub fn load_script(stream: &mut TcpStream, script: &str) -> Result<Box<[u8]>, SetupError> {
    match resp_call(
        stream,
        "SCRIPT LOAD",
        &[b"SCRIPT", b"LOAD", script.as_bytes()],
    )? {
        Value::Bulk(sha) if sha.len() == 40 => Ok(sha.into_boxed_slice()),
        other => Err(SetupError::Invalid(format!(
            "SCRIPT LOAD returned {other:?}"
        ))),
    }
}

pub fn lease_commands(
    stream: &mut TcpStream,
    key: &[u8],
) -> Result<(LeaseRenew, LeaseRelease), SetupError> {
    const PROBE: &[u8] = b"0";
    let renew = match understood(stream, "SET IFEQ", &[b"SET", key, PROBE, b"IFEQ", PROBE])? {
        true => LeaseRenew::SetIfEq,
        false => LeaseRenew::Script,
    };
    let release = if understood(stream, "DELIFEQ", &[b"DELIFEQ", key, PROBE])? {
        LeaseRelease::DelIfEq
    } else if understood(stream, "DELEX IFEQ", &[b"DELEX", key, b"IFEQ", PROBE])? {
        LeaseRelease::DelexIfEq
    } else {
        LeaseRelease::Script
    };
    Ok((renew, release))
}

fn understood(stream: &mut TcpStream, what: &str, args: &[&[u8]]) -> Result<bool, SetupError> {
    match resp_call(stream, what, args) {
        Ok(_) => Ok(true),
        Err(SetupError::Server(..)) => Ok(false),
        Err(err) => Err(err),
    }
}

pub fn auth(stream: &mut TcpStream, credentials: &str) -> Result<(), SetupError> {
    let args: Vec<&[u8]> = match credentials.split_once(':') {
        Some((user, pass)) => vec![b"AUTH", user.as_bytes(), pass.as_bytes()],
        None => vec![b"AUTH", credentials.as_bytes()],
    };
    resp_call(stream, "AUTH", &args).map(|_| ())
}

pub fn cluster_slots(stream: &mut TcpStream, seed: &str) -> Result<SlotMap, SetupError> {
    let Value::Array(entries) = resp_call(stream, "CLUSTER SLOTS", &[b"CLUSTER", b"SLOTS"])? else {
        return Err(SetupError::Invalid(String::from(
            "CLUSTER SLOTS did not return an array",
        )));
    };
    let seed_host = seed.rsplit_once(':').map_or(seed, |(host, _)| host);
    let mut nodes: Vec<String> = Vec::new();
    let mut ranges = Vec::with_capacity(entries.len());
    for entry in &entries {
        let Value::Array(fields) = entry else {
            return Err(SetupError::Invalid(String::from("bad CLUSTER SLOTS entry")));
        };
        let [
            Value::Int(start),
            Value::Int(end),
            Value::Array(primary),
            ..,
        ] = fields.as_slice()
        else {
            return Err(SetupError::Invalid(String::from("bad CLUSTER SLOTS entry")));
        };
        let [Value::Bulk(host), Value::Int(port), ..] = primary.as_slice() else {
            return Err(SetupError::Invalid(String::from("bad CLUSTER SLOTS node")));
        };
        let host = match String::from_utf8_lossy(host).into_owned() {
            h if h.is_empty() || h == "?" => seed_host.to_string(),
            h => h,
        };
        let addr = if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };
        let node = match nodes.iter().position(|n| *n == addr) {
            Some(node) => node,
            None => {
                nodes.push(addr);
                nodes.len() - 1
            }
        };
        let slot =
            |v: i64| usize::try_from(v).map_err(|_| SetupError::Invalid(format!("bad slot {v}")));
        ranges.push((slot(*start)?, slot(*end)?, node));
    }
    SlotMap::new(nodes, &ranges).map_err(SetupError::Invalid)
}

pub fn keep_hello(
    stream: &mut Stream,
    secret: Option<&ClusterSecret>,
) -> Result<ServerInfo, SetupError> {
    let client_nonce = secret.map(|_| random_nonce()).transpose()?;
    let mut info = Vec::new();
    ClientInfo {
        name: SoftwareName::new(CLIENT_NAME),
        nonce: client_nonce,
        ..ClientInfo::default()
    }
    .encode(&mut info);
    let hello = keep_request(Command::Hello {
        version: PROTOCOL_VERSION,
        info: KeepValue::new(&info)
            .ok_or_else(|| SetupError::Invalid(String::from("hello: client info too long")))?,
    });
    let (arg, value) = exchange(stream, "hello", &hello, keep_response)?.ok("hello")?;
    if arg > PROTOCOL_VERSION {
        return Err(SetupError::Invalid(format!(
            "hello: server chose version {arg}"
        )));
    }
    let info = ServerInfo::parse(&value)
        .map_err(|e| SetupError::Invalid(format!("hello: server info: {e}")))?;
    match (info.challenge, secret, client_nonce) {
        (Some(challenge), Some(secret), Some(client)) => {
            let nonces = Nonces {
                client,
                server: challenge.nonce,
            };
            if !secret.verify(Side::Server, &nonces, &challenge.proof.0) {
                return Err(SetupError::Invalid(String::from(
                    "auth: the server proof does not match the cluster secret",
                )));
            }
            let proof = secret.proof(Side::Client, &nonces);
            exchange(
                stream,
                "auth",
                &keep_request(Command::Auth { proof: &proof.0 }),
                keep_response,
            )?
            .ok("auth")?;
        }
        (Some(_), _, _) => {
            return Err(SetupError::Invalid(String::from(
                "auth: the server requires plaintext Auth; pass --secret-file or set KEEP_SECRET",
            )));
        }
        (None, _, _) => {}
    }
    Ok(info)
}

fn keep_request(command: Command<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    Request {
        id: 0,
        reply: ReplyMode::Always,
        command,
    }
    .encode(&mut out);
    out
}

enum KeepReply {
    Ok { arg: u32, value: Vec<u8> },
    Refused(String),
}

impl KeepReply {
    fn ok(self, what: &str) -> Result<(u32, Vec<u8>), SetupError> {
        match self {
            KeepReply::Ok { arg, value } => Ok((arg, value)),
            KeepReply::Refused(reason) => Err(SetupError::Server(what.into(), reason)),
        }
    }
}

fn keep_response(buf: &[u8]) -> Result<Option<(KeepReply, usize)>, ProtocolError> {
    let (frame, len) = match Frame::decode(buf).map_err(|_| ProtocolError::Malformed)? {
        KeepDecoded::Complete { frame, len } => (frame, len),
        KeepDecoded::Incomplete { .. } => return Ok(None),
    };
    match ServerFrame::parse(&frame).map_err(|_| ProtocolError::Malformed)? {
        ServerFrame::Response(Response {
            status: Status::Ok,
            arg,
            value,
            ..
        }) => Ok(Some((
            KeepReply::Ok {
                arg,
                value: value.as_bytes().to_vec(),
            },
            len,
        ))),
        ServerFrame::Response(Response { status, value, .. })
        | ServerFrame::GoAway {
            status,
            reason: value,
        } => Ok(Some((
            KeepReply::Refused(format!(
                "{status:?} {}",
                String::from_utf8_lossy(value.as_bytes())
            )),
            len,
        ))),
        _ => Err(ProtocolError::UnexpectedReply),
    }
}

fn random_nonce() -> Result<Nonce, SetupError> {
    let mut nonce = [0u8; Nonce::LEN];
    File::open("/dev/urandom")
        .and_then(|mut source| source.read_exact(&mut nonce))
        .map_err(|e| SetupError::Io(String::from("client nonce"), e))?;
    Ok(Nonce(nonce))
}

#[cfg(test)]
mod tests {
    use super::*;
    use keep_proto::{Challenge, ShardInfo};
    use std::net::TcpListener;
    use std::thread;

    fn read_frame(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Vec<u8> {
        let mut chunk = [0u8; 4096];
        loop {
            if let Ok(KeepDecoded::Complete { len, .. }) = Frame::decode(buf) {
                return buf.drain(..len).collect();
            }
            let n = stream.read(&mut chunk).expect("read");
            assert!(n > 0, "client closed");
            buf.extend_from_slice(&chunk[..n]);
        }
    }

    fn serve_auth(secret: ClusterSecret) -> (String, thread::JoinHandle<bool>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut buf = Vec::new();
            let hello = read_frame(&mut stream, &mut buf);
            let Ok(KeepDecoded::Complete { frame, .. }) = Frame::decode(&hello) else {
                panic!("hello frame");
            };
            let request = Request::parse(&frame).expect("hello");
            let Command::Hello { info, .. } = request.command else {
                panic!("hello expected");
            };
            let client = ClientInfo::parse(info.as_bytes())
                .expect("client info")
                .nonce
                .expect("client nonce");
            let nonces = Nonces {
                client,
                server: Nonce([9; Nonce::LEN]),
            };
            let mut value = Vec::new();
            ServerInfo {
                shard: ShardInfo::new(1, 4),
                challenge: Some(Challenge {
                    nonce: nonces.server,
                    proof: secret.proof(Side::Server, &nonces),
                }),
                ..ServerInfo::default()
            }
            .encode(&mut value);
            let mut out = Vec::new();
            Response::encode_value(
                &mut out,
                request.id,
                KeepValue::new(&value).expect("value"),
                PROTOCOL_VERSION,
            );
            stream.write_all(&out).expect("write");
            let auth = read_frame(&mut stream, &mut buf);
            let Ok(KeepDecoded::Complete { frame, .. }) = Frame::decode(&auth) else {
                panic!("auth frame");
            };
            let request = Request::parse(&frame).expect("auth");
            let Command::Auth { proof } = request.command else {
                panic!("auth expected");
            };
            let accepted = secret.verify(Side::Client, &nonces, proof);
            out.clear();
            Response::encode_status(
                &mut out,
                request.id,
                if accepted {
                    Status::Ok
                } else {
                    Status::Unauthorized
                },
            );
            stream.write_all(&out).expect("write");
            accepted
        });
        (addr, handle)
    }

    // Plaintext Auth: the client sends a nonce, checks the server proof and answers with its own.
    #[test]
    fn plaintext_auth() {
        let secret = ClusterSecret::new(1, 2);
        let (addr, server) = serve_auth(secret);
        let mut stream = Stream::Plain(connect(&addr).expect("connect"));
        let info = keep_hello(&mut stream, Some(&secret)).expect("hello");
        assert_eq!(info.shard.map(|shard| shard.shard()), Some(1));
        assert!(server.join().expect("server"));
    }

    // A server proof made with another secret is refused before Auth is sent.
    #[test]
    fn plaintext_auth_rejects_wrong_server() {
        let (addr, server) = serve_auth(ClusterSecret::new(3, 4));
        let mut stream = Stream::Plain(connect(&addr).expect("connect"));
        assert!(keep_hello(&mut stream, Some(&ClusterSecret::new(1, 2))).is_err());
        drop(stream);
        assert!(server.join().is_err());
    }

    // A server that requires Auth is refused cleanly when no secret is configured.
    #[test]
    fn plaintext_auth_needs_secret() {
        let (addr, server) = serve_auth(ClusterSecret::new(1, 2));
        let mut stream = Stream::Plain(connect(&addr).expect("connect"));
        assert!(keep_hello(&mut stream, None).is_err());
        drop(stream);
        assert!(server.join().is_err());
    }

    // Owned RESP values nest and report their total length.
    #[test]
    fn owned_values() {
        let buf =
            b"*2\r\n*3\r\n:0\r\n:5460\r\n*3\r\n$9\r\n127.0.0.1\r\n:7000\r\n$2\r\nid\r\n+OK\r\n";
        let (value, len) = parse_owned(buf).expect("ok").expect("complete");
        assert_eq!(len, buf.len());
        let Value::Array(items) = value else {
            panic!("array expected");
        };
        assert_eq!(items.get(1), Some(&Value::Simple(b"OK".to_vec())));
    }
}
