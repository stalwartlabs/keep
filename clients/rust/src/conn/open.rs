/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use keep_proto::{
    ClientInfo, ClusterSecret, Command, Decoded, Frame, HEADER_LEN, Nonce, Nonces,
    PROTOCOL_VERSION, ReplyMode, Request, Response, ServerFrame, ServerInfo, Side, SoftwareName,
    Status, Value,
};
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::socket::{self, Sent, Socket};
use super::tls::{Connector, TlsSetup};
use crate::error::Error;

const CLIENT_NAME: &str = concat!("keep-client ", env!("CARGO_PKG_VERSION"));
const TCP_CONNECT: Duration = Duration::from_secs(1);
const TLS_ALERT: u8 = 0x15;
const TLS_HANDSHAKE: u8 = 0x16;
const TLS_MAJOR_VERSION: u8 = 0x03;

#[derive(Clone)]
pub(crate) struct Opener {
    pub rto_min: Option<Duration>,
    pub user_timeout: Option<Duration>,
    pub connect_timeout: Duration,
    pub tls: Option<Connector>,
    pub secret: Option<ClusterSecret>,
}

#[derive(Clone)]
pub(crate) struct Openers {
    pub base: Opener,
    pub tls: Option<TlsSetup>,
}

impl Openers {
    pub fn for_host(&self, host: &str) -> Result<Opener, Error> {
        let mut opener = self.base.clone();
        if let Some(tls) = &self.tls {
            opener.tls = Some(tls.connector(host)?);
        }
        Ok(opener)
    }

    pub fn for_addr(&self, addr: SocketAddr) -> Result<Opener, Error> {
        self.for_host(&addr.ip().to_string())
    }
}

pub(crate) struct Handshake {
    pub socket: Socket,
    pub info: ServerInfo,
}

fn connect_error(err: io::Error) -> Error {
    Error::Connect(err.kind())
}

fn handshake_error(reason: impl ToString) -> Error {
    Error::Handshake(reason.to_string().into_boxed_str())
}

impl Opener {
    pub async fn open(&self, addr: SocketAddr) -> Result<Handshake, Error> {
        timeout(self.connect_timeout, self.establish(addr))
            .await
            .unwrap_or(Err(Error::Connect(io::ErrorKind::TimedOut)))
    }

    async fn establish(&self, addr: SocketAddr) -> Result<Handshake, Error> {
        let stream = timeout(TCP_CONNECT, TcpStream::connect(addr))
            .await
            .unwrap_or_else(|_| Err(io::ErrorKind::TimedOut.into()))
            .map_err(connect_error)?;
        socket::tune(&stream, self.rto_min, self.user_timeout).map_err(connect_error)?;
        let session = match &self.tls {
            Some(tls) => Some(tls.handshake(&stream).await?),
            None => None,
        };
        let socket = Socket::new(stream, session);
        let auth = match (self.secret, &self.tls) {
            (Some(secret), None) => {
                let mut nonce = [0; Nonce::LEN];
                getrandom::fill(&mut nonce)
                    .map_err(|_| handshake_error("no randomness for the nonce"))?;
                Some((secret, Nonce(nonce)))
            }
            _ => None,
        };
        let mut info = Vec::new();
        ClientInfo {
            name: SoftwareName::new(CLIENT_NAME),
            node: None,
            extensions: 0,
            nonce: auth.map(|(_, nonce)| nonce),
        }
        .encode(&mut info);
        let hello = Command::Hello {
            version: PROTOCOL_VERSION,
            info: Value::new(&info).ok_or_else(|| handshake_error("client info too long"))?,
        };
        let mut buf = Vec::new();
        let (arg, value) = match exchange(&socket, &hello, &mut buf).await {
            Ok(reply) => reply,
            Err(_)
                if self.tls.is_none()
                    && matches!(
                        buf.as_slice(),
                        [TLS_ALERT | TLS_HANDSHAKE, TLS_MAJOR_VERSION, ..]
                    ) =>
            {
                return Err(handshake_error("server requires TLS"));
            }
            Err(err) => return Err(err),
        };
        if arg > PROTOCOL_VERSION {
            return Err(handshake_error(format!("server chose version {arg}")));
        }
        let info = ServerInfo::parse(&value).map_err(handshake_error)?;
        match (info.challenge, auth, &self.tls) {
            (Some(challenge), Some((secret, client)), _) => {
                let nonces = Nonces {
                    client,
                    server: challenge.nonce,
                };
                if !secret.verify(Side::Server, &nonces, &challenge.proof.0) {
                    return Err(handshake_error("server proof does not match the secret"));
                }
                let proof = secret.proof(Side::Client, &nonces);
                exchange(&socket, &Command::Auth { proof: &proof.0 }, &mut buf).await?;
            }
            (None, Some(_), _) => {
                return Err(handshake_error("server did not prove the cluster secret"));
            }
            (Some(_), None, Some(_)) => {
                return Err(handshake_error(
                    "server sent a plaintext challenge on a TLS connection",
                ));
            }
            (Some(_), None, None) => {
                return Err(handshake_error(
                    "server requires authentication and no secret is configured",
                ));
            }
            (None, None, _) => {}
        }
        Ok(Handshake { socket, info })
    }
}

async fn exchange(
    socket: &Socket,
    command: &Command<'_>,
    buf: &mut Vec<u8>,
) -> Result<(u32, Vec<u8>), Error> {
    buf.clear();
    Request {
        id: 0,
        reply: ReplyMode::Always,
        command: *command,
    }
    .encode(buf);
    let mut written = 0;
    while let Some(rest) = buf.get(written..).filter(|rest| !rest.is_empty()) {
        match socket.send(rest).map_err(connect_error)? {
            Sent::All => break,
            Sent::Blocked(accepted) => {
                written += accepted;
                socket.writable().await.map_err(connect_error)?;
            }
        }
    }
    read_frame(socket, buf).await?;
    let Ok(Decoded::Complete { frame, .. }) = Frame::decode(buf) else {
        return Err(handshake_error("incomplete reply"));
    };
    match ServerFrame::parse(&frame).map_err(handshake_error)? {
        ServerFrame::Response(Response {
            status: Status::Ok,
            arg,
            value,
            ..
        }) => Ok((arg, value.to_vec())),
        ServerFrame::Response(response) => Err(handshake_error(format!(
            "{:?}: {}",
            response.status,
            String::from_utf8_lossy(response.value.as_bytes())
        ))),
        ServerFrame::GoAway { status, reason } => Err(handshake_error(format!(
            "goaway {status:?}: {}",
            String::from_utf8_lossy(reason.as_bytes())
        ))),
        _ => Err(handshake_error("push before the reply")),
    }
}

async fn read_frame(socket: &Socket, buf: &mut Vec<u8>) -> Result<(), Error> {
    buf.clear();
    buf.resize(HEADER_LEN, 0);
    let mut filled = 0;
    loop {
        let Some(spare) = buf.get_mut(filled..).filter(|spare| !spare.is_empty()) else {
            match Frame::decode(buf).map_err(handshake_error)? {
                Decoded::Complete { .. } => return Ok(()),
                Decoded::Incomplete { needed } => {
                    buf.resize(needed, 0);
                    continue;
                }
            }
        };
        match socket.try_read(spare) {
            Ok(0) => return Err(Error::Connect(io::ErrorKind::UnexpectedEof)),
            Ok(read) => filled += read,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                socket.readable().await.map_err(connect_error)?;
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) if err.kind() == io::ErrorKind::InvalidData => {
                return Err(Error::Tls(err.to_string().into_boxed_str()));
            }
            Err(err) => return Err(connect_error(err)),
        }
    }
}
