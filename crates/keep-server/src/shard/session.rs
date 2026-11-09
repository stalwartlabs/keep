/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use keep_proto::{
    Challenge, ClientInfo, ClusterSecret, Command, Frame, HEADER_LEN, MAX_HELLO_LEN, Nonce, Nonces,
    Op, PROTOCOL_VERSION, Request, Response, ServerFrame, ServerInfo, Side, Status, Value,
};
use keep_rt::clock::Nanos;

use crate::stream::Topics;

pub(super) const PRE_AUTH_FRAME: usize = HEADER_LEN + MAX_HELLO_LEN;

const FIRST_PUSH_CODE: u8 = 0x80;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Stage {
    Hello,
    Auth(Nonces),
    Ready,
    Closing,
    Done,
}

impl Stage {
    #[inline]
    pub(super) fn is_authenticated(self) -> bool {
        match self {
            Stage::Ready | Stage::Closing | Stage::Done => true,
            Stage::Hello | Stage::Auth(_) => false,
        }
    }

    #[inline]
    pub(super) fn frame_limit(self) -> usize {
        if self.is_authenticated() {
            usize::MAX
        } else {
            PRE_AUTH_FRAME
        }
    }
}

#[derive(Debug)]
pub struct Session {
    pub(super) input: Vec<u8>,
    pub(super) stage: Stage,
    pub(super) forwarded: u32,
    pub(super) seen: Nanos,
    pub(super) topics: Topics,
}

impl Session {
    pub(super) fn new(now: Nanos) -> Session {
        Session {
            input: Vec::new(),
            stage: Stage::Hello,
            forwarded: 0,
            seen: now,
            topics: Topics::default(),
        }
    }
}

#[derive(Debug)]
pub(super) struct Fatal;

#[cold]
#[inline(never)]
pub(super) fn go_away(out: &mut Vec<u8>, status: Status, reason: &'static str) -> Fatal {
    let reason = Value::new(reason.as_bytes()).unwrap_or(Value::EMPTY);
    ServerFrame::GoAway { status, reason }.encode(out);
    Fatal
}

enum Auth {
    Challenge(ClusterSecret),
    Tls,
}

pub(super) struct Greeting {
    info: Box<[u8]>,
    auth: Auth,
}

impl Greeting {
    pub(super) fn new(info: &ServerInfo, secret: ClusterSecret) -> Greeting {
        let mut encoded = Vec::new();
        info.encode(&mut encoded);
        Greeting {
            info: encoded.into_boxed_slice(),
            auth: Auth::Challenge(secret),
        }
    }

    pub(super) fn over_tls(&mut self) {
        self.auth = Auth::Tls;
    }

    #[cold]
    #[inline(never)]
    pub(super) fn step(
        &self,
        stage: &mut Stage,
        frame: &Frame<'_>,
        out: &mut Vec<u8>,
    ) -> Result<(), Fatal> {
        match Op::from_code(frame.code) {
            Some(Op::Hello) => self.hello(stage, frame, out),
            Some(Op::Auth) => self.auth(stage, frame, out),
            _ if frame.code >= FIRST_PUSH_CODE => Err(go_away(
                out,
                Status::BadRequest,
                "server frame code from a client",
            )),
            _ => {
                Response::encode_status(out, frame.id, Status::Unauthorized);
                Ok(())
            }
        }
    }

    fn hello(&self, stage: &mut Stage, frame: &Frame<'_>, out: &mut Vec<u8>) -> Result<(), Fatal> {
        if *stage != Stage::Hello {
            Response::encode_status(out, frame.id, Status::BadRequest);
            return Ok(());
        }
        let Ok(Request {
            id,
            command: Command::Hello { version, info },
            ..
        }) = Request::parse(frame)
        else {
            return Err(go_away(out, Status::BadRequest, "malformed hello"));
        };
        if version == 0 {
            return Err(go_away(
                out,
                Status::BadRequest,
                "no common protocol version",
            ));
        }
        let Ok(client) = ClientInfo::parse(info.as_bytes()) else {
            return Err(go_away(out, Status::BadRequest, "malformed hello"));
        };
        let secret = match &self.auth {
            Auth::Challenge(secret) => secret,
            Auth::Tls => {
                let value = Value::new(&self.info).unwrap_or(Value::EMPTY);
                Response::encode_value(out, id, value, version.min(PROTOCOL_VERSION));
                *stage = Stage::Ready;
                return Ok(());
            }
        };
        let Some(client) = client.nonce else {
            return Err(go_away(
                out,
                Status::BadRequest,
                "plaintext mode needs a client nonce",
            ));
        };
        let mut server = [0u8; Nonce::LEN];
        if getrandom::fill(&mut server).is_err() {
            return Err(go_away(out, Status::Internal, "no randomness"));
        }
        let nonces = Nonces {
            client,
            server: Nonce(server),
        };
        let challenge = ServerInfo {
            challenge: Some(Challenge {
                nonce: nonces.server,
                proof: secret.proof(Side::Server, &nonces),
            }),
            ..ServerInfo::default()
        };
        let mut value = Vec::with_capacity(self.info.len() + 64);
        value.extend_from_slice(&self.info);
        challenge.encode(&mut value);
        let value = Value::new(&value).unwrap_or(Value::EMPTY);
        Response::encode_value(out, id, value, version.min(PROTOCOL_VERSION));
        *stage = Stage::Auth(nonces);
        Ok(())
    }

    fn auth(&self, stage: &mut Stage, frame: &Frame<'_>, out: &mut Vec<u8>) -> Result<(), Fatal> {
        let Auth::Challenge(secret) = &self.auth else {
            Response::encode_status(out, frame.id, Status::BadRequest);
            return Ok(());
        };
        let Stage::Auth(nonces) = *stage else {
            Response::encode_status(out, frame.id, Status::Unauthorized);
            return Ok(());
        };
        match Request::parse(frame) {
            Ok(Request {
                id,
                command: Command::Auth { proof },
                ..
            }) if secret.verify(Side::Client, &nonces, proof) => {
                Response::encode_status(out, id, Status::Ok);
                *stage = Stage::Ready;
                Ok(())
            }
            _ => Err(go_away(out, Status::Unauthorized, "authentication failed")),
        }
    }
}
