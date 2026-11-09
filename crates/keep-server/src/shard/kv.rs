/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use keep_engine::{
    Gate, IncrError, KeepEngine, Key as EngineKey, LockOutcome, SetMode as EngineSetMode,
    SetOutcome, Time, Token, TokenOutcome, Ttl as EngineTtl,
};
use keep_proto::{Command, ReplyMode, Request, Response, SetMode, Status, Ttl, TtlReport, Value};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Limits {
    pub max_lock_ttl: Ttl,
    pub max_value: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Handled {
    Replied,
    NotKeyed,
}

#[inline]
pub fn key_of<'a>(command: &Command<'a>) -> Option<&'a [u8]> {
    match command {
        Command::Get { key, .. }
        | Command::Exists { key }
        | Command::Set { key, .. }
        | Command::Delete { key }
        | Command::Incr { key, .. }
        | Command::TryLock { key, .. }
        | Command::Unlock { key, .. }
        | Command::Renew { key, .. } => Some(key.as_bytes()),
        Command::DeletePrefix { .. }
        | Command::Publish { .. }
        | Command::Subscribe { .. }
        | Command::Unsubscribe { .. }
        | Command::Hello { .. }
        | Command::Ping
        | Command::Topology
        | Command::Auth { .. }
        | Command::Leave => None,
    }
}

#[derive(Clone, Copy)]
pub(super) struct Reply {
    id: u32,
    quiet: bool,
}

impl Reply {
    #[inline]
    pub(super) fn to(request: &Request<'_>) -> Reply {
        Reply {
            id: request.id,
            quiet: request.reply == ReplyMode::ErrorsOnly,
        }
    }

    #[inline]
    pub(super) fn status(self, out: &mut Vec<u8>, status: Status) {
        if !self.quiet || status.is_error() {
            Response::encode_status(out, self.id, status);
        }
    }

    #[inline]
    pub(super) fn value(self, out: &mut Vec<u8>, value: &[u8], arg: u32) {
        match Value::new(value) {
            Some(value) if !self.quiet => Response::encode_value(out, self.id, value, arg),
            Some(_) => {}
            None => self.status(out, Status::Internal),
        }
    }

    #[inline]
    fn counter(self, out: &mut Vec<u8>, counter: i64) {
        if !self.quiet {
            Response::encode_counter(out, self.id, counter);
        }
    }
}

#[inline]
fn engine_ttl(ttl: Ttl) -> Option<EngineTtl> {
    EngineTtl::from_secs(ttl.secs())
}

fn found(found: bool) -> Status {
    if found { Status::Ok } else { Status::NotFound }
}

fn token_status(outcome: TokenOutcome) -> Status {
    match outcome {
        TokenOutcome::Done => Status::Ok,
        TokenOutcome::NotFound => Status::NotFound,
        TokenOutcome::Conflict => Status::Conflict,
    }
}

#[inline]
pub fn execute(
    engine: &mut KeepEngine,
    request: &Request<'_>,
    now: Time,
    gate: Gate,
    limits: Limits,
    out: &mut Vec<u8>,
) -> Handled {
    let reply = Reply::to(request);
    let Some(key) = key_of(&request.command) else {
        return Handled::NotKeyed;
    };
    let Some(key) = EngineKey::new(key) else {
        reply.status(out, Status::BadRequest);
        return Handled::Replied;
    };
    let lock_ttl = |ttl: Ttl| {
        (ttl.secs() <= limits.max_lock_ttl.secs())
            .then(|| engine_ttl(ttl))
            .flatten()
    };
    match request.command {
        Command::Get { ttl, .. } => match engine.get(key, now) {
            Some(hit) => {
                let arg = match ttl {
                    TtlReport::Include => hit.deadline.reported(now).map_or(0, EngineTtl::secs),
                    TtlReport::Omit => 0,
                };
                reply.value(out, hit.value, arg);
            }
            None => reply.status(out, Status::NotFound),
        },
        Command::Exists { .. } => reply.status(out, found(engine.exists(key, now))),
        Command::Set {
            value, ttl, mode, ..
        } => {
            if value.len() > limits.max_value {
                reply.status(out, Status::TooLarge);
                return Handled::Replied;
            }
            let mode = match mode {
                SetMode::Always => EngineSetMode::Always,
                SetMode::IfAbsent => EngineSetMode::IfAbsent,
            };
            let status =
                match engine.set(key, value.as_bytes(), ttl.and_then(engine_ttl), mode, now) {
                    SetOutcome::Stored => Status::Ok,
                    SetOutcome::Exists => Status::Conflict,
                    SetOutcome::Full => Status::Full,
                };
            reply.status(out, status);
        }
        Command::Delete { .. } => reply.status(out, found(engine.delete(key, now))),
        Command::Incr { delta, ttl, .. } => {
            match engine.incr(key, delta, ttl.and_then(engine_ttl), now) {
                Ok(counter) => reply.counter(out, counter),
                Err(IncrError::WrongType) => reply.status(out, Status::WrongType),
                Err(IncrError::Overflow) => reply.status(out, Status::Overflow),
                Err(IncrError::Full) => reply.status(out, Status::Full),
            }
        }
        Command::TryLock { ttl, token, .. } => {
            let Some(ttl) = lock_ttl(ttl) else {
                reply.status(out, Status::BadRequest);
                return Handled::Replied;
            };
            let status = match engine.try_lock(key, Token::new(*token.as_bytes()), ttl, now, gate) {
                LockOutcome::Granted => Status::Ok,
                LockOutcome::Held => Status::Conflict,
                LockOutcome::Full => Status::Full,
            };
            reply.status(out, status);
        }
        Command::Unlock { token, .. } => {
            let outcome = engine.unlock(key, Token::new(*token.as_bytes()), now);
            reply.status(out, token_status(outcome));
        }
        Command::Renew { ttl, token, .. } => {
            let Some(ttl) = lock_ttl(ttl) else {
                reply.status(out, Status::BadRequest);
                return Handled::Replied;
            };
            let status = match token {
                Some(token) => {
                    token_status(engine.renew_lock(key, Token::new(*token.as_bytes()), ttl, now))
                }
                None => found(engine.renew(key, ttl, now)),
            };
            reply.status(out, status);
        }
        Command::DeletePrefix { .. }
        | Command::Publish { .. }
        | Command::Subscribe { .. }
        | Command::Unsubscribe { .. }
        | Command::Hello { .. }
        | Command::Ping
        | Command::Topology
        | Command::Auth { .. }
        | Command::Leave => return Handled::NotKeyed,
    }
    Handled::Replied
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-server/shard/kv.rs"]
mod tests;
