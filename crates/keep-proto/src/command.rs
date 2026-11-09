/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::fmt;

use crate::frame::Frame;
use crate::header::put_frame;
use crate::limits::MAX_HELLO_LEN;
use crate::op::Op;
use crate::types::{Flags, Key, LockToken, Topic, Ttl, Value};

/// Length of an Auth proof.
pub const PROOF_LEN: usize = 32;

/// A validated request.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Request<'a> {
    /// The request id the response echoes.
    pub id: u32,
    /// Whether a successful request gets a response.
    pub reply: ReplyMode,
    /// The operation and its arguments.
    pub command: Command<'a>,
}

/// Whether a successful request gets a response.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReplyMode {
    /// Every request gets a response.
    Always,
    /// Only failed requests get a response (QUIET).
    ErrorsOnly,
}

/// How `Set` treats an existing key.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SetMode {
    /// Overwrite any existing value.
    Always,
    /// Store only when the key is absent.
    IfAbsent,
}

/// Whether a `Get` response carries the remaining TTL.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TtlReport {
    /// Return the value only.
    Omit,
    /// Return the remaining TTL too (WITH_TTL).
    Include,
}

/// Request content.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Command<'a> {
    /// Read a key.
    Get {
        /// The key to read.
        key: Key<'a>,
        /// Whether to report the remaining TTL.
        ttl: TtlReport,
    },
    /// Test whether a key holds a live value.
    Exists {
        /// The key to test.
        key: Key<'a>,
    },
    /// Store a value.
    Set {
        /// The key to store.
        key: Key<'a>,
        /// The value to store.
        value: Value<'a>,
        /// Expiry, or `None` to keep the key until deleted.
        ttl: Option<Ttl>,
        /// Whether an existing key is overwritten.
        mode: SetMode,
    },
    /// Delete a key.
    Delete {
        /// The key to delete.
        key: Key<'a>,
    },
    /// Add to a counter, creating it at 0.
    Incr {
        /// The counter key.
        key: Key<'a>,
        /// The amount to add, possibly negative.
        delta: i64,
        /// Expiry set when the counter is created.
        ttl: Option<Ttl>,
    },
    /// Acquire a lock for a token.
    TryLock {
        /// The lock key.
        key: Key<'a>,
        /// How long the lock lives.
        ttl: Ttl,
        /// The holder's token.
        token: LockToken,
    },
    /// Release a lock held by a token.
    Unlock {
        /// The lock key.
        key: Key<'a>,
        /// The holder's token.
        token: LockToken,
    },
    /// Extend a key's expiry.
    Renew {
        /// The key to extend.
        key: Key<'a>,
        /// The new expiry from now.
        ttl: Ttl,
        /// Extend only while this token holds the key.
        token: Option<LockToken>,
    },
    /// Delete every key under a prefix.
    DeletePrefix {
        /// The prefix.
        prefix: Key<'a>,
    },
    /// Publish an event.
    Publish {
        /// The topic.
        topic: Topic,
        /// The event payload.
        payload: Value<'a>,
    },
    /// Subscribe to a topic on this connection.
    Subscribe {
        /// The topic.
        topic: Topic,
    },
    /// End a subscription on this connection.
    Unsubscribe {
        /// The topic.
        topic: Topic,
    },
    /// Open the session and exchange infos.
    Hello {
        /// The protocol version the client speaks.
        version: u32,
        /// The encoded client info.
        info: Value<'a>,
    },
    /// Round-trip with no effect.
    Ping,
    /// Fetch the cluster topology.
    Topology,
    /// Authenticate with a proof over the server challenge.
    Auth {
        /// The proof.
        proof: &'a [u8; PROOF_LEN],
    },
    /// Ask the server to leave the cluster.
    Leave,
}

const _: () = assert!(size_of::<Command<'static>>() <= 48);

/// A well framed request whose content is invalid. Answered with
/// `BadRequest`; the connection stays usable, except after a bad Hello.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CommandError {
    /// No op has this code.
    UnknownOp(u8),
    /// The op does not accept these flags.
    UnsupportedFlags {
        /// The op.
        op: Op,
        /// The flag bits it does not accept.
        flags: u8,
    },
    /// The op needs a key.
    MissingKey(Op),
    /// The op takes no key.
    UnexpectedKey(Op),
    /// The op takes no value.
    UnexpectedValue(Op),
    /// The op takes no argument.
    UnexpectedArg(Op),
    /// The op needs a TTL.
    MissingTtl(Op),
    /// An Incr delta of this length; it must be 8 bytes.
    BadDelta(usize),
    /// A lock token of this length; it must be 8 bytes.
    BadToken(usize),
    /// A topic id above `0xFFFF`.
    BadTopic(u32),
    /// An Auth proof of this length; it must be 32 bytes.
    BadProof(usize),
    /// A Hello value of this length is too long.
    HelloTooLong(usize),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::UnknownOp(code) => write!(f, "unknown op 0x{code:02x}"),
            CommandError::UnsupportedFlags { op, flags } => {
                write!(f, "flags 0x{flags:02x} not supported by {op:?}")
            }
            CommandError::MissingKey(op) => write!(f, "{op:?} needs a key"),
            CommandError::UnexpectedKey(op) => write!(f, "{op:?} takes no key"),
            CommandError::UnexpectedValue(op) => write!(f, "{op:?} takes no value"),
            CommandError::UnexpectedArg(op) => write!(f, "{op:?} takes no arg"),
            CommandError::MissingTtl(op) => write!(f, "{op:?} needs a TTL"),
            CommandError::BadDelta(len) => write!(f, "delta must be 8 bytes, got {len}"),
            CommandError::BadToken(len) => write!(f, "lock token must be 8 bytes, got {len}"),
            CommandError::BadTopic(topic) => write!(f, "topic {topic} out of range"),
            CommandError::BadProof(len) => write!(f, "auth proof must be 32 bytes, got {len}"),
            CommandError::HelloTooLong(len) => write!(f, "hello value of {len} bytes too long"),
        }
    }
}

impl std::error::Error for CommandError {}

impl CommandError {
    /// Whether the error closes the connection: the server answers it with `GoAway`
    /// and `BadRequest`, while other errors get `BadRequest` and keep the connection.
    pub fn is_fatal(&self) -> bool {
        match *self {
            CommandError::UnknownOp(code) => code >= 0x80,
            CommandError::HelloTooLong(_) => true,
            CommandError::UnsupportedFlags { op, .. }
            | CommandError::UnexpectedKey(op)
            | CommandError::UnexpectedValue(op)
            | CommandError::UnexpectedArg(op)
            | CommandError::MissingKey(op)
            | CommandError::MissingTtl(op) => op == Op::Hello,
            CommandError::BadDelta(_)
            | CommandError::BadToken(_)
            | CommandError::BadTopic(_)
            | CommandError::BadProof(_) => false,
        }
    }
}

impl Command<'_> {
    /// The op of this command.
    #[inline]
    pub fn op(&self) -> Op {
        match self {
            Command::Get { .. } => Op::Get,
            Command::Exists { .. } => Op::Exists,
            Command::Set { .. } => Op::Set,
            Command::Delete { .. } => Op::Delete,
            Command::Incr { .. } => Op::Incr,
            Command::TryLock { .. } => Op::TryLock,
            Command::Unlock { .. } => Op::Unlock,
            Command::Renew { .. } => Op::Renew,
            Command::DeletePrefix { .. } => Op::DeletePrefix,
            Command::Publish { .. } => Op::Publish,
            Command::Subscribe { .. } => Op::Subscribe,
            Command::Unsubscribe { .. } => Op::Unsubscribe,
            Command::Hello { .. } => Op::Hello,
            Command::Ping => Op::Ping,
            Command::Topology => Op::Topology,
            Command::Auth { .. } => Op::Auth,
            Command::Leave => Op::Leave,
        }
    }
}

impl<'a> Request<'a> {
    /// Validates a frame into a request.
    #[inline]
    pub fn parse(frame: &Frame<'a>) -> Result<Request<'a>, CommandError> {
        let op = Op::from_code(frame.code).ok_or(CommandError::UnknownOp(frame.code))?;
        let flags = Flags(frame.flags);
        if flags.without(op.allowed_flags()) != Flags::NONE {
            return Err(CommandError::UnsupportedFlags {
                op,
                flags: frame.flags,
            });
        }
        let fields = Fields { op, frame };
        let command = match op {
            Op::Get => {
                fields.no_value()?.no_arg()?;
                Command::Get {
                    key: fields.key()?,
                    ttl: if flags.contains(Flags::WITH_TTL) {
                        TtlReport::Include
                    } else {
                        TtlReport::Omit
                    },
                }
            }
            Op::Exists => {
                fields.no_value()?.no_arg()?;
                Command::Exists { key: fields.key()? }
            }
            Op::Set => Command::Set {
                key: fields.key()?,
                value: Value::framed(frame.value),
                ttl: Ttl::from_secs(frame.arg),
                mode: if flags.contains(Flags::IF_ABSENT) {
                    SetMode::IfAbsent
                } else {
                    SetMode::Always
                },
            },
            Op::Delete => {
                fields.no_value()?.no_arg()?;
                Command::Delete { key: fields.key()? }
            }
            Op::Incr => {
                let delta = <[u8; 8]>::try_from(frame.value)
                    .map_err(|_| CommandError::BadDelta(frame.value.len()))?;
                Command::Incr {
                    key: fields.key()?,
                    delta: i64::from_le_bytes(delta),
                    ttl: Ttl::from_secs(frame.arg),
                }
            }
            Op::TryLock => Command::TryLock {
                key: fields.key()?,
                ttl: fields.ttl()?,
                token: fields.token()?,
            },
            Op::Unlock => {
                fields.no_arg()?;
                Command::Unlock {
                    key: fields.key()?,
                    token: fields.token()?,
                }
            }
            Op::Renew => Command::Renew {
                key: fields.key()?,
                ttl: fields.ttl()?,
                token: match frame.value {
                    [] => None,
                    _ => Some(fields.token()?),
                },
            },
            Op::DeletePrefix => {
                fields.no_value()?.no_arg()?;
                Command::DeletePrefix {
                    prefix: fields.key()?,
                }
            }
            Op::Publish => {
                fields.no_key()?;
                Command::Publish {
                    topic: fields.topic()?,
                    payload: Value::framed(frame.value),
                }
            }
            Op::Subscribe => {
                fields.no_key()?.no_value()?;
                Command::Subscribe {
                    topic: fields.topic()?,
                }
            }
            Op::Unsubscribe => {
                fields.no_key()?.no_value()?;
                Command::Unsubscribe {
                    topic: fields.topic()?,
                }
            }
            Op::Hello => {
                fields.no_key()?;
                if frame.value.len() > MAX_HELLO_LEN {
                    return Err(CommandError::HelloTooLong(frame.value.len()));
                }
                Command::Hello {
                    version: frame.arg,
                    info: Value::framed(frame.value),
                }
            }
            Op::Ping => {
                fields.no_key()?.no_value()?.no_arg()?;
                Command::Ping
            }
            Op::Topology => {
                fields.no_key()?.no_value()?.no_arg()?;
                Command::Topology
            }
            Op::Auth => {
                fields.no_key()?.no_arg()?;
                Command::Auth {
                    proof: <&[u8; PROOF_LEN]>::try_from(frame.value)
                        .map_err(|_| CommandError::BadProof(frame.value.len()))?,
                }
            }
            Op::Leave => {
                fields.no_key()?.no_value()?.no_arg()?;
                Command::Leave
            }
        };
        Ok(Request {
            id: frame.id,
            reply: if flags.contains(Flags::QUIET) {
                ReplyMode::ErrorsOnly
            } else {
                ReplyMode::Always
            },
            command,
        })
    }

    /// Appends the request as a frame to `out`.
    #[inline]
    pub fn encode(&self, out: &mut Vec<u8>) {
        let quiet = Flags::QUIET.when(self.reply == ReplyMode::ErrorsOnly);
        let delta: [u8; 8];
        let (op, flags, key, value, arg): (Op, Flags, &[u8], &[u8], u32) = match &self.command {
            Command::Get { key, ttl } => (
                Op::Get,
                Flags::WITH_TTL.when(*ttl == TtlReport::Include),
                key,
                &[],
                0,
            ),
            Command::Exists { key } => (Op::Exists, Flags::NONE, key, &[], 0),
            Command::Set {
                key,
                value,
                ttl,
                mode,
            } => (
                Op::Set,
                Flags::IF_ABSENT.when(*mode == SetMode::IfAbsent),
                key,
                value,
                Ttl::wire(*ttl),
            ),
            Command::Delete { key } => (Op::Delete, Flags::NONE, key, &[], 0),
            Command::Incr {
                key,
                delta: by,
                ttl,
            } => {
                delta = by.to_le_bytes();
                (Op::Incr, Flags::NONE, key, &delta, Ttl::wire(*ttl))
            }
            Command::TryLock { key, ttl, token } => {
                (Op::TryLock, Flags::NONE, key, token.as_bytes(), ttl.secs())
            }
            Command::Unlock { key, token } => (Op::Unlock, Flags::NONE, key, token.as_bytes(), 0),
            Command::Renew { key, ttl, token } => (
                Op::Renew,
                Flags::NONE,
                key,
                token.as_ref().map_or(&[][..], |token| token.as_bytes()),
                ttl.secs(),
            ),
            Command::DeletePrefix { prefix } => (Op::DeletePrefix, Flags::NONE, prefix, &[], 0),
            Command::Publish { topic, payload } => {
                (Op::Publish, Flags::NONE, &[], payload, topic.wire())
            }
            Command::Subscribe { topic } => (Op::Subscribe, Flags::NONE, &[], &[], topic.wire()),
            Command::Unsubscribe { topic } => {
                (Op::Unsubscribe, Flags::NONE, &[], &[], topic.wire())
            }
            Command::Hello { version, info } => (Op::Hello, Flags::NONE, &[], info, *version),
            Command::Ping => (Op::Ping, Flags::NONE, &[], &[], 0),
            Command::Topology => (Op::Topology, Flags::NONE, &[], &[], 0),
            Command::Auth { proof } => (Op::Auth, Flags::NONE, &[], *proof, 0),
            Command::Leave => (Op::Leave, Flags::NONE, &[], &[], 0),
        };
        let flags = match op {
            Op::Hello | Op::Auth => flags,
            _ => flags.union(quiet),
        };
        put_frame(out, self.id, op as u8, flags.0, arg, key, value);
    }
}

struct Fields<'f, 'a> {
    op: Op,
    frame: &'f Frame<'a>,
}

impl<'a> Fields<'_, 'a> {
    #[inline]
    fn key(&self) -> Result<Key<'a>, CommandError> {
        Key::new(self.frame.key).ok_or(CommandError::MissingKey(self.op))
    }

    #[inline]
    fn token(&self) -> Result<LockToken, CommandError> {
        LockToken::new(self.frame.value).ok_or(CommandError::BadToken(self.frame.value.len()))
    }

    #[inline]
    fn ttl(&self) -> Result<Ttl, CommandError> {
        Ttl::from_secs(self.frame.arg).ok_or(CommandError::MissingTtl(self.op))
    }

    #[inline]
    fn topic(&self) -> Result<Topic, CommandError> {
        Topic::new(self.frame.arg).ok_or(CommandError::BadTopic(self.frame.arg))
    }

    #[inline]
    fn no_key(&self) -> Result<&Self, CommandError> {
        if self.frame.key.is_empty() {
            Ok(self)
        } else {
            Err(CommandError::UnexpectedKey(self.op))
        }
    }

    #[inline]
    fn no_value(&self) -> Result<&Self, CommandError> {
        if self.frame.value.is_empty() {
            Ok(self)
        } else {
            Err(CommandError::UnexpectedValue(self.op))
        }
    }

    #[inline]
    fn no_arg(&self) -> Result<&Self, CommandError> {
        if self.frame.arg == 0 {
            Ok(self)
        } else {
            Err(CommandError::UnexpectedArg(self.op))
        }
    }
}
