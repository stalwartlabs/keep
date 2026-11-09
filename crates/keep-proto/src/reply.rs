/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::fmt;

use crate::frame::Frame;
use crate::header::{Header, put_frame};
use crate::limits::HEADER_LEN;
use crate::types::{Topic, Value};

const COUNTER_LEN: usize = size_of::<i64>();

macro_rules! codes {
    ($(#[$meta:meta])* $vis:vis enum $name:ident { $($(#[$vmeta:meta])* $variant:ident = $code:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
        #[repr(u8)]
        $vis enum $name {
            $($(#[$vmeta])* $variant = $code),+
        }

        impl $name {
            /// Every code in order.
            pub const ALL: &'static [$name] = &[$($name::$variant),+];

            /// The variant of wire code `code`.
            #[inline]
            pub fn from_code(code: u8) -> Option<$name> {
                match code {
                    $($code => Some($name::$variant),)+
                    _ => None,
                }
            }
        }
    };
}

codes! {
    /// Response status, `0x00` to `0x7F`. Codes from `0x10` are errors and
    /// may carry a UTF-8 message as the value.
    pub enum Status {
        /// Success.
        Ok = 0x00,
        /// The key or subscription is absent.
        NotFound = 0x01,
        /// The condition failed: the key exists, another holder has it, or the topic is subscribed.
        Conflict = 0x02,
        /// The request is invalid.
        BadRequest = 0x10,
        /// Incr on a value that is not a counter.
        WrongType = 0x11,
        /// Incr past the `i64` range.
        Overflow = 0x12,
        /// A value over the server's limit.
        TooLarge = 0x13,
        /// Not authenticated, or not allowed on this listener.
        Unauthorized = 0x15,
        /// Another member owns the key; the argument carries its index.
        Moved = 0x16,
        /// The server refuses the request for now; the argument carries the scope.
        Busy = 0x17,
        /// The server failed; the request may have executed.
        Internal = 0x18,
        /// Refused at the memory cap, or too many subscriptions on the connection.
        Full = 0x19,
    }
}

codes! {
    /// Server push codes, `0x80` to `0xFF`.
    pub enum Push {
        /// A stream event.
        Event = 0x80,
        /// The server is closing the connection.
        GoAway = 0x81,
        /// An idle subscription's last sequence number.
        Heartbeat = 0x82,
        /// A subscription's final sequence number.
        End = 0x83,
        /// The cluster topology changed.
        TopologyChanged = 0x84,
    }
}

impl Status {
    /// Whether the status is an error.
    #[inline]
    pub fn is_error(self) -> bool {
        self as u8 >= Status::BadRequest as u8
    }

    /// Whether the request certainly did not execute: every error but
    /// `Internal`.
    #[inline]
    pub fn not_executed(self) -> bool {
        self.is_error() && self != Status::Internal
    }
}

/// What a `Busy` refuses, carried in its `arg`: this request only, or every
/// keyed request of the server until its lease is valid again. Unknown
/// values read as `Request`, so a later scope stays compatible.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(u32)]
pub enum BusyScope {
    /// Only this request is refused.
    Request = 0,
    /// Every keyed request of the server is refused.
    Server = 1,
}

impl BusyScope {
    /// Reads the scope from a `Busy` argument.
    #[inline]
    pub fn from_arg(arg: u32) -> BusyScope {
        if arg == BusyScope::Server as u32 {
            BusyScope::Server
        } else {
            BusyScope::Request
        }
    }
}

/// A response to the request with the same id.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Response<'a> {
    /// The request id.
    pub id: u32,
    /// The status.
    pub status: Status,
    /// The status-specific argument.
    pub arg: u32,
    /// The value or error message.
    pub value: Value<'a>,
}

/// A stream event; `seq` increases by one per event on a subscription, so a
/// gap means lost events.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Event<'a> {
    /// The event's sequence number on the subscription.
    pub seq: u32,
    /// The topic.
    pub topic: Topic,
    /// The payload.
    pub payload: Value<'a>,
}

/// The last sequence number sent on a subscription: periodic while idle
/// (`Heartbeat`) or final (`End`), so the loss of trailing events shows.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Mark {
    /// The last sequence number sent.
    pub last_seq: u32,
    /// The topic.
    pub topic: Topic,
}

/// A frame received by the client.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ServerFrame<'a> {
    /// A response to a request.
    Response(Response<'a>),
    /// A stream event.
    Event(Event<'a>),
    /// The server is closing the connection.
    GoAway {
        /// Why the server closes.
        status: Status,
        /// A UTF-8 message, possibly empty.
        reason: Value<'a>,
    },
    /// The subscription is idle at this mark.
    Heartbeat(Mark),
    /// The subscription ended at this mark.
    End(Mark),
    /// The topology changed; `epoch_low` is the low half of the new epoch.
    TopologyChanged {
        /// The low 32 bits of the new epoch.
        epoch_low: u32,
    },
}

/// A server frame the client cannot interpret. Fatal for the connection.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ServerFrameError {
    /// No status or push has this code.
    UnknownCode(u8),
    /// A response carries this unknown status code.
    UnknownStatus(u32),
    /// A server frame carries a key.
    UnexpectedKey(u8),
    /// A server frame carries flags.
    UnexpectedFlags(u8),
    /// A frame of this code carries a value it must not.
    UnexpectedValue(u8),
    /// A push of this code carries a request id.
    UnexpectedId(u8),
    /// An event or mark names a topic above `0xFFFF`.
    BadTopic(u32),
}

impl fmt::Display for ServerFrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerFrameError::UnknownCode(code) => write!(f, "unknown frame code 0x{code:02x}"),
            ServerFrameError::UnknownStatus(code) => write!(f, "unknown status {code}"),
            ServerFrameError::UnexpectedKey(code) => {
                write!(f, "frame code 0x{code:02x} carries a key")
            }
            ServerFrameError::UnexpectedFlags(code) => {
                write!(f, "frame code 0x{code:02x} carries flags")
            }
            ServerFrameError::UnexpectedValue(code) => {
                write!(f, "frame code 0x{code:02x} carries a value")
            }
            ServerFrameError::UnexpectedId(code) => {
                write!(f, "frame code 0x{code:02x} carries an id")
            }
            ServerFrameError::BadTopic(topic) => write!(f, "topic {topic} out of range"),
        }
    }
}

impl std::error::Error for ServerFrameError {}

impl<'a> Response<'a> {
    /// Appends a response that carries only a status.
    #[inline]
    pub fn encode_status(out: &mut Vec<u8>, id: u32, status: Status) {
        put_frame(out, id, status as u8, 0, 0, &[], &[]);
    }

    /// Appends a `Busy` response refusing `scope`.
    #[inline]
    pub fn encode_busy(out: &mut Vec<u8>, id: u32, scope: BusyScope) {
        put_frame(out, id, Status::Busy as u8, 0, scope as u32, &[], &[]);
    }

    /// The scope of a `Busy` response; `None` for any other status.
    #[inline]
    pub fn busy_scope(&self) -> Option<BusyScope> {
        (self.status == Status::Busy).then(|| BusyScope::from_arg(self.arg))
    }

    /// Appends an `Ok` response carrying `value` and `arg`.
    #[inline]
    pub fn encode_value(out: &mut Vec<u8>, id: u32, value: Value<'_>, arg: u32) {
        put_frame(out, id, Status::Ok as u8, 0, arg, &[], value.as_bytes());
    }

    /// Appends the `Ok` response of an Incr.
    #[inline]
    pub fn encode_counter(out: &mut Vec<u8>, id: u32, counter: i64) {
        let header = Header {
            id,
            code: Status::Ok as u8,
            flags: 0,
            key_len: 0,
            ext_words: 0,
            value_len: COUNTER_LEN as u32,
            arg: 0,
        };
        let mut frame = [0; HEADER_LEN + COUNTER_LEN];
        let (head, value) = frame.split_at_mut(HEADER_LEN);
        head.copy_from_slice(&header.encode());
        value.copy_from_slice(&counter.to_le_bytes());
        out.extend_from_slice(&frame);
    }

    /// Appends the response as a frame to `out`.
    #[inline]
    pub fn encode(&self, out: &mut Vec<u8>) {
        put_frame(
            out,
            self.id,
            self.status as u8,
            0,
            self.arg,
            &[],
            self.value.as_bytes(),
        );
    }

    /// The value of an Incr response.
    #[inline]
    pub fn counter(&self) -> Option<i64> {
        <[u8; 8]>::try_from(self.value.as_bytes())
            .ok()
            .map(i64::from_le_bytes)
    }
}

impl Event<'_> {
    /// Appends the event as a frame to `out`.
    #[inline]
    pub fn encode(&self, out: &mut Vec<u8>) {
        put_frame(
            out,
            self.seq,
            Push::Event as u8,
            0,
            self.topic.wire(),
            &[],
            self.payload.as_bytes(),
        );
    }
}

impl<'a> ServerFrame<'a> {
    /// Interprets a frame received by the client.
    #[inline]
    pub fn parse(frame: &Frame<'a>) -> Result<ServerFrame<'a>, ServerFrameError> {
        if !frame.key.is_empty() {
            return Err(ServerFrameError::UnexpectedKey(frame.code));
        }
        if frame.flags != 0 {
            return Err(ServerFrameError::UnexpectedFlags(frame.code));
        }
        let value = Value::framed(frame.value);
        if let Some(status) = Status::from_code(frame.code) {
            return Ok(ServerFrame::Response(Response {
                id: frame.id,
                status,
                arg: frame.arg,
                value,
            }));
        }
        let topic = || Topic::new(frame.arg).ok_or(ServerFrameError::BadTopic(frame.arg));
        let push = Push::from_code(frame.code);
        if matches!(
            push,
            Some(Push::Heartbeat | Push::End | Push::TopologyChanged)
        ) && !frame.value.is_empty()
        {
            return Err(ServerFrameError::UnexpectedValue(frame.code));
        }
        if matches!(push, Some(Push::GoAway | Push::TopologyChanged)) && frame.id != 0 {
            return Err(ServerFrameError::UnexpectedId(frame.code));
        }
        match push {
            Some(Push::Event) => Ok(ServerFrame::Event(Event {
                seq: frame.id,
                topic: topic()?,
                payload: value,
            })),
            Some(Push::Heartbeat) => Ok(ServerFrame::Heartbeat(Mark {
                last_seq: frame.id,
                topic: topic()?,
            })),
            Some(Push::End) => Ok(ServerFrame::End(Mark {
                last_seq: frame.id,
                topic: topic()?,
            })),
            Some(Push::TopologyChanged) => Ok(ServerFrame::TopologyChanged {
                epoch_low: frame.arg,
            }),
            Some(Push::GoAway) => {
                let status = u8::try_from(frame.arg)
                    .ok()
                    .and_then(Status::from_code)
                    .ok_or(ServerFrameError::UnknownStatus(frame.arg))?;
                Ok(ServerFrame::GoAway {
                    status,
                    reason: value,
                })
            }
            None => Err(ServerFrameError::UnknownCode(frame.code)),
        }
    }

    /// Appends the frame to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            ServerFrame::Response(response) => response.encode(out),
            ServerFrame::Event(event) => event.encode(out),
            ServerFrame::GoAway { status, reason } => put_frame(
                out,
                0,
                Push::GoAway as u8,
                0,
                u32::from(*status as u8),
                &[],
                reason.as_bytes(),
            ),
            ServerFrame::Heartbeat(mark) => mark.encode(out, Push::Heartbeat),
            ServerFrame::End(mark) => mark.encode(out, Push::End),
            ServerFrame::TopologyChanged { epoch_low } => {
                put_frame(out, 0, Push::TopologyChanged as u8, 0, *epoch_low, &[], &[])
            }
        }
    }
}

impl Mark {
    fn encode(self, out: &mut Vec<u8>, push: Push) {
        put_frame(
            out,
            self.last_seq,
            push as u8,
            0,
            self.topic.wire(),
            &[],
            &[],
        );
    }
}
