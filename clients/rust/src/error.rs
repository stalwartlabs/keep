/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::fmt;
use std::io;

use keep_proto::Status;
use keep_proto::keys::KeyFileError;

/// A failed client call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// Invalid settings or URL.
    Config(Box<str>),
    /// A key of this length is empty or too long. Not executed.
    BadKey(usize),
    /// A value of this length exceeds the protocol limit. Not executed.
    ValueTooLarge(usize),
    /// No connection could be opened.
    Connect(io::ErrorKind),
    /// The TLS setup or handshake failed.
    Tls(Box<str>),
    /// The Hello handshake failed.
    Handshake(Box<str>),
    /// No healthy connection reaches the key's owner. Not executed; retry
    /// later.
    Unavailable,
    /// The connection was lost or the attempt timed out after the request was
    /// written: it may or may not have executed. A call whose resend after
    /// such an attempt did not execute still returns this.
    Unknown(Cause),
    /// The server answered with an error status. Not executed.
    Server {
        /// The error status.
        status: Status,
        /// The server's message, possibly empty.
        message: Box<str>,
    },
    /// The key belongs to another server at this topology epoch. Not
    /// executed.
    Moved(u64),
    /// The server's reply did not fit the request.
    UnexpectedReply,
    /// `renew_lock` found the lock gone and another holder took it.
    HeldByOther,
    /// The client already subscribes this topic.
    AlreadySubscribed(u16),
}

/// Why an outcome is unknown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    /// The connection closed.
    Closed,
    /// The connection failed with this I/O error.
    Io(io::ErrorKind),
    /// The server sent GoAway with this status.
    GoAway(Status),
    /// The server broke the protocol.
    Protocol,
    /// The attempt timed out.
    TimedOut,
    /// The request ran on some servers and not on others (DeletePrefix).
    Partial,
}

impl Error {
    pub(crate) fn busy() -> Error {
        Error::Server {
            status: Status::Busy,
            message: Box::default(),
        }
    }

    /// Whether the request certainly did not execute, so any op may be sent
    /// again.
    pub fn not_executed(&self) -> bool {
        match self {
            Error::Unavailable | Error::Moved(_) => true,
            Error::Server { status, .. } => status.not_executed(),
            _ => false,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Config(reason) => write!(f, "configuration: {reason}"),
            Error::BadKey(len) => write!(f, "key length {len} outside 1..=255"),
            Error::ValueTooLarge(len) => write!(f, "value length {len} too large"),
            Error::Connect(kind) => write!(f, "connect: {kind}"),
            Error::Tls(reason) => write!(f, "tls: {reason}"),
            Error::Handshake(reason) => write!(f, "handshake: {reason}"),
            Error::Unavailable => f.write_str("no healthy connection to the key's server"),
            Error::Unknown(cause) => write!(f, "outcome unknown: {cause:?}"),
            Error::Server { status, message } => write!(f, "{status:?}: {message}"),
            Error::Moved(epoch) => write!(f, "key moved at topology epoch {epoch}"),
            Error::UnexpectedReply => f.write_str("reply does not match the request"),
            Error::HeldByOther => f.write_str("lock held by another token"),
            Error::AlreadySubscribed(topic) => write!(f, "topic {topic} already subscribed"),
        }
    }
}

impl std::error::Error for Error {}

impl From<KeyFileError> for Error {
    fn from(err: KeyFileError) -> Error {
        Error::Config(err.into_message())
    }
}
