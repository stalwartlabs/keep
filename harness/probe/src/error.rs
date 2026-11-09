/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::{fmt, io};

#[derive(Debug)]
pub enum Error {
    Io {
        context: &'static str,
        source: io::Error,
    },
    #[cfg(any(not(target_os = "linux"), not(netprobe_c_echo)))]
    Unsupported(&'static str),
    Protocol(&'static str),
    Usage(&'static str),
    Internal(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io { context, source } => write!(f, "{context}: {source}"),
            #[cfg(any(not(target_os = "linux"), not(netprobe_c_echo)))]
            Error::Unsupported(what) => write!(f, "unsupported: {what}"),
            Error::Protocol(what) => write!(f, "protocol error: {what}"),
            Error::Usage(what) => write!(f, "usage: {what}"),
            Error::Internal(what) => write!(f, "internal error: {what}"),
        }
    }
}

impl std::error::Error for Error {}

pub trait Context<T> {
    fn context(self, context: &'static str) -> Result<T>;
}

impl<T> Context<T> for io::Result<T> {
    fn context(self, context: &'static str) -> Result<T> {
        self.map_err(|source| Error::Io { context, source })
    }
}
