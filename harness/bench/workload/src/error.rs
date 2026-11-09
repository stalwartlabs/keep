/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::{fmt, io, path::Path};

#[derive(Debug)]
pub enum WorkloadError {
    Invalid {
        context: Box<str>,
        reason: Box<str>,
    },
    Io {
        path: Box<str>,
        source: io::Error,
    },
    Trace {
        path: Box<str>,
        line: usize,
        reason: Box<str>,
    },
    Toml {
        path: Box<str>,
        source: toml::de::Error,
    },
}

impl WorkloadError {
    pub fn invalid(context: &str, reason: &str) -> Self {
        WorkloadError::Invalid {
            context: context.into(),
            reason: reason.into(),
        }
    }

    pub fn io(path: &Path, source: io::Error) -> Self {
        WorkloadError::Io {
            path: path.display().to_string().into_boxed_str(),
            source,
        }
    }

    pub fn trace(path: &Path, line: usize, reason: &str) -> Self {
        WorkloadError::Trace {
            path: path.display().to_string().into_boxed_str(),
            line,
            reason: reason.into(),
        }
    }
}

impl fmt::Display for WorkloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkloadError::Invalid { context, reason } => write!(f, "{context}: {reason}"),
            WorkloadError::Io { path, source } => write!(f, "{path}: {source}"),
            WorkloadError::Trace { path, line, reason } => write!(f, "{path}:{line}: {reason}"),
            WorkloadError::Toml { path, source } => write!(f, "{path}: {source}"),
        }
    }
}
