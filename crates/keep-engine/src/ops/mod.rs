/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

mod incr;
mod kv;
mod lock;

pub use incr::IncrError;
pub(crate) use kv::Write;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum SetMode {
    Always,
    IfAbsent,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum SetOutcome {
    Stored,
    Exists,
    Full,
}
