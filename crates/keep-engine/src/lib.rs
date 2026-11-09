/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

mod cap;
mod engine;
mod entry;
mod hash;
mod key;
mod lock;
mod ops;
mod purge;
mod region;
mod snapshot;
mod sweep;
mod table;
mod time;
mod value;

pub use cap::{Cap, Eviction, MemoryUse, Pinned};
pub use engine::{Engine, Hit, KeepEngine};
pub use hash::{Hash, KeyHasher};
pub use key::{Key, KeyClass};
pub use lock::{Gate, LockOutcome, Token, TokenOutcome};
pub use ops::{IncrError, SetMode, SetOutcome};
pub use snapshot::{ExportCursor, Record};
pub use sweep::SweepPace;
pub use table::Sweep;
pub use time::{Deadline, Time, Ttl};
pub use value::ValueKind;
