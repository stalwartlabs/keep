/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::sync::atomic::{AtomicU32, Ordering, fence};

const AWAKE: u32 = 0;
const PARKED: u32 = 1;

#[repr(align(128))]
pub(crate) struct Parker {
    state: AtomicU32,
}

impl Parker {
    pub(crate) fn new() -> Parker {
        Parker {
            state: AtomicU32::new(AWAKE),
        }
    }

    pub(crate) fn prepare(&self) {
        self.state.store(PARKED, Ordering::Relaxed);
        fence(Ordering::SeqCst);
    }

    pub(crate) fn resume(&self) {
        self.state.store(AWAKE, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn claim_wake(&self) -> bool {
        fence(Ordering::SeqCst);
        self.state.load(Ordering::Relaxed) == PARKED
            && self.state.swap(AWAKE, Ordering::AcqRel) == PARKED
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-rt/mesh/parker.rs"]
mod tests;
