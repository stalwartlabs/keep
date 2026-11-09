/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::sync::atomic::{AtomicU64, Ordering};

macro_rules! counters {
    ($($name:ident),+ $(,)?) => {
        #[derive(Debug, Default)]
        #[repr(align(128))]
        pub struct LoopStats {
            $($name: AtomicU64,)+
        }

        #[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
        pub struct LoopCounts {
            $(pub $name: u64,)+
        }

        impl LoopStats {
            pub fn counts(&self) -> LoopCounts {
                LoopCounts {
                    $($name: self.$name.load(Ordering::Relaxed),)+
                }
            }
        }
    };
}

counters!(
    turns, parks, wakes_sent, reads, bytes_in, writes, bytes_out, accepted, refused, closed,
    stalls,
);

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[inline]
fn add(cell: &AtomicU64, value: u64) {
    cell.store(
        cell.load(Ordering::Relaxed).wrapping_add(value),
        Ordering::Relaxed,
    );
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
impl LoopStats {
    #[inline]
    pub(crate) fn turn(&self, parked: bool, wakes: u32) {
        add(&self.turns, 1);
        add(&self.parks, u64::from(parked));
        add(&self.wakes_sent, u64::from(wakes));
    }

    #[inline]
    pub(crate) fn read(&self, bytes: usize) {
        add(&self.reads, 1);
        add(&self.bytes_in, bytes as u64);
    }

    #[inline]
    pub(crate) fn write(&self, bytes: usize) {
        add(&self.writes, 1);
        add(&self.bytes_out, bytes as u64);
    }

    pub(crate) fn accepted(&self) {
        add(&self.accepted, 1);
    }

    #[cold]
    pub(crate) fn refused(&self) {
        add(&self.refused, 1);
    }

    pub(crate) fn closed(&self) {
        add(&self.closed, 1);
    }

    #[cold]
    pub(crate) fn stall(&self) {
        add(&self.stalls, 1);
    }
}
