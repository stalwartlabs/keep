/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use keep_engine::MemoryUse;

macro_rules! counters {
    ($($name:ident),+ $(,)?) => {
        #[derive(Debug, Default)]
        #[repr(align(128))]
        pub struct ShardStats {
            $($name: AtomicU64,)+
            footprint: AtomicUsize,
            large_bytes: AtomicUsize,
            large_evicted: AtomicU64,
            large_orphaned: AtomicU64,
        }

        #[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
        pub struct ShardCounts {
            $(pub $name: u64,)+
        }

        impl ShardStats {
            pub fn counts(&self) -> ShardCounts {
                ShardCounts {
                    $($name: self.$name.load(Ordering::Relaxed),)+
                }
            }

            pub(crate) fn add(&self, counts: &ShardCounts) {
                $(add(&self.$name, counts.$name);)+
            }
        }

        impl std::ops::AddAssign for ShardCounts {
            fn add_assign(&mut self, other: ShardCounts) {
                $(self.$name = self.$name.wrapping_add(other.$name);)+
            }
        }
    };
}

counters!(
    requests, forwarded, remote, busy, published, delivered, skipped, slow, skip_runs, windows,
    held_ns
);

impl ShardStats {
    pub(crate) fn record_memory(&self, memory: MemoryUse) {
        self.footprint.store(memory.footprint, Ordering::Relaxed);
        self.large_bytes
            .store(memory.large_bytes, Ordering::Relaxed);
        self.large_evicted
            .store(memory.large_evicted, Ordering::Relaxed);
        self.large_orphaned
            .store(memory.large_orphaned, Ordering::Relaxed);
    }

    pub fn memory(&self) -> MemoryUse {
        MemoryUse {
            footprint: self.footprint.load(Ordering::Relaxed),
            large_bytes: self.large_bytes.load(Ordering::Relaxed),
            large_evicted: self.large_evicted.load(Ordering::Relaxed),
            large_orphaned: self.large_orphaned.load(Ordering::Relaxed),
        }
    }
}

#[inline]
fn add(cell: &AtomicU64, value: u64) {
    if value != 0 {
        cell.store(
            cell.load(Ordering::Relaxed).wrapping_add(value),
            Ordering::Relaxed,
        );
    }
}
