/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::ops::AddAssign;

macro_rules! counters {
    ($($name:ident),+ $(,)?) => {
        /// Client counters, summed over every connection.
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
        pub struct Stats {
            $(
                #[doc = concat!("The `", stringify!($name), "` counter.")]
                pub $name: u64
            ),+
        }

        impl Stats {
            /// Every counter with its name, in declaration order.
            pub fn fields(&self) -> impl Iterator<Item = (&'static str, u64)> {
                [$((stringify!($name), self.$name)),+].into_iter()
            }
        }

        impl AddAssign for Stats {
            fn add_assign(&mut self, other: Stats) {
                $(self.$name += other.$name;)+
            }
        }
    };
}

counters! {
    frames_out,
    writes,
    bytes_out,
    write_stalls,
    frames_in,
    reads,
    bytes_in,
    unknown_replies,
    events,
    connects,
    connect_failures,
    escapes,
    retries,
    timeouts,
    fail_fast,
    owner_timeouts,
    refreshes,
    pings,
    goaways,
    quiet_errors,
    stray_events,
    stream_drops,
    feed_misplaced,
    feed_node_drawn,
    gate_waits,
    gate_probes,
}
