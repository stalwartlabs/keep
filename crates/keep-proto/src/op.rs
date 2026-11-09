/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use crate::types::Flags;

macro_rules! ops {
    ($($(#[$meta:meta])* $name:ident = $code:literal),+ $(,)?) => {
        /// Request operation codes of version 1.
        #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
        #[repr(u8)]
        pub enum Op {
            $($(#[$meta])* $name = $code),+
        }

        impl Op {
            /// Every op in order.
            pub const ALL: &'static [Op] = &[$(Op::$name),+];

            /// The op of wire code `code`.
            #[inline]
            pub fn from_code(code: u8) -> Option<Op> {
                match code {
                    $($code => Some(Op::$name),)+
                    _ => None,
                }
            }
        }
    };
}

ops! {
    /// Read a key.
    Get = 0x01,
    /// Test a key.
    Exists = 0x02,
    /// Store a value.
    Set = 0x03,
    /// Delete a key.
    Delete = 0x04,
    /// Add to a counter.
    Incr = 0x05,
    /// Acquire a lock.
    TryLock = 0x06,
    /// Release a lock.
    Unlock = 0x07,
    /// Extend an expiry.
    Renew = 0x08,
    /// Delete keys by prefix.
    DeletePrefix = 0x09,
    /// Publish an event.
    Publish = 0x10,
    /// Subscribe to a topic.
    Subscribe = 0x11,
    /// End a subscription.
    Unsubscribe = 0x12,
    /// Open the session.
    Hello = 0x20,
    /// Round-trip.
    Ping = 0x21,
    /// Fetch the topology.
    Topology = 0x22,
    /// Authenticate.
    Auth = 0x24,
    /// Leave the cluster.
    Leave = 0x26,
}

impl Op {
    /// The flag bits this op accepts.
    #[inline]
    pub const fn allowed_flags(self) -> Flags {
        match self {
            Op::Get => Flags::QUIET.union(Flags::WITH_TTL),
            Op::Set => Flags::QUIET.union(Flags::IF_ABSENT),
            Op::Hello | Op::Auth => Flags::NONE,
            _ => Flags::QUIET,
        }
    }
}
