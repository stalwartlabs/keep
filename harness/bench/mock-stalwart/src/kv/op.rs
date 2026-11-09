/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use serde::Serialize;

macro_rules! kv_ops {
    ($($variant:ident => $name:tt),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
        #[repr(u8)]
        pub enum KvOp {
            $($variant),+
        }

        impl KvOp {
            pub const ALL: &'static [KvOp] = &[$(KvOp::$variant),+];
            pub const COUNT: usize = Self::ALL.len();

            pub fn name(self) -> &'static str {
                match self {
                    $(KvOp::$variant => $name),+
                }
            }

            pub fn parse(name: &[u8]) -> Option<KvOp> {
                hashify::map!(name, KvOp, $($name => KvOp::$variant),+).copied()
            }
        }
    };
}

kv_ops! {
    Get => "get",
    Exists => "exists",
    Set => "set",
    Incr => "incr",
    TryLock => "try_lock",
    Unlock => "unlock",
    Renew => "renew",
    Delete => "delete",
    DeletePrefix => "delete_prefix",
    TryLease => "try_lease",
    RenewLease => "renew_lease",
    ReleaseLease => "release_lease",
    SetMany => "set_many",
}

impl KvOp {
    #[inline]
    pub fn index(self) -> usize {
        self as usize
    }
}
