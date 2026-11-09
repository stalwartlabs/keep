/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

macro_rules! op_kinds {
    ($($variant:ident => $name:tt),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        #[repr(u8)]
        pub enum OpKind {
            $($variant),+
        }

        impl OpKind {
            pub const ALL: &'static [OpKind] = &[$(OpKind::$variant),+];
            pub const COUNT: usize = Self::ALL.len();

            pub fn name(self) -> &'static str {
                match self {
                    $(OpKind::$variant => $name),+
                }
            }

            pub fn parse(name: &[u8]) -> Option<OpKind> {
                hashify::map!(name, OpKind, $($name => OpKind::$variant),+).copied()
            }
        }
    };
}

op_kinds! {
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

impl OpKind {
    pub fn index(self) -> usize {
        self as usize
    }

    pub fn carries_value(self) -> bool {
        matches!(
            self,
            OpKind::Set | OpKind::TryLock | OpKind::Renew | OpKind::SetMany
        )
    }

    pub fn carries_token(self) -> bool {
        matches!(
            self,
            OpKind::TryLease | OpKind::RenewLease | OpKind::ReleaseLease
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Miss,
    Error,
}

#[derive(Clone, Copy, Debug)]
pub struct Request<'a> {
    pub kind: OpKind,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub ttl: u32,
    pub delta: i64,
    pub batch: u8,
    pub token: u64,
}
