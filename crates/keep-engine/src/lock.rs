/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Token([u8; 8]);

impl Token {
    pub const fn new(bytes: [u8; 8]) -> Token {
        Token(bytes)
    }

    pub fn from_slice(bytes: &[u8]) -> Option<Token> {
        bytes.try_into().ok().map(Token)
    }

    #[inline]
    pub fn bytes(self) -> [u8; 8] {
        self.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Gate {
    Open,
    Quarantined,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum LockOutcome {
    Granted,
    Held,
    Full,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum TokenOutcome {
    Done,
    NotFound,
    Conflict,
}
