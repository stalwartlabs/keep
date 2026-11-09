/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

/// Protocol version spoken by this crate, announced and negotiated in Hello.
pub const PROTOCOL_VERSION: u32 = 1;

/// Length of the fixed frame header in bytes.
pub const HEADER_LEN: usize = 16;

/// Bytes per header extension word.
pub const EXT_WORD_LEN: usize = 8;

/// Longest key.
pub const MAX_KEY_LEN: usize = u8::MAX as usize;

/// Largest value, payload or message a frame may carry.
pub const MAX_VALUE_LEN: u32 = 4 << 20;

/// Largest Hello value, client or server info.
pub const MAX_HELLO_LEN: usize = 1024;

/// Highest topic id; larger values of the `arg` field are reserved.
pub const MAX_TOPIC: u32 = 0xFFFF;

/// Most members a topology may list.
pub const MAX_MEMBERS: usize = 4096;

/// Most subscriptions one connection may hold; a Subscribe beyond answers
/// `Full`.
pub const MAX_SUBSCRIPTIONS: usize = 4096;
