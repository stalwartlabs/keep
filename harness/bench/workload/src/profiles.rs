/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

macro_rules! kv_profile {
    ($file:literal) => {
        concat!(
            include_str!("../profiles/families.toml"),
            "\n",
            include_str!(concat!("../profiles/", $file))
        )
    };
}

pub const MIXED: &str = kv_profile!("kv-mixed.toml");
pub const MTA: &str = kv_profile!("kv-mta.toml");
pub const IMAP: &str = kv_profile!("kv-imap.toml");
pub const HTTP: &str = kv_profile!("kv-http.toml");
pub const ATTACK: &str = kv_profile!("kv-attack.toml");
pub const SWAP: &str = kv_profile!("kv-swap.toml");
pub const SWAP_BATCH: &str = kv_profile!("kv-swap-batch.toml");
pub const LEASE: &str = kv_profile!("kv-lease.toml");

pub const EVENTS_MIXED: &str = include_str!("../profiles/events-mixed.toml");
pub const EVENTS_MTA: &str = include_str!("../profiles/events-mta.toml");
pub const EVENTS_IMAP: &str = include_str!("../profiles/events-imap.toml");
pub const EVENTS_HTTP: &str = include_str!("../profiles/events-http.toml");
pub const EVENTS_PROVISIONAL: &str = include_str!("../profiles/events-provisional.toml");

pub fn builtin(name: &str) -> Option<&'static str> {
    hashify::map!(name.as_bytes(), &'static str,
        "mixed" => MIXED,
        "mta" => MTA,
        "imap" => IMAP,
        "http" => HTTP,
        "attack" => ATTACK,
        "swap" => SWAP,
        "swap-batch" => SWAP_BATCH,
        "lease" => LEASE,
        "stalwart" => EVENTS_MIXED,
        "events-mixed" => EVENTS_MIXED,
        "events-mta" => EVENTS_MTA,
        "events-imap" => EVENTS_IMAP,
        "events-http" => EVENTS_HTTP,
        "provisional" => EVENTS_PROVISIONAL,
    )
    .copied()
}

pub const KV_PROFILES: &[&str] = &[
    "mixed",
    "mta",
    "imap",
    "http",
    "attack",
    "swap",
    "swap-batch",
    "lease",
];
pub const EVENT_PROFILES: &[&str] = &[
    "stalwart",
    "events-mixed",
    "events-mta",
    "events-imap",
    "events-http",
    "provisional",
];
