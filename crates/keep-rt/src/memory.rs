/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fs;
use std::io;

use crate::cpu::{CGROUP_ROOT, own_cgroup};

pub fn budget() -> io::Result<usize> {
    let total = total_memory()?;
    Ok(cgroup_limit().map_or(total, |limit| limit.min(total)))
}

fn total_memory() -> io::Result<usize> {
    let (pages, page_size) = unsafe {
        (
            libc::sysconf(libc::_SC_PHYS_PAGES),
            libc::sysconf(libc::_SC_PAGESIZE),
        )
    };
    match (usize::try_from(pages), usize::try_from(page_size)) {
        (Ok(pages), Ok(page_size)) if pages > 0 && page_size > 0 => {
            Ok(pages.saturating_mul(page_size))
        }
        _ => Err(io::Error::other(
            "the total memory of this host is unknown; pass --memory",
        )),
    }
}

fn cgroup_limit() -> Option<usize> {
    own_cgroup()?
        .ancestors()
        .take_while(|dir| dir.starts_with(CGROUP_ROOT))
        .filter_map(|dir| parse_memory_max(&fs::read_to_string(dir.join("memory.max")).ok()?))
        .min()
}

fn parse_memory_max(text: &str) -> Option<usize> {
    text.trim().parse().ok()
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-rt/memory.rs"]
mod tests;
