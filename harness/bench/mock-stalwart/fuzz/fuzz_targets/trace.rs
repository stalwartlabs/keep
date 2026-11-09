/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

#![no_main]

use libfuzzer_sys::fuzz_target;
use mock_stalwart::workload::trace::{EventTrace, KvTrace, TraceNodes};
use std::path::Path;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = KvTrace::parse(text, Path::new("fuzz"), TraceNodes::All);
        let _ = EventTrace::parse(text, Path::new("fuzz"), TraceNodes::Only(1));
    }
});
