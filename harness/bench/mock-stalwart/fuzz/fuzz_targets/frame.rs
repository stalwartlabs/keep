/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

#![no_main]

use libfuzzer_sys::fuzz_target;
use mock_stalwart::stream::frame::Message;

fuzz_target!(|data: &[u8]| {
    match Message::decode(data) {
        Ok(Message::Events { header, events }) => {
            for event in events.flatten() {
                let _ = event.verify(header.origin);
            }
        }
        Ok(Message::Acks { entries, .. }) => entries.for_each(drop),
        Err(_) => {}
    }
});
