/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::io;
use std::time::Duration;

use keep_client::{Client, Ttl};
use tokio::time::sleep;

const KEYS: u64 = 4_096;
const FIRST_BYTE: u8 = 0x6c;
const TTL_SECS: u32 = 600;
const RETRY: Duration = Duration::from_millis(200);
const LENS: [usize; 4] = [32 << 10, 64 << 10, 256 << 10, 1 << 20];

/// Writes large values on keys of its own, whatever the answers, so a run's
/// workload meets a server at its memory cap. No history records them.
pub async fn write(url: String, period: Duration) -> io::Result<()> {
    let client = loop {
        match Client::connect_url(&url).await {
            Ok(client) => break client,
            Err(error) => {
                eprintln!("keep-jepsen large-writer: connect: {error}");
                sleep(RETRY).await;
            }
        }
    };
    let values = LENS.map(|len| vec![FIRST_BYTE; len]);
    let (mut written, mut refused, mut step) = (0u64, 0u64, 0u64);
    let mut key = [FIRST_BYTE; 16];
    loop {
        if let Some(id) = key.get_mut(1..9) {
            id.copy_from_slice(&(step % KEYS).to_le_bytes());
        }
        let index = usize::try_from(step % LENS.len() as u64).unwrap_or(0);
        if let Some(value) = values.get(index) {
            match client.set(&key, value, Ttl::from_secs(TTL_SECS)).await {
                Ok(()) => written += 1,
                Err(_) => refused += 1,
            }
        }
        if step.is_multiple_of(1_024) {
            eprintln!("keep-jepsen large-writer: {written} written, {refused} refused");
        }
        step += 1;
        sleep(period).await;
    }
}
