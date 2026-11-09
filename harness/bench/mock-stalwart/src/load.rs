/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{cpu::thread_cpu_ns, stream::subscriber::spin};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{runtime::Handle, sync::watch, task::JoinHandle};

const IDLE_TICK: Duration = Duration::from_millis(1);
const BURNERS_PER_WORKER: usize = 2;

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct Load {
    pub fraction: f64,
    pub chunk: Duration,
}

pub fn spawn(
    handle: &Handle,
    load: Load,
    workers: usize,
    busy: &Arc<AtomicU64>,
    stop: &watch::Receiver<bool>,
) -> Vec<JoinHandle<()>> {
    let started = Instant::now();
    (0..workers * BURNERS_PER_WORKER)
        .map(|_| {
            let busy = busy.clone();
            let mut stop = stop.clone();
            handle.spawn(async move {
                let capacity = load.fraction * workers as f64;
                loop {
                    if *stop.borrow() {
                        return;
                    }
                    let budget = (capacity * started.elapsed().as_nanos() as f64) as u64;
                    if busy.load(Ordering::Relaxed) < budget {
                        let before = thread_cpu_ns();
                        spin(load.chunk);
                        busy.fetch_add(thread_cpu_ns().saturating_sub(before), Ordering::Relaxed);
                        tokio::task::yield_now().await;
                    } else {
                        tokio::select! {
                            _ = tokio::time::sleep(IDLE_TICK) => {}
                            _ = stop.changed() => return,
                        }
                    }
                }
            })
        })
        .collect()
}
