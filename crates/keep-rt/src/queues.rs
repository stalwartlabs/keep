/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fs;
use std::path::Path;

const CONTROL_VECTORS: [&str; 5] = ["hwc", "async", "admin", "mbox", "ctrl"];

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RxQueues {
    cpus: Vec<usize>,
}

impl RxQueues {
    pub fn detect() -> Option<RxQueues> {
        let devices = pci_network_devices();
        if devices.is_empty() {
            return None;
        }
        let interrupts = fs::read_to_string("/proc/interrupts").ok()?;
        let cpus: Vec<usize> = queue_irqs(&interrupts, &devices)
            .filter_map(effective_cpu)
            .collect();
        (!cpus.is_empty()).then_some(RxQueues { cpus })
    }

    pub fn from_cpus(cpus: Vec<usize>) -> RxQueues {
        RxQueues { cpus }
    }

    pub fn mismatch(&self, shard_cpus: &[usize]) -> Option<(usize, usize)> {
        shard_cpus.iter().find_map(|&cpu| {
            let queues = self.cpus.iter().filter(|&&queue| queue == cpu).count();
            (queues != 1).then_some((cpu, queues))
        })
    }
}

fn pci_network_devices() -> Vec<String> {
    let Ok(entries) = fs::read_dir("/sys/class/net") else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| fs::canonicalize(entry.path().join("device")).ok())
        .filter(|device| is_pci(device))
        .filter_map(|device| {
            device
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_owned)
        })
        .collect()
}

fn is_pci(device: &Path) -> bool {
    fs::canonicalize(device.join("subsystem"))
        .ok()
        .is_some_and(|subsystem| subsystem.ends_with("bus/pci"))
}

fn queue_irqs<'a>(interrupts: &'a str, devices: &'a [String]) -> impl Iterator<Item = u32> + 'a {
    interrupts.lines().skip(1).filter_map(move |line| {
        let (irq, rest) = line.trim_start().split_once(':')?;
        let irq = irq.parse::<u32>().ok()?;
        let name = rest.split_whitespace().last()?;
        let ours = devices.iter().any(|device| name.contains(device.as_str()));
        let control = CONTROL_VECTORS
            .iter()
            .any(|fragment| name.contains(fragment));
        (ours && !control).then_some(irq)
    })
}

fn effective_cpu(irq: u32) -> Option<usize> {
    let list = fs::read_to_string(format!("/proc/irq/{irq}/effective_affinity_list")).ok()?;
    let list = list.trim();
    let first = list.split([',', '-']).next()?;
    let cpu = first.parse().ok()?;
    (first.len() == list.len()).then_some(cpu)
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-rt/queues.rs"]
mod tests;
