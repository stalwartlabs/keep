/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub(crate) const CGROUP_ROOT: &str = "/sys/fs/cgroup";

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct CpuList(Vec<usize>);

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CpuListError(Box<str>);

impl fmt::Display for CpuListError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid cpu list element {:?}", self.0)
    }
}

impl std::error::Error for CpuListError {}

const MAX_CPU: usize = 1 << 16;

impl FromStr for CpuList {
    type Err = CpuListError;

    fn from_str(list: &str) -> Result<Self, Self::Err> {
        let mut cpus = Vec::new();
        for part in list
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
        {
            let invalid = || CpuListError(part.into());
            let cpu = |text: &str| {
                text.parse::<usize>()
                    .ok()
                    .filter(|&cpu| cpu < MAX_CPU)
                    .ok_or_else(invalid)
            };
            match part.split_once('-') {
                Some((first, last)) => {
                    let (first, last) = (cpu(first)?, cpu(last)?);
                    if first > last {
                        return Err(invalid());
                    }
                    cpus.extend(first..=last);
                }
                None => cpus.push(cpu(part)?),
            }
        }
        Ok(CpuList(cpus))
    }
}

impl FromIterator<usize> for CpuList {
    fn from_iter<I: IntoIterator<Item = usize>>(iter: I) -> Self {
        CpuList(iter.into_iter().collect())
    }
}

impl CpuList {
    pub fn as_slice(&self) -> &[usize] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<usize> {
        self.0.get(index).copied()
    }

    pub fn effective() -> io::Result<CpuList> {
        effective_cpus().map(CpuList)
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct CpuBudget {
    pub cpus: CpuList,
    pub quota: Option<f64>,
}

impl CpuBudget {
    pub fn detect() -> io::Result<CpuBudget> {
        Ok(CpuBudget {
            cpus: CpuList::effective()?,
            quota: cgroup_quota(),
        })
    }

    pub fn shards(&self) -> usize {
        let cpus = self.cpus.len().max(1);
        match self.quota {
            Some(quota) if quota.is_finite() => (quota.floor() as usize).clamp(1, cpus),
            _ => cpus,
        }
    }

    pub fn exclusive(&self) -> bool {
        self.quota
            .is_none_or(|quota| quota >= self.cpus.len() as f64)
    }
}

pub(crate) fn own_cgroup() -> Option<PathBuf> {
    let membership = fs::read_to_string("/proc/self/cgroup").ok()?;
    let path = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))?;
    Some(Path::new(CGROUP_ROOT).join(path.trim_start_matches('/')))
}

fn cgroup_quota() -> Option<f64> {
    own_cgroup()?
        .ancestors()
        .take_while(|dir| dir.starts_with(CGROUP_ROOT))
        .filter_map(|dir| parse_cpu_max(&fs::read_to_string(dir.join("cpu.max")).ok()?))
        .reduce(f64::min)
}

fn parse_cpu_max(text: &str) -> Option<f64> {
    let (quota, period) = text.trim().split_once(' ')?;
    let quota: f64 = quota.parse().ok()?;
    let period: f64 = period.parse().ok()?;
    (period > 0.0).then_some(quota / period)
}

#[cfg(target_os = "linux")]
pub fn pin_current_thread(cpu: usize) -> io::Result<()> {
    if cpu >= libc::CPU_SETSIZE as usize {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        if libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn pin_current_thread(_cpu: usize) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn effective_cpus() -> io::Result<Vec<usize>> {
    let set = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return Err(io::Error::last_os_error());
        }
        set
    };
    Ok((0..libc::CPU_SETSIZE as usize)
        .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
        .collect())
}

#[cfg(not(target_os = "linux"))]
fn effective_cpus() -> io::Result<Vec<usize>> {
    Ok((0..std::thread::available_parallelism()?.get()).collect())
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-rt/cpu.rs"]
mod tests;
