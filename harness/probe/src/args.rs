/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use clap::ValueEnum;
use serde::Serialize;
use std::{num::ParseIntError, str::FromStr};

const MAX_CPU_LIST: usize = 4096;

#[derive(Clone, Debug)]
pub struct CpuList(Box<[usize]>);

impl CpuList {
    pub fn count(&self) -> usize {
        self.0.len()
    }

    pub fn cpu_for(&self, slot: usize) -> Option<usize> {
        self.0.get(slot % self.0.len()).copied()
    }
}

impl FromStr for CpuList {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let number = |v: &str| {
            v.trim()
                .parse::<usize>()
                .map_err(|e: ParseIntError| format!("{v:?}: {e}"))
        };
        let mut cpus = Vec::new();
        for part in s.split(',') {
            match part.split_once('-') {
                Some((lo, hi)) => {
                    let (lo, hi) = (number(lo)?, number(hi)?);
                    if hi < lo || hi - lo >= MAX_CPU_LIST {
                        return Err(format!("bad CPU range {part:?}"));
                    }
                    cpus.extend(lo..=hi);
                }
                None => cpus.push(number(part)?),
            }
            if cpus.len() > MAX_CPU_LIST {
                return Err(format!("more than {MAX_CPU_LIST} CPUs"));
            }
        }
        if cpus.is_empty() {
            return Err("empty CPU list".into());
        }
        Ok(Self(cpus.into_boxed_slice()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Wait {
    Block,
    Spin,
    BusyPoll,
}

impl Wait {
    pub fn spins(self) -> bool {
        self == Wait::Spin
    }
}

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Arrival {
    Uniform,
    Poisson,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Proto {
    Tcp,
    Udp,
}

impl Proto {
    pub fn probe_name(self) -> &'static str {
        match self {
            Proto::Tcp => "tcp-ping",
            Proto::Udp => "udp-ping",
        }
    }
}
