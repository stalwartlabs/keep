/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use serde::Deserialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Basis {
    Nodes,
    Accounts,
    ActiveAccounts,
    ClientIps,
    MessagesPerHour,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(untagged)]
pub enum CountSpec {
    Fixed(u64),
    Scaled {
        per: Basis,
        factor: f64,
        #[serde(default)]
        min: u64,
    },
}

impl Default for CountSpec {
    fn default() -> Self {
        CountSpec::Fixed(0)
    }
}

impl CountSpec {
    pub fn resolve(&self, deployment: &Deployment) -> u64 {
        match *self {
            CountSpec::Fixed(count) => count,
            CountSpec::Scaled { per, factor, min } => {
                ((deployment.basis(per) as f64 * factor).round() as u64).max(min)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Deployment {
    pub name: &'static str,
    pub nodes: u64,
    pub accounts: u64,
    pub active_accounts: u64,
    pub client_ips: u64,
    pub messages_per_hour: u64,
}

impl Deployment {
    pub fn basis(&self, basis: Basis) -> u64 {
        match basis {
            Basis::Nodes => self.nodes,
            Basis::Accounts => self.accounts,
            Basis::ActiveAccounts => self.active_accounts,
            Basis::ClientIps => self.client_ips,
            Basis::MessagesPerHour => self.messages_per_hour,
        }
    }
}

pub const LAB: Deployment = Deployment {
    name: "lab",
    nodes: 1,
    accounts: 500,
    active_accounts: 500,
    client_ips: 1,
    messages_per_hour: 140_000,
};

pub const SMALL: Deployment = Deployment {
    name: "small",
    nodes: 3,
    accounts: 5_000,
    active_accounts: 1_000,
    client_ips: 10_000,
    messages_per_hour: 20_000,
};

pub const MEDIUM: Deployment = Deployment {
    name: "medium",
    nodes: 8,
    accounts: 100_000,
    active_accounts: 20_000,
    client_ips: 200_000,
    messages_per_hour: 400_000,
};

pub const LARGE: Deployment = Deployment {
    name: "large",
    nodes: 32,
    accounts: 1_000_000,
    active_accounts: 200_000,
    client_ips: 2_000_000,
    messages_per_hour: 4_000_000,
};

pub const HYPER: Deployment = Deployment {
    name: "hyper",
    nodes: 256,
    accounts: 10_000_000,
    active_accounts: 2_000_000,
    client_ips: 20_000_000,
    messages_per_hour: 40_000_000,
};

pub const DEPLOYMENTS: &[&str] = &["lab", "small", "medium", "large", "hyper"];

pub fn builtin(name: &str) -> Option<&'static Deployment> {
    hashify::map!(name.as_bytes(), &'static Deployment,
        "lab" => &LAB,
        "small" => &SMALL,
        "medium" => &MEDIUM,
        "large" => &LARGE,
        "hyper" => &HYPER,
    )
    .copied()
}
