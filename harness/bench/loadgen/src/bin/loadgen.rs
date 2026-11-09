/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::process::ExitCode;

use clap::Parser;
use loadgen::cli::{Cli, Command};
use loadgen::run;
use loadgen::workload::profile;

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().collect();
    match Cli::parse().command {
        Command::Run(args) => match run::execute(&args, argv) {
            Ok(true) => ExitCode::SUCCESS,
            Ok(false) => {
                eprintln!("run INVALID, see summary");
                ExitCode::from(2)
            }
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            }
        },
        Command::Profile { name } => match profile::builtin_text(&name) {
            Some(Ok(text)) => {
                print!("{text}");
                ExitCode::SUCCESS
            }
            Some(Err(e)) => {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            }
            None => {
                eprintln!(
                    "unknown profile {name:?}; built-in: stalwart-mix, getset, footprint, stalwart:<{}>",
                    stalwart_workload::profiles::KV_PROFILES.join("|")
                );
                ExitCode::FAILURE
            }
        },
    }
}
