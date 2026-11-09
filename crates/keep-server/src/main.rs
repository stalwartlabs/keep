/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::process::ExitCode;

#[cfg(target_os = "linux")]
fn main() -> ExitCode {
    cli::main()
}

#[cfg(not(target_os = "linux"))]
fn main() -> ExitCode {
    eprintln!("keep: the server runs on Linux only");
    ExitCode::FAILURE
}

#[cfg(target_os = "linux")]
mod cli;
