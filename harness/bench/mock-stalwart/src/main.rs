/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use clap::Parser;
use mock_stalwart::{
    clock,
    config::{self, Cli, Command, Config, ConfigError, FlushArgs, MergeArgs, ProfileArgs, RunArgs},
    instance::{self, Shared},
    kv, report, workload,
};
use std::process::ExitCode;

fn main() -> ExitCode {
    let result = match Cli::parse().command {
        Command::Run(args) => run(*args),
        Command::Flush(args) => flush(args),
        Command::Merge(args) => merge(args),
        Command::Profile(args) => profile(args),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: RunArgs) -> Result<(), String> {
    let config = Config::try_from(args).map_err(|err: ConfigError| err.to_string())?;
    let shared = Shared::new(config.instances);
    let first = config.node;
    let results = std::thread::scope(|scope| {
        let handles = (first..first + config.instances)
            .map(|node| {
                let (config, shared) = (&config, &shared);
                std::thread::Builder::new()
                    .name(format!("ms-instance-{node}"))
                    .spawn_scoped(scope, move || instance::run(config, node, shared))
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| match handle {
                Ok(handle) => handle
                    .join()
                    .map_err(|_| "instance thread panicked".to_string())?
                    .map_err(|err| err.to_string()),
                Err(err) => Err(err.to_string()),
            })
            .collect::<Vec<_>>()
    });
    let mut failure = None;
    for result in results {
        match result {
            Ok(report) => {
                report.print();
                if let Some(dir) = &config.out {
                    report
                        .write(dir)
                        .map_err(|err| format!("writing report: {err}"))?;
                }
            }
            Err(err) => failure = Some(err),
        }
    }
    failure.map_or(Ok(()), Err)
}

fn flush(args: FlushArgs) -> Result<(), String> {
    let target = config::parse_kv_target(&args.kv)
        .map_err(|err| err.to_string())?
        .ok_or_else(|| "nothing to flush".to_string())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| err.to_string())?;
    runtime.block_on(async {
        let backend = kv::KvBackend::connect(
            &target,
            kv::Mapping::Native,
            kv::ConnShape::Pool(1),
            clock::Clock::start(),
        )
        .await
        .map_err(|err| err.to_string())?;
        backend.flush().await.map_err(|err| err.to_string())
    })
}

fn merge(args: MergeArgs) -> Result<(), String> {
    let (percentiles, counters) = report::merge(&args.files).map_err(|err| err.to_string())?;
    println!(
        "{:<28}{:>12}{:>12}{:>12}{:>12}{:>12}{:>12}",
        "tag", "count", "p50", "p99", "p99.9", "p99.99", "max"
    );
    for (tag, value) in &percentiles {
        println!(
            "{:<28}{:>12}{:>12}{:>12}{:>12}{:>12}{:>12}",
            tag, value.count, value.p50, value.p99, value.p999, value.p9999, value.max
        );
    }
    if let Some(path) = args.json {
        let json = serde_json::json!({ "histograms": percentiles, "counters": counters });
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&json).map_err(|err| err.to_string())?,
        )
        .map_err(|err| err.to_string())?;
    } else {
        println!(
            "{}",
            serde_json::to_string_pretty(&counters).map_err(|err| err.to_string())?
        );
    }
    Ok(())
}

fn profile(args: ProfileArgs) -> Result<(), String> {
    let text = workload::profiles::builtin(&args.name).ok_or_else(|| {
        format!(
            "no built-in profile {}; KV profiles: {}; event profiles: {}",
            args.name,
            workload::profiles::KV_PROFILES.join(", "),
            workload::profiles::EVENT_PROFILES.join(", ")
        )
    })?;
    print!("{text}");
    Ok(())
}
