/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

mod attribute;
mod boot;
mod load;

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use hdrhistogram::Histogram;

use crate::boot::{Interval, Pairing, Sample};
use crate::load::{Run, load_run};

#[derive(Parser)]
#[command(
    name = "analysis",
    about = "HDR merge and bootstrap comparisons for loadgen results"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(ValueEnum, Clone, Copy, PartialEq, Eq)]
enum Format {
    Md,
    Csv,
}

#[derive(ValueEnum, Clone, Copy, PartialEq, Eq)]
enum Metric {
    Latency,
    Service,
}

impl Metric {
    fn name(self) -> &'static str {
        match self {
            Metric::Latency => "latency",
            Metric::Service => "service",
        }
    }
}

#[derive(Subcommand)]
enum Command {
    #[command(
        about = "Pool runs (hosts of one run are summed) and print percentiles per point and op"
    )]
    Merge {
        #[arg(required = true)]
        runs: Vec<PathBuf>,
        #[arg(long, value_enum, default_value = "md")]
        format: Format,
        #[arg(long, value_enum, default_value = "latency")]
        metric: Metric,
        #[arg(
            long,
            value_delimiter = ',',
            default_value = "0.5,0.9,0.99,0.999,0.9999"
        )]
        quantiles: Vec<f64>,
    },
    #[command(about = "Ratio B/A per point, op and quantile with a bootstrap 95% CI")]
    Compare {
        #[arg(long, num_args = 1.., required = true)]
        a: Vec<PathBuf>,
        #[arg(long, num_args = 1.., required = true)]
        b: Vec<PathBuf>,
        #[arg(
            long,
            help = "A and B runs are interleaved pairs on the same VMs (resampled jointly)"
        )]
        paired: bool,
        #[arg(long, value_enum, default_value = "latency")]
        metric: Metric,
        #[arg(long, value_delimiter = ',', default_value = "0.5,0.99,0.999")]
        quantiles: Vec<f64>,
        #[arg(
            long,
            value_delimiter = ',',
            help = "Ops to report (default: all plus every op)"
        )]
        ops: Vec<String>,
        #[arg(
            long,
            default_value_t = 0.0,
            help = "RTT floor subtracted from latencies, microseconds"
        )]
        floor_us: f64,
        #[arg(
            long,
            default_value_t = 0.0,
            help = "Noise floor as a fraction, e.g. 0.02"
        )]
        noise_floor: f64,
        #[arg(long, default_value_t = 10_000)]
        replicates: usize,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, value_enum, default_value = "md")]
        format: Format,
    },
    #[command(
        about = "Attribute latency outliers (loadgen --outlier-us) to causes from tail probe events"
    )]
    Attribute {
        #[arg(help = "loadgen output directory with summary.json and outliers.jsonl")]
        run: PathBuf,
        #[arg(long, help = "tail probe events of the client VM")]
        client_probes: Option<PathBuf>,
        #[arg(
            long,
            help = "tail probe events (and server stage samples) of the server VM"
        )]
        server_probes: Option<PathBuf>,
        #[arg(
            long,
            help = "Consider outliers above this latency in us (default: each point's p99.9)"
        )]
        min_us: Option<f64>,
        #[arg(
            long,
            default_value_t = 200.0,
            help = "Slack for matching server events, us"
        )]
        tolerance_us: f64,
        #[arg(
            long,
            default_value_t = 0.5,
            help = "Share of the latency above p50 a cause must explain"
        )]
        explain_share: f64,
        #[arg(
            long,
            help = "Write one JSON line per outlier with every candidate cause"
        )]
        out: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    let result = match Cli::parse().command {
        Command::Merge {
            runs,
            format,
            metric,
            quantiles,
        } => merge(&runs, format, metric, &quantiles),
        Command::Compare {
            a,
            b,
            paired,
            metric,
            quantiles,
            ops,
            floor_us,
            noise_floor,
            replicates,
            seed,
            format,
        } => compare(&Compare {
            a: &a,
            b: &b,
            pairing: if paired {
                Pairing::Paired
            } else {
                Pairing::Independent
            },
            metric,
            quantiles: &quantiles,
            ops: &ops,
            floor_ns: floor_us * 1e3,
            noise_floor,
            replicates,
            seed,
            format,
        }),
        Command::Attribute {
            run,
            client_probes,
            server_probes,
            min_us,
            tolerance_us,
            explain_share,
            out,
        } => attribute::attribute(&attribute::Options {
            run: &run,
            client: client_probes.as_deref(),
            server: server_probes.as_deref(),
            min_us,
            tolerance_us,
            explain_share,
            out: out.as_deref(),
        }),
    };
    match result {
        Ok(text) => {
            print!("{text}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn load_all(dirs: &[PathBuf]) -> Result<Vec<Run>, String> {
    dirs.iter()
        .map(|dir| load_run(dir).map_err(|e| e.to_string()))
        .collect()
}

fn quantile_label(q: f64) -> String {
    let pct = q * 100.0;
    let mut label = format!("p{pct:.4}");
    while label.ends_with('0') {
        label.pop();
    }
    if label.ends_with('.') {
        label.pop();
    }
    label
}

fn common_points(runs: &[Run]) -> BTreeSet<usize> {
    let mut iter = runs.iter();
    let first: BTreeSet<usize> = iter
        .next()
        .map(|run| run.points.keys().copied().collect())
        .unwrap_or_default();
    iter.fold(first, |acc, run| {
        acc.into_iter()
            .filter(|p| run.points.contains_key(p))
            .collect()
    })
}

fn op_names(runs: &[Run], point: usize, metric: Metric, wanted: &[String]) -> Vec<String> {
    let suffix = metric.name();
    let mut names: BTreeSet<String> = BTreeSet::new();
    for run in runs {
        if let Some(p) = run.points.get(&point) {
            names.extend(p.hists.keys().filter_map(|name| {
                let (op, m) = name.split_once('.')?;
                (m == suffix).then(|| op.to_string())
            }));
        }
    }
    let mut ordered: Vec<String> = names
        .into_iter()
        .filter(|op| wanted.is_empty() || wanted.iter().any(|w| w == op))
        .collect();
    ordered.sort_by_key(|op| (op != "all", op.clone()));
    ordered
}

fn merge(
    dirs: &[PathBuf],
    format: Format,
    metric: Metric,
    quantiles: &[f64],
) -> Result<String, String> {
    let runs = load_all(dirs)?;
    let mut out = String::new();
    let labels: Vec<String> = quantiles.iter().map(|&q| quantile_label(q)).collect();
    let header = format!(
        "point,runs,offered_rate,completed_rate,valid,util_max,op,count,{},max_us",
        labels
            .iter()
            .map(|l| format!("{l}_us"))
            .collect::<Vec<_>>()
            .join(",")
    );
    match format {
        Format::Csv => out.push_str(&header),
        Format::Md => {
            let cols: Vec<&str> = header.split(',').collect();
            let _ = write!(
                out,
                "| {} |\n|{}",
                cols.join(" | "),
                "---|".repeat(cols.len())
            );
        }
    }
    out.push('\n');
    for point in common_points(&runs) {
        let points: Vec<_> = runs
            .iter()
            .filter_map(|run| run.points.get(&point))
            .collect();
        let n = points.len() as f64;
        let offered = points.iter().map(|p| p.offered_rate).sum::<f64>() / n;
        let completed = points.iter().map(|p| p.completed_rate).sum::<f64>() / n;
        let valid = points.iter().all(|p| p.valid);
        let util = points.iter().map(|p| p.util_max).fold(0.0, f64::max);
        for op in op_names(&runs, point, metric, &[]) {
            let key = format!("{op}.{}", metric.name());
            let mut pooled: Option<Histogram<u64>> = None;
            for hist in points.iter().filter_map(|p| p.hists.get(&key)) {
                match &mut pooled {
                    Some(pooled) => pooled.add(hist).map_err(|e| format!("{e:?}"))?,
                    None => pooled = Some(hist.clone()),
                }
            }
            let Some(hist) = pooled else { continue };
            let values: Vec<String> = quantiles
                .iter()
                .map(|&q| format!("{:.2}", hist.value_at_quantile(q) as f64 / 1e3))
                .collect();
            let row = format!(
                "{point},{},{offered:.0},{completed:.0},{valid},{util:.3},{op},{},{},{:.2}",
                points.len(),
                hist.len(),
                values.join(","),
                hist.max() as f64 / 1e3
            );
            match format {
                Format::Csv => out.push_str(&row),
                Format::Md => {
                    let _ = write!(
                        out,
                        "| {} |",
                        row.split(',').collect::<Vec<_>>().join(" | ")
                    );
                }
            }
            out.push('\n');
        }
    }
    Ok(out)
}

struct Compare<'a> {
    a: &'a [PathBuf],
    b: &'a [PathBuf],
    pairing: Pairing,
    metric: Metric,
    quantiles: &'a [f64],
    ops: &'a [String],
    floor_ns: f64,
    noise_floor: f64,
    replicates: usize,
    seed: u64,
    format: Format,
}

fn compare(args: &Compare<'_>) -> Result<String, String> {
    let a = load_all(args.a)?;
    let b = load_all(args.b)?;
    if args.pairing == Pairing::Paired && a.len() != b.len() {
        return Err(format!(
            "--paired needs as many A runs as B runs ({} vs {})",
            a.len(),
            b.len()
        ));
    }
    let both: Vec<Run> = a.iter().chain(b.iter()).cloned().collect();
    let header =
        "point,offered_a,offered_b,op,stat,runs_a,runs_b,a,b,ratio,ci_low,ci_high,verdict,valid";
    let mut out = String::new();
    match args.format {
        Format::Csv => out.push_str(header),
        Format::Md => {
            let cols: Vec<&str> = header.split(',').collect();
            let _ = write!(
                out,
                "| {} |\n|{}",
                cols.join(" | "),
                "---|".repeat(cols.len())
            );
        }
    }
    out.push('\n');
    for point in common_points(&both) {
        let pa: Vec<_> = a.iter().filter_map(|run| run.points.get(&point)).collect();
        let pb: Vec<_> = b.iter().filter_map(|run| run.points.get(&point)).collect();
        let mean = |points: &[&load::Point]| {
            points.iter().map(|p| p.offered_rate).sum::<f64>() / points.len().max(1) as f64
        };
        let valid = pa.iter().chain(pb.iter()).all(|p| p.valid);
        let mut emit = |op: &str,
                        stat: &str,
                        ci: Interval,
                        unit_scale: f64,
                        higher_is_better: bool| {
            let verdict = verdict(&ci, args.noise_floor, higher_is_better);
            let row = format!(
                "{point},{:.0},{:.0},{op},{stat},{},{},{:.2},{:.2},{:.4},{:.4},{:.4},{verdict},{valid}",
                mean(&pa),
                mean(&pb),
                pa.len(),
                pb.len(),
                ci.a / unit_scale,
                ci.b / unit_scale,
                ci.ratio,
                ci.low,
                ci.high
            );
            match args.format {
                Format::Csv => out.push_str(&row),
                Format::Md => {
                    let _ = write!(
                        out,
                        "| {} |",
                        row.split(',').collect::<Vec<_>>().join(" | ")
                    );
                }
            }
            out.push('\n');
        };
        for op in op_names(&both, point, args.metric, args.ops) {
            let key = format!("{op}.{}", args.metric.name());
            let runs_a: Vec<(&Histogram<u64>, f64)> = pa
                .iter()
                .filter_map(|p| Some((p.hists.get(&key)?, p.completed_rate)))
                .collect();
            let runs_b: Vec<(&Histogram<u64>, f64)> = pb
                .iter()
                .filter_map(|p| Some((p.hists.get(&key)?, p.completed_rate)))
                .collect();
            if runs_a.len() != pa.len() || runs_b.len() != pb.len() {
                continue;
            }
            let sample = Sample::new(&runs_a, &runs_b);
            if op == "all"
                && let Some(ci) = sample.throughput_ratio(args.replicates, args.pairing, args.seed)
            {
                emit(&op, "throughput", ci, 1.0, true);
            }
            for (i, &q) in args.quantiles.iter().enumerate() {
                let seed = args.seed.wrapping_add(i as u64 + 1);
                if let Some(ci) =
                    sample.quantile_ratio(q, args.floor_ns, args.replicates, args.pairing, seed)
                {
                    emit(&op, &format!("{}_us", quantile_label(q)), ci, 1e3, false);
                }
            }
        }
    }
    Ok(out)
}

fn verdict(ci: &Interval, noise_floor: f64, higher_is_better: bool) -> &'static str {
    let above = ci.low > 1.0 + noise_floor;
    let below = ci.high < 1.0 - noise_floor;
    match (above, below, higher_is_better) {
        (true, _, true) | (_, true, false) => "B better",
        (true, _, false) | (_, true, true) => "B worse",
        _ => "no difference",
    }
}
