#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
"""Stream tables from harness/bench/stream.sh results.

usage: stream_table.py [--jsonl FILE] RUN_DIR...

Per cell: accounting recomputed with stream_accounting.py, validity (loss = gaps,
missing end markers, incomplete deliveries or missing node reports; late = node windows more than
200 ms apart), broker cores and peak memory from the container cgroup samples inside the common
measurement window (not averaged over the whole sampling period), broker ns per received event.
Then per run, nodes, rate and target: medians over the valid reps.
"""
import argparse
import collections
import glob
import importlib.util
import json
import os
import re
import statistics

HERE = os.path.dirname(os.path.abspath(__file__))
ACCOUNTING = os.path.join(HERE, "stream_accounting.py")
NAME = re.compile(r"^(?P<sat>sat-)?(?P<target>.+)-n(?P<nodes>\d+)-r(?P<rate>\d+)-stalwart-(?P<rep>\d+)$")
LATE_MS = 200


def load_accounting():
    spec = importlib.util.spec_from_file_location("stream_accounting", ACCOUNTING)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.account


def broker(directory, reports):
    starts = [r["window_start_unix_ns"] for r in reports if r.get("window_start_unix_ns")]
    if not starts:
        return None, None
    begin = max(starts)
    end = begin + min(r["window_ns"] for r in reports)
    with open(os.path.join(directory, "cgsample.jsonl")) as f:
        samples = [json.loads(line) for line in f if line.strip()]
    inside = [s for s in samples if begin <= s["t_ns"] <= end]
    if len(inside) < 2:
        return None, None
    cores = (inside[-1]["cpu_us"] - inside[0]["cpu_us"]) / ((inside[-1]["t_ns"] - inside[0]["t_ns"]) / 1e3)
    return cores, max(s["mem_bytes"] for s in inside) / 2**20


def cell(account, run, directory, match):
    nodes = int(match["nodes"])
    reports = []
    for path in glob.glob(os.path.join(directory, "node-*.json")):
        with open(path) as f:
            reports.append(json.load(f))
    row = {"run": os.path.basename(os.path.normpath(run)), "cell": os.path.basename(directory),
           "saturate": bool(match["sat"]), "target": match["target"], "cell_nodes": nodes,
           "rate": int(match["rate"]), "rep": int(match["rep"]), "reported": len(reports)}
    accounting = account(directory) if reports else None
    if accounting is None:
        return {**row, "valid": False, "reason": "no-reports"}
    cores, mib = broker(directory, reports)
    window_s = min(r["window_ns"] for r in reports) / 1e9
    received = accounting["events_received"] / window_s
    lost = (accounting["gaps"] > 0 or accounting["end_markers"] < accounting["end_markers_expected"]
            or accounting["deliveries_incomplete"] > 0 or accounting["nodes"] < nodes)
    late = (accounting["window_start_spread_ms"] or 0) > LATE_MS
    reason = "late" if late else "lost" if lost else "ok"
    return {**row, **accounting, "valid": reason == "ok", "reason": reason, "broker_cores": cores,
            "broker_mib": mib, "received_per_s": received,
            "broker_ns_per_received": cores * 1e9 / received if cores and received else None}


def median(rows, key):
    values = [r[key] for r in rows if r.get(key) is not None]
    return statistics.median(values) if values else None


def fmt(value, digits=0):
    return "-" if value is None else f"{value:.{digits}f}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jsonl")
    parser.add_argument("runs", nargs="+")
    args = parser.parse_args()
    account = load_accounting()
    rows = []
    for run in args.runs:
        for directory in sorted(glob.glob(os.path.join(run, "*"))):
            match = NAME.match(os.path.basename(directory))
            if match and os.path.isdir(directory):
                rows.append(cell(account, run, directory, match))
    if args.jsonl:
        with open(args.jsonl, "w") as f:
            f.writelines(json.dumps(r) + "\n" for r in rows)
    groups = collections.defaultdict(list)
    for r in rows:
        groups[(r["run"], r["saturate"], r["cell_nodes"], r["rate"], r["target"])].append(r)
    print("| run | nodes | rate | target | valid/reps | echo p50 | echo p99 | echo_last p99 worst node | broker cores | broker ns/event | broker MiB | client ns/event |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for key in sorted(groups, key=lambda k: (k[0], k[2], k[3], k[4])):
        run, _, nodes, rate, target = key
        valid = [r for r in groups[key] if r["valid"]]
        reasons = ",".join(sorted({r["reason"] for r in groups[key] if not r["valid"]}))
        print(f"| {run} | {nodes} | {rate} | {target} | {len(valid)}/{len(groups[key])}{' ' + reasons if reasons else ''} | "
              f"{fmt(median(valid, 'node1_echo_p50_us'))} | {fmt(median(valid, 'node1_echo_p99_us'))} | "
              f"{fmt(median(valid, 'echo_last_p99_us_max'))} | {fmt(median(valid, 'broker_cores'), 2)} | "
              f"{fmt(median(valid, 'broker_ns_per_received'))} | {fmt(median(valid, 'broker_mib'))} | "
              f"{fmt(median(valid, 'client_ns_per_event_received_mean'))} |")


if __name__ == "__main__":
    main()
