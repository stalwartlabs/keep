#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
"""KV tables from harness/bench/kv.sh results.

usage: kv_table.py [--analysis BIN] [--jsonl FILE] RUN_DIR...

Each RUN_DIR is the results directory of one run (kv.sh OUT). Per sweep (a directory with genN/summary.json) and
point: offered and completed rate (generators summed), validity, generator lag, latency p50/p99/p99.9
of all ops (pooled with `analysis merge`) and as excess over the block's generator-to-server RTT p50,
server CPU per op and memory (server-cpu.jsonl). Then a summary across blocks per sweep kind.
"""
import argparse
import csv
import io
import json
import math
import os
import re
import statistics
import subprocess
import sys

NAME = re.compile(r"^(?P<tag>.+)-c(?P<cores>\d+)-(?P<server>[a-z-]+?)-(?P<mapping>stalwart|native|meta|keep|hash|unpinned|tls)-(?P<shape>wide|narrow|x48)-r(?P<rep>\d+)$")
SATURATED = 0.99
BOUND_US = 1000.0
COARSE = 1.25


def load_json(path):
    with open(path) as f:
        return json.load(f)


def jsonl(path):
    if not os.path.isfile(path):
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.startswith("{")]


def rtt_floor_us(run_dir):
    p50s = []
    for name in sorted(os.listdir(run_dir)):
        if name.startswith("rtt-") and name.endswith(".jsonl"):
            for r in jsonl(os.path.join(run_dir, name)):
                ends = set(r.get("label", "").split(">"))
                if "srv" in ends and any(e.startswith("gen") for e in ends):
                    p50s.append(r["rtt_ns"]["p50"] / 1e3)
    return statistics.median(p50s) if p50s else None


def merged(analysis, sweep):
    out = subprocess.run([analysis, "merge", sweep, "--format", "csv"], capture_output=True, text=True)
    if out.returncode != 0:
        return {}
    rows = {}
    for row in csv.DictReader(io.StringIO(out.stdout)):
        if row["op"] == "all":
            rows[int(row["point"])] = row
    return rows


def generators(sweep):
    gens = []
    for name in sorted(os.listdir(sweep)):
        path = os.path.join(sweep, name, "summary.json")
        if re.fullmatch(r"gen\d+", name) and os.path.isfile(path):
            gens.append(load_json(path))
    return gens


def sweep_rows(analysis, run_dir, name, floor):
    m = NAME.match(name)
    sweep = os.path.join(run_dir, name)
    gens = generators(sweep)
    if not m or not gens:
        return []
    lat = merged(analysis, sweep)
    cpu = {r["point"]: r for r in jsonl(os.path.join(sweep, "server-cpu.jsonl"))}
    rows = []
    for index in sorted({p["point"] for g in gens for p in g["points"]}):
        points = [p for g in gens for p in g["points"] if p["point"] == index]
        offered = sum(p["offered_rate"] for p in points)
        completed = sum(p["completed_rate"] for p in points)
        l = lat.get(index, {})
        c = cpu.get(index, {})
        row = {
            "block": os.path.basename(os.path.normpath(run_dir)),
            "profile": m["tag"],
            "cores": int(m["cores"]),
            "server": m["server"],
            "mapping": m["mapping"],
            "shape": m["shape"],
            "point": index,
            "offered": offered,
            "completed": completed,
            "saturated": completed < SATURATED * offered,
            "valid": all(p["validity"]["valid"] for p in points),
            "reasons": sorted({r for p in points for r in p["validity"]["reasons"]}),
            "errors": sum(p["errors"]["replies"] + p["errors"]["timeouts"] + p["errors"]["failed"] for p in points),
            "lag_p99_us": max(p["lag"]["p99_us"] for p in points),
            "lag_p999_us": max(p["lag"]["p999_us"] for p in points),
            "util_max": max(p["generator"]["util_max"] for p in points),
            "retrans": sum(p.get("tcp_retrans") or 0 for p in points),
            "p50_us": float(l["p50_us"]) if l else None,
            "p99_us": float(l["p99_us"]) if l else None,
            "p999_us": float(l["p99.9_us"]) if l else None,
            "floor_us": floor,
            "server_cores": c.get("server_cpu_cores"),
            "server_ns_per_op": c.get("server_cpu_ns_per_op") if l else None,
            "server_mem_bytes": c.get("server_mem_bytes"),
        }
        if c and not c.get("server_mem_bytes"):
            row["valid"] = False
            row["reasons"].append("no server process sampled (server down; generator files may be stale)")
        rows.append(row)
    return rows


def fmt(value, digits=1):
    if value is None:
        return "-"
    return f"{value:,.{digits}f}"


def rate(r):
    return f"{r / 1e3:,.0f}k" if r < 1e6 else f"{r / 1e6:.2f}M"


def excess(row, key):
    if row[key] is None or row["floor_us"] is None:
        return None
    return row[key] - row["floor_us"]


def print_points(rows):
    print("| Block | Sweep | Offered | Completed | Valid | p50 / p99 / p99.9 us | Excess p50 / p99 us | Lag p99 / p99.9 us | Server cores | Server ns/op | Memory MB |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        sweep = f"c{r['cores']} {r['server']} {r['mapping']} {r['shape']}"
        valid = "yes" if r["valid"] and not r["errors"] else "no: " + "; ".join(r["reasons"])[:60]
        mem = r["server_mem_bytes"] / 1e6 if r["server_mem_bytes"] else None
        print(f"| {r['block']} | {sweep} | {rate(r['offered'])} | {rate(r['completed'])}{' sat' if r['saturated'] else ''} | {valid} | "
              f"{fmt(r['p50_us'])} / {fmt(r['p99_us'])} / {fmt(r['p999_us'])} | {fmt(excess(r, 'p50_us'))} / {fmt(excess(r, 'p99_us'))} | "
              f"{fmt(r['lag_p99_us'])} / {fmt(r['lag_p999_us'])} | {fmt(r['server_cores'], 2)} | {fmt(r['server_ns_per_op'], 0)} | {fmt(mem, 0)} |")


def eligible(r):
    return r["valid"] and not r["errors"] and not r["saturated"] and r["p99_us"] is not None


def server_failed(r):
    return bool(r["errors"]) or r["saturated"]


def crossing(pts):
    pts = sorted(pts, key=lambda r: r["offered"])
    first = next((i for i, r in enumerate(pts) if eligible(r)), None)
    if first is None:
        return (None, "no valid point")
    if pts[first]["p99_us"] >= BOUND_US:
        return (None, f"below {rate(pts[first]['offered'])}")
    over = next((i for i, r in enumerate(pts) if i > first and (server_failed(r) or (eligible(r) and r["p99_us"] >= BOUND_US))), None)
    if over is None:
        lo = max((r for r in pts if eligible(r)), key=lambda r: r["offered"])
        return (None, f"at least {rate(lo['offered'])}")
    lo = next(r for r in reversed(pts[:over]) if eligible(r))
    hi = pts[over]
    later = [rate(r["offered"]) for r in pts[over + 1:] if eligible(r) and r["p99_us"] < BOUND_US]
    note = f"; non-monotonic, under 1 ms again at {', '.join(later)}" if later else ""
    skipped = [rate(r["offered"]) for r in pts[first:over] if not eligible(r)]
    if skipped:
        note += f"; skipped lag-invalid {', '.join(skipped)}"
    if not eligible(hi):
        return (None, f"{rate(lo['offered'])} to {rate(hi['offered'])}, upper point failed{note}")
    span = math.log(hi["p99_us"]) - math.log(lo["p99_us"])
    x = lo["offered"] + (math.log(BOUND_US) - math.log(lo["p99_us"])) / span * (hi["offered"] - lo["offered"])
    if hi["offered"] > COARSE * lo["offered"]:
        note = f"; coarse, bracket {rate(lo['offered'])} to {rate(hi['offered'])}" + note
    return (x, f"{rate(x)}{note}")


def print_summary(rows):
    kinds = {}
    for r in rows:
        kinds.setdefault((r["profile"], r["cores"], r["server"], r["mapping"], r["shape"]), []).append(r)
    print("\n## Summary across blocks (median, min to max)\n")
    print("| Profile | Cores | Server | Mapping | Shape | Blocks | p99 = 1 ms crossing ops/s | Per core | Highest valid rate | p99 excess at lowest / second point us | Server ns/op at second point |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for key in sorted(kinds):
        group = kinds[key]
        blocks = sorted({r["block"] for r in group})
        cross, cross_text, highest, p99_low, p99_second, ns_second = [], [], [], [], [], []
        for block in blocks:
            pts = sorted((r for r in group if r["block"] == block), key=lambda r: r["point"])
            value, text = crossing(pts)
            cross_text.append(text)
            if value is not None:
                cross.append(value)
            valid = [r["offered"] for r in pts if r["valid"] and not r["errors"] and not r["saturated"]]
            highest.append(max(valid) if valid else 0.0)
            if pts and excess(pts[0], "p99_us") is not None:
                p99_low.append(excess(pts[0], "p99_us"))
            if len(pts) > 1:
                if excess(pts[1], "p99_us") is not None:
                    p99_second.append(excess(pts[1], "p99_us"))
                if pts[1]["server_ns_per_op"] is not None:
                    ns_second.append(pts[1]["server_ns_per_op"])

        def spread(values, scale=None, digits=0):
            if not values:
                return "-"
            f = scale or (lambda v: fmt(v, digits))
            return f"{f(statistics.median(values))} ({f(min(values))} to {f(max(values))})"

        cores = key[1]
        if len(cross) == len(blocks) and len(blocks) > 1:
            cross_cell = spread(cross, rate)
            core_cell = spread([c / cores for c in cross], rate)
        else:
            cross_cell = "; ".join(cross_text)
            core_cell = "; ".join(rate(c / cores) for c in cross) or "-"
        print(f"| {key[0]} | {cores} | {key[2]} | {key[3]} | {key[4]} | {len(blocks)} | {cross_cell} | {core_cell} | {spread(highest, rate)} | "
              f"{spread(p99_low, digits=1)} / {spread(p99_second, digits=1)} | {spread(ns_second)} |")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("runs", nargs="+")
    parser.add_argument("--analysis", default="analysis", help="path of the analysis binary")
    parser.add_argument("--jsonl", help="also write every point row to this file")
    args = parser.parse_args()
    rows = []
    for run_dir in args.runs:
        if not os.path.isdir(run_dir):
            sys.exit(f"{run_dir} is not a directory")
        floor = rtt_floor_us(run_dir)
        for name in sorted(os.listdir(run_dir)):
            if os.path.isdir(os.path.join(run_dir, name)):
                rows.extend(sweep_rows(args.analysis, run_dir, name, floor))
        shortfall = {s["name"] for s in jsonl(os.path.join(run_dir, "shortfall.jsonl"))}
        print(f"{os.path.basename(os.path.normpath(run_dir))}: RTT floor {fmt(floor)} us, {len(shortfall)} sweeps with rates above the generator-safe ceiling", file=sys.stderr)
    if args.jsonl:
        with open(args.jsonl, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
    print_points(rows)
    print_summary(rows)


if __name__ == "__main__":
    main()
