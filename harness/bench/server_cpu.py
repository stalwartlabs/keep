# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
import argparse
import bisect
import json
import pathlib


def load_samples(path):
    samples = []
    for line in pathlib.Path(path).read_text().splitlines():
        if line.startswith("{"):
            sample = json.loads(line)
            samples.append((sample["t_ns"], sample["cpu_us"], sample["mem_bytes"]))
    samples.sort()
    return samples


def at(samples, t_ns):
    times = [s[0] for s in samples]
    i = bisect.bisect_left(times, t_ns)
    return samples[min(i, len(samples) - 1)]


def arg_seconds(argv, name, default):
    for flag, value in zip(argv, argv[1:]):
        if flag == name:
            return float(value)
    return default


def windows(summary):
    points = summary["points"]
    if all("meas_start_unix_ns" in p for p in points):
        return {p["point"]: (p["meas_start_unix_ns"], p["meas_end_unix_ns"]) for p in points}
    argv = summary["args"]
    warmup = arg_seconds(argv, "--warmup", 30.0)
    settle = arg_seconds(argv, "--settle", 5.0)
    cursor = summary["clock"]["unix_base_ns"] if "clock" in summary else summary["start_unix_ms"] * 1_000_000
    result = {}
    for p in sorted(points, key=lambda p: p["point"]):
        cursor += int((warmup if p["point"] == 0 else settle) * 1e9)
        end = cursor + int(p["measure_s"] * 1e9)
        result[p["point"]] = (cursor, end)
        cursor = end
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Server CPU per op and memory per load point: joins loadgen summaries (one or more generator hosts of one run) with baselines/servers.sh sample output"
    )
    parser.add_argument("--samples", required=True, help="cgsample.jsonl from the server VM")
    parser.add_argument("runs", nargs="+", help="loadgen output directories of the generator hosts of one run")
    args = parser.parse_args()
    samples = load_samples(args.samples)
    points = {}
    for run in args.runs:
        summary = json.loads((pathlib.Path(run) / "summary.json").read_text())
        spans = windows(summary)
        for point in summary["points"]:
            start, end = spans[point["point"]]
            entry = points.setdefault(point["point"], {"start": start, "end": end, "completed": 0, "rate": 0.0})
            entry["completed"] += point["completed"]
            entry["rate"] += point["completed_rate"]
    rows = []
    for index in sorted(points):
        point = points[index]
        first, last = at(samples, point["start"]), at(samples, point["end"])
        cpu_us = last[1] - first[1]
        seconds = max(last[0] - first[0], 1) / 1e9
        rows.append({
            "point": index,
            "completed_rate": round(point["rate"], 1),
            "server_cpu_cores": round(cpu_us / 1e6 / seconds, 3),
            "server_cpu_ns_per_op": round(cpu_us / seconds * 1e3 / max(point["rate"], 1e-9), 1),
            "server_mem_bytes": last[2],
        })
    for row in rows:
        print(json.dumps(row))


if __name__ == "__main__":
    main()
