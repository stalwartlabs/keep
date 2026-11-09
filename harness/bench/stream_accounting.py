#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
import glob
import json
import os
import sys


def mean(values):
    present = [v for v in values if v]
    return sum(present) / len(present) if present else None


def window_spread_ms(reports):
    starts = [report["window_start_unix_ns"] for report in reports if report.get("window_start_unix_ns")]
    return (max(starts) - min(starts)) / 1e6 if starts else None


def account(directory):
    reports = [json.load(open(path)) for path in sorted(glob.glob(os.path.join(directory, "node-*.json")))]
    nodes = [report["stream"]["counters"] for report in reports if report.get("stream")]
    if not nodes:
        return None
    total = lambda key: sum(node[key] for node in nodes)
    streams = [report["stream"] for report in reports if report.get("stream")]
    published = total("events_published")
    first = reports[0]["stream"]
    return {
        "dir": os.path.basename(os.path.normpath(directory)),
        "nodes": len(nodes),
        "events_published": published,
        "messages_published": total("messages_published"),
        "events_received": total("events_received"),
        "events_expected": published * (len(nodes) - 1),
        "gaps": total("gaps"),
        "gap_messages": total("gap_messages"),
        "verify_failures": total("verify_failures"),
        "duplicates": total("duplicates"),
        "end_markers": total("end_markers_received"),
        "end_markers_expected": len(nodes) * (len(nodes) - 1),
        "deliveries_complete": total("deliveries_complete"),
        "deliveries_incomplete": total("deliveries_incomplete"),
        "disconnects": total("disconnects"),
        "node1_oneway_p50_us": first["oneway"]["p50"] / 1e3,
        "node1_echo_p50_us": first["echo"]["p50"] / 1e3,
        "node1_echo_p99_us": first["echo"]["p99"] / 1e3,
        "node1_echo_last_p99_us": first["echo_last"]["p99"] / 1e3,
        "echo_last_p99_us_max": max(stream["echo_last"]["p99"] for stream in streams) / 1e3,
        "event_lag_p999_us_max": max(stream["lag"]["p999"] for stream in streams) / 1e3,
        "publish_errors": total("publish_errors"),
        "producer_stalls": total("producer_stalls"),
        "client_cores_max": max(report["cpu"]["client_cores"] for report in reports),
        "window_start_spread_ms": window_spread_ms(reports),
        "client_ns_per_event_received_mean": mean(report["cpu"].get("client_ns_per_event_received") for report in reports),
        "client_ns_per_event_received_max": max((report["cpu"].get("client_ns_per_event_received") or 0) for report in reports),
    }


if __name__ == "__main__":
    for directory in sys.argv[1:]:
        row = account(directory)
        if row:
            print(json.dumps(row))
