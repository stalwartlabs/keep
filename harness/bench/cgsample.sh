#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
interval=${1:-1}
while :; do
  t=$(date +%s%N)
  cpu=0
  mem=0
  for cg in /sys/fs/cgroup/system.slice/docker-*.scope; do
    [ -d "$cg" ] || continue
    usage=$(awk '$1 == "usage_usec" { print $2 }' "$cg/cpu.stat")
    memory=$(cat "$cg/memory.current")
    cpu=$(( cpu + usage ))
    mem=$(( mem + memory ))
  done
  printf '{"t_ns":%s,"cpu_us":%s,"mem_bytes":%s}\n' "$t" "$cpu" "$mem"
  sleep "$interval"
done
