#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
set -euo pipefail

usage() {
  cat <<'USAGE'
usage: fill.sh start | stop

As root: start runs one SCHED_IDLE spinner per vCPU (systemd units keep-fill-N in keepfill.slice, CPU
weight idle), so no vCPU of the VM halts while a benchmark runs and every runnable thread preempts the
spinners; stop removes them. Exit 1 when a spinner is not active in the idle slice.
USAGE
}

UNIT=keep-fill-
SLICE=keepfill.slice

stop() {
  local units
  units=$(systemctl list-units --plain --no-legend --all "$UNIT*" | awk '{ print $1 }')
  if [ -n "$units" ]; then
    # shellcheck disable=SC2086
    systemctl stop $units
  fi
}

start() {
  local cpu last state slice deadline
  stop
  if [ ! -f "/run/systemd/system/$SLICE" ]; then
    mkdir -p /run/systemd/system
    printf '[Unit]\nDescription=keep harness idle fill\n\n[Slice]\nCPUWeight=idle\n' > "/run/systemd/system/$SLICE"
    systemctl daemon-reload
  fi
  last=$(( $(nproc) - 1 ))
  for cpu in $(seq 0 "$last"); do
    systemd-run --unit="$UNIT$cpu" --slice="$SLICE" --collect --quiet --property="CPUAffinity=$cpu" \
      --property=CPUSchedulingPolicy=idle --property=CPUWeight=idle /bin/sh -c 'while :; do :; done'
  done
  deadline=$(( SECONDS + 10 ))
  for cpu in $(seq 0 "$last"); do
    while :; do
      state=$(systemctl show -p ActiveState --value "$UNIT$cpu.service")
      slice=$(systemctl show -p Slice --value "$UNIT$cpu.service")
      [ "$state" = active ] && [ "$slice" = "$SLICE" ] && break
      [ "$SECONDS" -lt "$deadline" ] || { echo "fill.sh: $UNIT$cpu is $state in $slice" >&2; exit 1; }
      sleep 0.5
    done
  done
  [ "$(cat "/sys/fs/cgroup/$SLICE/cpu.idle")" = 1 ] || { echo "fill.sh: $SLICE is not idle" >&2; exit 1; }
  echo "fill: $(( last + 1 )) spinners"
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  -h|--help) usage ;;
  *) usage >&2; exit 2 ;;
esac
