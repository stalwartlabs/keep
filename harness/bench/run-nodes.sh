#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
set -euo pipefail

BIN=${BIN:-mock-stalwart}
NODES=${NODES:-3}
FIRST=${FIRST:-1}
EXPECTED=${EXPECTED:-$NODES}
OUT=${OUT:-out}
LEAD=${LEAD:-5}
CPUS_PER_NODE=${CPUS_PER_NODE:-0}

start=${START:-$(python3 -c "import time; print(f'{time.time() + ${LEAD}:.3f}')")}
mkdir -p "$OUT"
pids=()
for ((i = 0; i < NODES; i++)); do
    node=$((FIRST + i))
    pin=()
    if [[ "$CPUS_PER_NODE" -gt 0 ]] && command -v taskset >/dev/null; then
        pin=(taskset -c "$((i * CPUS_PER_NODE))-$((i * CPUS_PER_NODE + CPUS_PER_NODE - 1))")
    fi
    ${pin[@]+"${pin[@]}"} "$BIN" run --node-id "$node" --start-at "$start" --expected-nodes "$EXPECTED" \
        --out "$OUT" "$@" >"$OUT/node-$node.log" 2>&1 &
    pids+=($!)
done
status=0
for pid in "${pids[@]}"; do
    wait "$pid" || status=1
done
"$BIN" merge "$OUT"/node-*.hlog "$OUT"/node-*.json --json "$OUT/merged.json"
exit $status
