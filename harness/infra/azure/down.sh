#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
. "$(dirname "$0")/lib.sh"

usage() {
  cat <<'USAGE'
usage: down.sh TOPOLOGY --run RUN [--rg-prefix P] [--dry-run]

Tears down every resource group of a topology run and waits until Azure reports them gone. Idempotent:
missing groups are skipped. Local state in HARNESS_WORK/RUN is kept (hosts.json is renamed hosts.json.down).
USAGE
}

main() {
  local role rg groups="" i left
  while [ $# -gt 0 ]; do common_arg "$@"; shift "$SHIFT"; done
  finish_common
  for role in $(region_roles); do
    rg=$(rg_name "$role")
    if dry; then az_do group delete -n "$rg" --yes --no-wait -o none > /dev/null; continue; fi
    if [ "$(az group exists -n "$rg")" = true ]; then
      say "deleting $rg"
      az group delete -n "$rg" --yes --no-wait -o none
      groups="$groups $rg"
    fi
  done
  dry && return 0
  for i in $(seq 1 120); do
    left=""
    for rg in $groups; do [ "$(az group exists -n "$rg")" = true ] && left="$left $rg"; done
    [ -z "$left" ] && break
    [ $((i % 6)) = 0 ] && say "waiting for$left"
    sleep 10
  done
  [ -z "$left" ] || die "still deleting after 20 minutes:$left (rerun down.sh)"
  [ -f "$(run_dir)/hosts.json" ] && mv "$(run_dir)/hosts.json" "$(run_dir)/hosts.json.down"
  say "run $RUN deleted"
}

main "$@"
