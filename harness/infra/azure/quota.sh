#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
. "$(dirname "$0")/lib.sh"

usage() {
  cat <<'USAGE'
usage: quota.sh TOPOLOGY --run RUN [--location ROLE=LOC]...

Pre-flight checks for a topology: every size is offered without restriction in its location, and the
regional and per-family vCPU quotas leave room for the VMs that do not exist yet. Exit 1 when a check fails.
USAGE
}

check_location() {
  local loc="$1" need skus usage
  need=$(jq -c --arg loc "$loc" '[.[] | select(.location == $loc)]' <<< "$PLAN")
  if dry; then az_read vm list-skus -l "$loc" --resource-type virtualMachines -o json || true; az_read vm list-usage -l "$loc" -o json || true; return 0; fi
  skus=$(az vm list-skus -l "$loc" --resource-type virtualMachines -o json | jq -c '[.[] | {name,
    family, restricted: ((.restrictions // []) | length > 0),
    vcpus: ([.capabilities[]? | select(.name == "vCPUs") | .value | tonumber][0])}]')
  usage=$(az vm list-usage -l "$loc" -o json)
  jq -nr --argjson need "$need" --argjson skus "$skus" --argjson usage "$usage" --arg loc "$loc" '
    def usage($k): [$usage[] | select((.name.value | ascii_downcase) == ($k | ascii_downcase))][0];
    [$need[] | . as $v | ($skus[] | select(.name == $v.size)) as $s
      | {size: $v.size, name: $v.name, exists: $v.exists, family: $s.family, vcpus: $s.vcpus, restricted: $s.restricted}] as $rows
    | ($need | map(.size) - ($rows | map(.size)) | unique) as $missing
    | (if ($missing | length) > 0 then "FAIL \($loc): sizes not offered: \($missing | join(" "))" else empty end),
      ($rows[] | select(.restricted) | "FAIL \($loc): \(.size) is restricted for this subscription"),
      ([$rows[] | select(.exists | not)] as $new
       | ([$new[].vcpus] | add // 0) as $total
       | usage("cores") as $c
       | "\(if (($c.limit | tonumber) - ($c.currentValue | tonumber)) >= $total then "ok  " else "FAIL" end) \($loc): regional vCPUs \($c.currentValue)/\($c.limit) used, need \($total) more",
         ($new | group_by(.family)[] | (map(.vcpus) | add) as $n | usage(.[0].family) as $f
          | if $f == null then "ok   \($loc): \(.[0].family) has no separate quota"
            else "\(if (($f.limit | tonumber) - ($f.currentValue | tonumber)) >= $n then "ok  " else "FAIL" end) \($loc): \($f.name.localizedValue) \($f.currentValue)/\($f.limit) used, need \($n) more" end))'
}

main() {
  local out loc existing name
  while [ $# -gt 0 ]; do common_arg "$@"; shift "$SHIFT"; done
  finish_common
  PLAN="[]"
  for name in $(vm_names); do
    existing=false
    if ! dry && az vm show -g "$(rg_name "$(vm_field "$name" region)")" -n "$name" -o none 2> /dev/null; then existing=true; fi
    PLAN=$(jq -c --arg n "$name" --arg s "$(vm_field "$name" size)" --arg loc "$(region_location "$(vm_field "$name" region)")" \
      --argjson e "$existing" '. + [{name: $n, size: $s, location: $loc, exists: $e}]' <<< "$PLAN")
  done
  out=""
  for loc in $(jq -r '[.[].location] | unique[]' <<< "$PLAN"); do
    out="$out$(check_location "$loc")"$'\n'
  done
  printf '%s' "$out"
  if printf '%s' "$out" | grep -q '^FAIL'; then exit 1; fi
}

main "$@"
