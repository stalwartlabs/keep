#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
. "$(dirname "$0")/lib.sh"

usage() {
  cat <<'USAGE'
usage: up.sh TOPOLOGY --run RUN [--location ROLE=LOC]... [--rg-prefix P] [--no-network] [--no-vms] [--skip-quota]
             [--dry-run]

Deploys a topology on Azure: quota check, resource groups, NSGs and VNets, proximity placement groups and
VMs, global peering, inventory. Every step is idempotent, so rerunning resumes after a failure.
--no-network and --no-vms skip the steps for resource groups and VMs created by other tooling (the VNet
must then be named "vnet" with subnet "main" and NSG "nsg"). Prints the inventory path
(HARNESS_WORK/RUN/hosts.json).
USAGE
}

main() {
  local network=1 vms=1 quota=1 args=() here
  here="$(dirname "$0")"
  while [ $# -gt 0 ]; do
    case "$1" in
      --no-network) network=0; shift ;;
      --no-vms) vms=0; shift ;;
      --skip-quota) quota=0; shift ;;
      *) common_arg "$@"; args+=("${@:1:$SHIFT}"); shift "$SHIFT" ;;
    esac
  done
  finish_common
  if [ "$quota" = 1 ] && [ "$vms" = 1 ]; then "$here/quota.sh" "${args[@]}"; fi
  if [ "$network" = 1 ]; then "$here/network.sh" create "${args[@]}"; fi
  if [ "$vms" = 1 ]; then "$here/vms.sh" create "${args[@]}"; fi
  if [ "$(topo '.peerings | length')" -gt 0 ]; then "$here/network.sh" peer "${args[@]}"; fi
  "$here/inventory.sh" "${args[@]}"
}

main "$@"
