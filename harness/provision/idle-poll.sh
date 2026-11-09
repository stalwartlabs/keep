#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
	cat <<'USAGE'
usage: idle-poll.sh

Adds idle=poll to the kernel command line (a GRUB drop-in, then update-grub), so no vCPU halts between
requests: the condition of every published latency row, on every server and client VM alike. Takes
effect after a reboot, which setup.sh performs and then checks in /proc/cmdline. Idempotent.
USAGE
}

[[ ${1:-} != -h && ${1:-} != --help ]] || { usage; exit 0; }
require_root
DROPIN=/etc/default/grub.d/99-keep-harness-idle.cfg
mkdir -p /etc/default/grub.d
text='GRUB_CMDLINE_LINUX_DEFAULT="$GRUB_CMDLINE_LINUX_DEFAULT idle=poll"'
if [[ "$(cat "$DROPIN" 2>/dev/null)" != "$text" ]]; then
	printf '%s\n' "$text" >"$DROPIN"
	update-grub
fi
if grep -qw idle=poll /proc/cmdline; then
	log "idle=poll active"
else
	log "idle=poll set; reboot to apply"
fi
