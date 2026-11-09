#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
	cat <<'USAGE'
usage: os-tuning.sh

The host settings every reported Keep number was measured with (as root): tools (ethtool, sysstat, perf,
bpftrace, jq, python3), the apt timers and unattended upgrades off, irqbalance left as the image has it
(absent on Debian 13, so the MANA driver's IRQ affinity stays as the driver sets it), the lab sysctls,
nofile 1048576, the Hyper-V KVP daemon on and the MANA receive ring at 8192 entries (kept by a udev rule).
Idempotent.
USAGE
}

[[ ${1:-} != -h && ${1:-} != --help ]] || { usage; exit 0; }
require_root

SYSCTL_FILE=/etc/sysctl.d/90-keep-harness.conf
LIMITS_FILE=/etc/security/limits.d/90-keep-harness.conf
RING_RULE=/etc/udev/rules.d/90-keep-harness-mana-ring.rules
RX_RING=8192

irqbalance_before=$(systemctl is-active irqbalance 2>/dev/null || true)
for unit in unattended-upgrades.service apt-daily.timer apt-daily-upgrade.timer; do
	systemctl disable --now "$unit" >/dev/null 2>&1 || true
done
apt_install ethtool sysstat iproute2 numactl jq curl zstd pciutils procps psmisc util-linux python3 rsync \
	netcat-openbsd bpftrace linux-perf hyperv-daemons
if [[ $irqbalance_before != active ]]; then
	systemctl disable --now irqbalance >/dev/null 2>&1 || true
fi
for unit in hv-kvp-daemon.service hv_kvp_daemon.service hyperv-daemons.hv-kvp-daemon.service; do
	systemctl enable --now "$unit" >/dev/null 2>&1 || true
done

cat >"$SYSCTL_FILE.tmp" <<'SYSCTL'
net.core.rmem_max = 67108864
net.core.wmem_max = 67108864
net.core.somaxconn = 65535
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
fs.file-max = 4194304
kernel.perf_event_paranoid = -1
kernel.kptr_restrict = 0
SYSCTL
mv "$SYSCTL_FILE.tmp" "$SYSCTL_FILE"
sysctl -q -p "$SYSCTL_FILE"
printf '* soft nofile 1048576\n* hard nofile 1048576\n' >"$LIMITS_FILE"

iface=$(ip -o route show default | awk '{ for (i = 1; i < NF; i++) if ($i == "dev") { print $(i + 1); exit } }')
vf=""
for lower in /sys/class/net/"$iface"/lower_*; do
	[[ -e $lower ]] && vf=${lower##*/lower_}
done
if [[ -n $vf ]]; then
	max=$(ethtool -g "$vf" 2>/dev/null | awk '/^Pre-set maximums/ { p = 1 } p && /^RX:/ { print $2; exit }')
	ring=$RX_RING
	if [[ $max =~ ^[0-9]+$ ]] && ((max < ring)); then ring=$max; fi
	ethtool -G "$vf" rx "$ring" 2>/dev/null || log "NIC $vf: rx ring $ring not settable"
	printf 'ACTION=="add", SUBSYSTEM=="net", DRIVERS=="mana", RUN+="/usr/sbin/ethtool -G $name rx %s"\n' "$ring" >"$RING_RULE"
	udevadm control --reload-rules
fi

mkdir -p "$KEEP_DIR/bin" "$KEEP_DIR/out"
chown -R "$(admin_user):" "$KEEP_DIR"
log "os tuning applied: kernel $(uname -r), vf ${vf:-none} rx $(ethtool -g "${vf:-lo}" 2>/dev/null | awk '/^Current/ { p = 1 } p && /^RX:/ { print $2; exit }')"
