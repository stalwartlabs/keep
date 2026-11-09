#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
	cat <<'USAGE'
usage: jepsen-node.sh [PUBKEY]

Jepsen DB node: gcc and rsync (Jepsen's own OS setup installs the rest), root SSH login by key only and,
with PUBKEY (the control node's root key), that key in root's authorized_keys. Idempotent.
USAGE
}

[[ ${1:-} != -h && ${1:-} != --help ]] || { usage; exit 0; }
require_root
apt_install gcc rsync iptables
mkdir -p /root/.ssh
chmod 700 /root/.ssh
if [[ -n ${1:-} ]]; then
	touch /root/.ssh/authorized_keys
	grep -qxF "$1" /root/.ssh/authorized_keys || echo "$1" >>/root/.ssh/authorized_keys
	chmod 600 /root/.ssh/authorized_keys
fi
if ! grep -q '^PermitRootLogin prohibit-password' /etc/ssh/sshd_config; then
	sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config
	grep -q '^PermitRootLogin' /etc/ssh/sshd_config || echo 'PermitRootLogin prohibit-password' >>/etc/ssh/sshd_config
	systemctl reload ssh
fi
log "jepsen node ready"
