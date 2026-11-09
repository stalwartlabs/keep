#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
	cat <<'USAGE'
usage: jepsen-control.sh

Jepsen control node: JDK, Leiningen, gcc (the clock reset program), and a root SSH key whose public half
the DB nodes trust (printed as the last line, as "pubkey KEY"). Idempotent.
USAGE
}

[[ ${1:-} != -h && ${1:-} != --help ]] || { usage; exit 0; }
require_root
apt_install default-jdk-headless leiningen git gcc rsync openssh-client
mkdir -p /root/.ssh /root/jepsen /root/out
chmod 700 /root/.ssh
[[ -f /root/.ssh/id_rsa ]] || ssh-keygen -q -t rsa -b 4096 -N '' -f /root/.ssh/id_rsa
log "$(java -version 2>&1 | head -1); $(lein version 2>&1 | tail -1)"
echo "pubkey $(cat /root/.ssh/id_rsa.pub)"
