#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
	cat <<'USAGE'
usage: docker.sh

Installs Docker (Debian's docker.io and docker-cli) for the competitor containers and lets the admin user run it. Idempotent.
USAGE
}

[[ ${1:-} != -h && ${1:-} != --help ]] || { usage; exit 0; }
require_root
apt_install docker.io docker-cli
command -v docker >/dev/null || die "docker CLI missing after the install"
systemctl enable --now docker >/dev/null 2>&1
usermod -aG docker "$(admin_user)"
log "docker $(docker version --format '{{.Server.Version}}')"
