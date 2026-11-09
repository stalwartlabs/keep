# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
# shellcheck shell=bash disable=SC2034

set -euo pipefail

HARNESS_ROOT=${HARNESS_ROOT:-/opt/keep-harness}
KEEP_DIR=/opt/keep

if [[ -r "$HARNESS_ROOT/versions.env" ]]; then
	# shellcheck source=/dev/null
	source "$HARNESS_ROOT/versions.env"
fi

export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=a

APT_OPTS=(-o DPkg::Lock::Timeout=900 -o Acquire::Retries=5 -o Acquire::http::Timeout=20 -o Acquire::https::Timeout=20)

log() {
	printf '%s [%s] %s\n' "$(date -u +%FT%TZ)" "${0##*/}" "$*" >&2
}

die() {
	log "ERROR: $*"
	exit 1
}

require_root() {
	[[ $EUID -eq 0 ]] || die "must run as root"
}

require_var() {
	local name
	for name in "$@"; do
		[[ -n ${!name:-} ]] || die "$name is not set (versions.env)"
	done
}

APT_UPDATED=0
apt_update_once() {
	if [[ $APT_UPDATED -eq 0 ]]; then
		apt-get "${APT_OPTS[@]}" update -q
		APT_UPDATED=1
	fi
}

apt_install() {
	local missing=() pkg
	for pkg in "$@"; do
		dpkg-query -W -f='${Status}' "${pkg%%=*}" 2>/dev/null | grep -q 'install ok installed' || missing+=("$pkg")
	done
	[[ ${#missing[@]} -eq 0 ]] && return 0
	apt_update_once
	apt-get "${APT_OPTS[@]}" install -y -q --no-install-recommends -o Dpkg::Options::=--force-confold "${missing[@]}"
}

admin_user() {
	stat -c %U "$HARNESS_ROOT"
}

install_file() {
	local src=$1 dest=$2 mode=${3:-0644}
	if [[ -f "$dest" ]] && cmp -s "$src" "$dest"; then
		return 1
	fi
	install -D -m "$mode" "$src" "$dest"
	return 0
}
