#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
	cat <<'USAGE'
usage: competitors.sh

Pulls every competitor image pinned in versions.env (tag and digest) and the Fluvio CLI (checksum checked), and writes
/opt/keep/baselines/versions.txt: tag and digest per image, the CLI, each server's own version line. Idempotent.
USAGE
}

[[ ${1:-} != -h && ${1:-} != --help ]] || { usage; exit 0; }
require_root
require_var VALKEY REDIS DRAGONFLY GARNET POGOCACHE MEMCACHED NATS KAFKA ZENOH FLUVIO DEBIAN \
	FLUVIO_CLI_URL FLUVIO_CLI_SHA256_X86_64 FLUVIO_CLI_SHA256_AARCH64
command -v docker >/dev/null || die "docker is missing (provision docker first)"
dir=$KEEP_DIR/baselines
mkdir -p "$dir/fluvio"
versions=$dir/versions.txt.tmp
: >"$versions"
for image in "$VALKEY" "$REDIS" "$DRAGONFLY" "$GARNET" "$POGOCACHE" "$MEMCACHED" "$NATS" "$KAFKA" "$ZENOH" "$FLUVIO" "$DEBIAN"; do
	docker pull -q "$image" >/dev/null
	echo "$image $(docker image inspect --format '{{index .RepoDigests 0}}' "$image")" >>"$versions"
done
case $(uname -m) in
x86_64) arch=x86_64 sha=$FLUVIO_CLI_SHA256_X86_64 ;;
aarch64) arch=aarch64 sha=$FLUVIO_CLI_SHA256_AARCH64 ;;
*) die "no Fluvio CLI for $(uname -m)" ;;
esac
if ! [[ -x $dir/fluvio/fluvio ]]; then
	curl -fsSL --retry 5 -o "$dir/fluvio/cli.zip" "$FLUVIO_CLI_URL/fluvio-$arch-unknown-linux-musl.zip"
	echo "$sha  $dir/fluvio/cli.zip" | sha256sum -c --quiet || die "Fluvio CLI checksum mismatch"
	python3 -m zipfile -e "$dir/fluvio/cli.zip" "$dir/fluvio"
	chmod +x "$dir/fluvio/fluvio"
	rm -f "$dir/fluvio/cli.zip"
fi
echo "fluvio-cli $arch $sha" >>"$versions"
{
	docker run --rm "$VALKEY" valkey-server --version
	docker run --rm "$REDIS" redis-server --version
	docker run --rm "$DRAGONFLY" --version
	docker run --rm "$MEMCACHED" memcached -V
	docker run --rm "$NATS" --version
} >>"$versions" 2>&1 || true
mv "$versions" "$dir/versions.txt"
chown -R "$(admin_user):" "$dir"
log "competitor images pulled: $(wc -l <"$dir/versions.txt") lines in $dir/versions.txt"
