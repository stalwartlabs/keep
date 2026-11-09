#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
set -euo pipefail
usage() {
  cat <<'USAGE'
usage: run.sh NODES_FILE BIN_DIR [jepsen test options...]
Runs on the Jepsen control node (Debian, lein, ssh as root to every node). NODES_FILE lists one node IP per
line; BIN_DIR holds the Linux keep and keep-jepsen binaries (harness/build.sh HOSTS.json jepsen installs them
in /opt/keep/bin). Starts keep-jepsen proxy on 127.0.0.1:7100, then lein run test. KEEP_ROUTE=none adds route=none to the proxy's
client URL (the unrouted arm: alternate it with the default between runs). KEEP_TLS=rpk runs every node with
--tls rpk: keep-jepsen rpk-keys writes one Ed25519 key per node (in NODES_FILE order) and a client key under
tls/ next to this script, the nodes get theirs at setup, and the proxy connects with tls=rpk; KEEP_TLS=off (default) runs
plaintext. KEEP_PRESSURE=on caps every node at 32 MiB and runs keep-jepsen large-writer beside the workload, on keys of
its own and outside the history; KEEP_PRESSURE=off (default) runs without. A second run.sh on the same control node
refuses to start while one runs.
Examples: --workload lock --nemesis kill,pause,partition,clock --time-limit 300
          --workload register --nemesis leave --time-limit 300
          --workload register-failover --nemesis kill,partition --time-limit 300
          --workload readback --nemesis heal-restart --time-limit 300
USAGE
}
[ $# -ge 2 ] || { usage >&2; exit 2; }
NODES=$(realpath "$1")
BIN=$(realpath "$2")
shift 2
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
exec 9> "$HERE/.run.lock"
flock -n 9 || { echo "run.sh: another run holds $HERE/.run.lock" >&2; exit 2; }
SECRET=00112233445566778899aabbccddeeff
URL="keep://$(sed '/^$/d; s/$/:7000/' "$NODES" | paste -sd, -)?cluster=on&secret=$SECRET&timeout=ms:500&deadline=ms:500&connect_timeout=ms:500"
case "${KEEP_ROUTE:-default}" in
  default) ;;
  none) URL="$URL&route=none" ;;
  *) echo "run.sh: KEEP_ROUTE must be default or none" >&2; exit 2 ;;
esac
echo "jepsen: client route ${KEEP_ROUTE:-default}"
TLS=()
case "${KEEP_TLS:-off}" in
  off) ;;
  rpk)
    TLS_DIR=$HERE/tls
    rm -rf "$TLS_DIR"
    "$BIN/keep-jepsen" rpk-keys --out "$TLS_DIR" --servers "$(grep -c '[^[:space:]]' "$NODES")"
    URL="$URL&tls=rpk&key=$TLS_DIR/client.key&servers=$TLS_DIR/servers.pem"
    TLS=(--tls-dir "$TLS_DIR")
    ;;
  *) echo "run.sh: KEEP_TLS must be off or rpk" >&2; exit 2 ;;
esac
echo "jepsen: tls ${KEEP_TLS:-off}"
"$BIN/keep-jepsen" proxy --listen 127.0.0.1:7100 --url "$URL" > "$HERE/proxy.log" 2>&1 &
PROXY=$!
trap 'kill $PROXY 2> /dev/null || true' EXIT
PRESSURE=()
case "${KEEP_PRESSURE:-off}" in
  off) ;;
  on)
    "$BIN/keep-jepsen" large-writer --url "$URL" > "$HERE/large-writer.log" 2>&1 &
    WRITER=$!
    trap 'kill $PROXY $WRITER 2> /dev/null || true' EXIT
    PRESSURE=(--memory 33554432)
    ;;
  *) echo "run.sh: KEEP_PRESSURE must be off or on" >&2; exit 2 ;;
esac
echo "jepsen: pressure ${KEEP_PRESSURE:-off}"
cd "$HERE"
KEY=()
[ -f "$HOME/.ssh/id_rsa" ] && KEY=(--ssh-private-key "$HOME/.ssh/id_rsa")
lein run test --nodes-file "$NODES" --username root "${KEY[@]}" --keep-bin "$BIN/keep" --keep-jepsen "$BIN/keep-jepsen" "${TLS[@]}" "${PRESSURE[@]}" "$@"
