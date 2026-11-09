#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../lib/common.sh
. "$HERE/../lib/common.sh"

usage() {
  cat <<'USAGE'
usage: harness/jepsen/jepsen.sh HOSTS.json STEP [ARGS]

The Keep Jepsen suite on a provisioned cluster (topology jepsen.json: control host ctl, DB hosts of role
db; setup.sh provisions them, build.sh HOSTS.json jepsen installs keep and keep-jepsen).
  install        copies the lein project, run.sh and series.sh to /root/jepsen on the control host, writes
                 /root/nodes.txt from the inventory's db hosts, checks root SSH to every node, and prepares
                 the control clock (chrony and systemd-timesyncd stopped, kernel tick and frequency reset
                 to nominal: the checker times on the control node)
  series ARG...  series.sh ARG... on the control host as a detached root job (series core, network,
                 lease, readback, tls, arm, smoke, or run names); waits for it
  fetch          copies /root/out from the control host into OUT/jepsen
  finish         restarts chrony on the control host
USAGE
}

RESET_SOURCE='#include <sys/timex.h>
int main(void) {
  struct timex t = {0};
  t.modes = ADJ_TICK | ADJ_FREQUENCY;
  t.tick = 10000;
  t.freq = 0;
  return adjtimex(&t) < 0;
}'

case "${1:-}" in -h|--help|"") usage; [ -n "${1:-}" ]; exit $? ;; esac
load_inventory "$1"
shift
CTL=$(host_names control | head -1)
[ -n "$CTL" ] || die "the inventory has no host with role control"

install() {
  local nodes
  nodes=$(for host in $(host_names db); do host_ip "$host"; done)
  [ -n "$nodes" ] || die "the inventory has no db hosts"
  tar -C "$HERE" -czf - project.clj run.sh series.sh src test \
    | hssh "$CTL" "sudo rm -rf /root/jepsen && sudo mkdir -p /root/jepsen /root/out && sudo tar -xzf - -C /root/jepsen && sudo chmod +x /root/jepsen/run.sh /root/jepsen/series.sh"
  printf '%s\n' "$nodes" | hssh "$CTL" "sudo tee /root/nodes.txt > /dev/null"
  printf '%s\n' "$RESET_SOURCE" | hssh "$CTL" "sudo tee /root/keep-reset-rate.c > /dev/null"
  hssh "$CTL" "nodes=\$(sudo cat /root/nodes.txt) && test -n \"\$nodes\" || exit 1
for n in \$nodes; do sudo ssh -o StrictHostKeyChecking=accept-new -o BatchMode=yes root@\$n 'echo ok \$(hostname)' || exit 1; done
sudo systemctl stop chrony systemd-timesyncd 2> /dev/null || true
sudo gcc -o /usr/local/bin/keep-reset-rate /root/keep-reset-rate.c && sudo /usr/local/bin/keep-reset-rate && echo clock reset
test -x /opt/keep/bin/keep && test -x /opt/keep/bin/keep-jepsen && sha256sum /opt/keep/bin/keep /opt/keep/bin/keep-jepsen" < /dev/null
}

case "${1:-}" in
  install) install ;;
  series)
    shift
    [ $# -ge 1 ] || die "series needs a preset or run ids"
    rc=0
    job_run "$CTL" "jepsen-$(printf '%s-' "$@" | tr -c 'A-Za-z0-9._-' -)" --again --sudo -- bash /root/jepsen/series.sh "$@" < /dev/null || rc=$?
    hssh "$CTL" "sudo cat /root/out/series.log" < /dev/null | tail -n 20
    exit "$rc"
    ;;
  fetch)
    mkdir -p "$OUT/jepsen"
    hssh "$CTL" "sudo tar -C /root/out -czf - ." < /dev/null | tar -xzf - -C "$OUT/jepsen"
    say "fetched into $OUT/jepsen"
    ;;
  finish) hssh "$CTL" "sudo systemctl start chrony 2> /dev/null || sudo systemctl start systemd-timesyncd" < /dev/null ;;
  *) usage >&2; exit 2 ;;
esac
