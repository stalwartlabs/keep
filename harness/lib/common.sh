# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
# shellcheck shell=bash

set -euo pipefail

HARNESS="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HARNESS_WORK="${HARNESS_WORK:-$HARNESS/work}"
HARNESS_POLL="${HARNESS_POLL:-15}"
REMOTE=/opt/keep-harness
JOB="$REMOTE/remote/job.sh"
BIN=/opt/keep/bin

die() { echo "${0##*/}: $*" >&2; exit 1; }
say() { echo "[$(date -u +%H:%M:%S)] ${0##*/}: $*" >&2; }

load_inventory() {
  mkdir -p "$HOME/.ssh"
  HOSTS="${1:?inventory (hosts.json) required}"
  [ -f "$HOSTS" ] || die "inventory $HOSTS not found"
  HOSTS="$(cd "$(dirname "$HOSTS")" && pwd)/$(basename "$HOSTS")"
  jq -e '.run and .track and (.hosts | length > 0)' "$HOSTS" > /dev/null || die "$HOSTS is not an inventory"
  RUN=$(jq -r .run "$HOSTS"); TRACK=$(jq -r .track "$HOSTS")
  SSH_USER=$(jq -r '.ssh.user // "keep"' "$HOSTS")
  SSH_KEY=$(jq -r '.ssh.key // empty' "$HOSTS")
  KNOWN_HOSTS=$(jq -r '.ssh.known_hosts // empty' "$HOSTS")
  KNOWN_HOSTS="${KNOWN_HOSTS:-$HARNESS_WORK/$RUN/known_hosts}"
  STATE="$(dirname "$HOSTS")"
  OUT="${HARNESS_OUT:-$STATE/results}"
  mkdir -p "$STATE" "$OUT"
}

inv() { jq -r "$@" "$HOSTS"; }

host_names() {
  if [ -n "${1:-}" ]; then inv --arg r "$1" '.hosts[] | select(.role == $r) | .name'; else inv '.hosts[].name'; fi
}

host_field() { inv --arg n "$1" --arg f "$2" '.hosts[] | select(.name == $n) | .[$f] // empty'; }

host_ip() { host_field "$1" private_ip; }

target_host() {
  local t
  t=$(inv --arg t "$1" '.targets[$t] // empty')
  [ -n "$t" ] || die "target $1 is not in the inventory's targets"
  echo "$t"
}

ssh_opts() {
  SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=15 -o ServerAliveInterval=15 -o ServerAliveCountMax=4
    -o StrictHostKeyChecking=accept-new -o "UserKnownHostsFile=$KNOWN_HOSTS" -o LogLevel=ERROR
    -o ControlMaster=auto -o "ControlPath=$HOME/.ssh/cm-keep-harness-%C" -o ControlPersist=120)
  if [ -n "$SSH_KEY" ]; then SSH_OPTS+=(-i "$SSH_KEY"); fi
}

hssh() {
  local name="$1" addr attempt rc
  shift
  addr="$SSH_USER@$(host_field "$name" public_ip)"
  ssh_opts
  for attempt in 1 2 3; do
    rc=0
    # shellcheck disable=SC2029
    ssh "${SSH_OPTS[@]}" "$addr" "$@" || rc=$?
    [ "$rc" = 255 ] || return "$rc"
    say "$name: ssh failed (attempt $attempt)"
    sleep $((attempt * 5))
  done
  return 255
}

rsync_to() {
  local name="$1" src="$2" dest="$3"
  shift 3
  ssh_opts
  rsync -az "$@" -e "ssh $(printf '%q ' "${SSH_OPTS[@]}")" "$src" "$SSH_USER@$(host_field "$name" public_ip):$dest"
}

rsync_from() {
  local name="$1" src="$2" dest="$3"
  shift 3
  ssh_opts
  rsync -az "$@" -e "ssh $(printf '%q ' "${SSH_OPTS[@]}")" "$SSH_USER@$(host_field "$name" public_ip):$src" "$dest"
}

quote() { if [ $# -gt 0 ]; then printf '%q ' "$@"; fi; }

job_run() {
  local host="$1" name="$2" opts=() timeout="${HARNESS_JOB_TIMEOUT:-86400}" start status delay=1
  shift 2
  while [ $# -gt 0 ]; do
    case "$1" in
      --sudo|--again) opts+=("$1"); shift ;;
      --timeout) timeout="$2"; shift 2 ;;
      --) shift; break ;;
      *) die "job_run: unknown option $1" ;;
    esac
  done
  start=$(date +%s)
  status=$(hssh "$host" "$JOB start $(quote "$name") $(quote ${opts[@]+"${opts[@]}"}) -- $(quote "$@")")
  while [ "$status" = running ]; do
    if [ $(($(date +%s) - start)) -gt "$timeout" ]; then
      hssh "$host" "$JOB stop $(quote "$name")" || true
      say "$host: job $name timed out after ${timeout}s"
      return 124
    fi
    sleep "$delay"
    delay=$((delay * 2)); [ "$delay" -le "$HARNESS_POLL" ] || delay="$HARNESS_POLL"
    status=$(hssh "$host" "$JOB status $(quote "$name")" || echo running)
  done
  case "$status" in
    "done 0") return 0 ;;
    "done "*)
      say "$host: job $name failed ($status); last lines:"
      hssh "$host" "$JOB log $(quote "$name") 30" >&2 || true
      return "${status#done }" ;;
    *) say "$host: job $name: unexpected status '$status'"; return 1 ;;
  esac
}

on_hosts() {
  local fn="$1" host pids=() hosts=() failed="" i=0 pid
  shift
  for host in "$@"; do "$fn" "$host" & pids+=($!); hosts+=("$host"); done
  for pid in ${pids[@]+"${pids[@]}"}; do
    wait "$pid" || failed="$failed ${hosts[$i]}"
    i=$((i + 1))
  done
  [ -z "$failed" ] || { say "$fn failed on:$failed"; return 1; }
}

phase_done() { [ -f "$STATE/phases/$1.done" ]; }

mark_done() { mkdir -p "$STATE/phases"; date -u +%FT%TZ > "$STATE/phases/$1.done"; }
