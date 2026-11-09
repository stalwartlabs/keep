#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
	cat <<'USAGE'
usage: toolchain.sh

Build host: compilers, cmake (rdkafka), pkg-config and OpenSSL headers, and Rust (RUST_TOOLCHAIN of
versions.env, installed by rustup-init RUSTUP_VERSION after its sha256 check) under /opt/rust and /opt/cargo, owned by the admin user. Idempotent.
USAGE
}

[[ ${1:-} != -h && ${1:-} != --help ]] || { usage; exit 0; }
require_root
apt_install build-essential clang lld llvm libclang-dev cmake ninja-build pkg-config autoconf automake libtool protobuf-compiler \
	libssl-dev zlib1g-dev libzstd-dev liblz4-dev libbz2-dev libsnappy-dev libcurl4-openssl-dev libsasl2-dev git rsync curl ca-certificates
export RUSTUP_HOME=/opt/rust CARGO_HOME=/opt/cargo
require_var RUST_TOOLCHAIN RUSTUP_VERSION RUSTUP_SHA256_X86_64 RUSTUP_SHA256_AARCH64
if [[ ! -x $CARGO_HOME/bin/rustup ]]; then
	case $(uname -m) in
	x86_64) sum=$RUSTUP_SHA256_X86_64 ;;
	aarch64) sum=$RUSTUP_SHA256_AARCH64 ;;
	*) die "no rustup-init for $(uname -m)" ;;
	esac
	curl --retry 5 --retry-all-errors --proto '=https' --tlsv1.2 -sSfo /tmp/rustup-init \
		"https://static.rust-lang.org/rustup/archive/$RUSTUP_VERSION/$(uname -m)-unknown-linux-gnu/rustup-init"
	echo "$sum  /tmp/rustup-init" | sha256sum -c --quiet || die "rustup-init checksum mismatch"
	chmod +x /tmp/rustup-init
	/tmp/rustup-init -y --no-modify-path --profile minimal --default-toolchain "$RUST_TOOLCHAIN"
	rm -f /tmp/rustup-init
fi
"$CARGO_HOME/bin/rustup" toolchain install --profile minimal "$RUST_TOOLCHAIN"
"$CARGO_HOME/bin/rustup" default "$RUST_TOOLCHAIN"
printf 'export RUSTUP_HOME=/opt/rust CARGO_HOME=/opt/cargo PATH="/opt/cargo/bin:$PATH"\n' >/etc/profile.d/keep-rust.sh
chown -R "$(admin_user):" /opt/rust /opt/cargo
log "$("$CARGO_HOME/bin/rustc" --version)"
