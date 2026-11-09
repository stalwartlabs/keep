# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM docker.io/library/debian:bullseye-slim@sha256:e5b6442dd2e9684cf5e87d8338b5968f3b348636fc0be6d7850a381e3731a2bd AS builder
ARG TARGETARCH
ARG RUST_VERSION=1.99.1
ARG GLIBC_MAX=34
ENV DEBIAN_FRONTEND=noninteractive \
    RUSTUP_HOME=/opt/rust \
    CARGO_HOME=/opt/cargo \
    PATH=/opt/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    CARGO_TERM_COLOR=always \
    CARGO_NET_RETRY=10 \
    CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc \
    CXX_x86_64_unknown_linux_gnu=x86_64-linux-gnu-g++ \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++ \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN printf 'deb http://archive.debian.org/debian bullseye main\ndeb http://archive.debian.org/debian bullseye-updates main\ndeb http://archive.debian.org/debian-security bullseye-security main\n' > /etc/apt/sources.list && \
    rm -f /etc/apt/sources.list.d/*.list /etc/apt/sources.list.d/*.sources && \
    case "$(dpkg --print-architecture)" in \
        amd64) cross="gcc-aarch64-linux-gnu g++-aarch64-linux-gnu libc6-dev-arm64-cross" ;; \
        arm64) cross="gcc-x86-64-linux-gnu g++-x86-64-linux-gnu libc6-dev-amd64-cross" ;; \
        *) exit 1 ;; \
    esac && \
    apt-get -o Acquire::Check-Valid-Until=false -o Acquire::Retries=5 update && \
    apt-get -o Acquire::Retries=5 install -yq --no-install-recommends build-essential ca-certificates curl git ${cross} && \
    rm -rf /var/lib/apt/lists/*
RUN arch="$(uname -m)" && \
    case "${arch}" in \
        x86_64) sum=b1c8cb14e30889e7e2999e6b69e62d9e9b9ffc0f4df8180564d306d9680e6c55 ;; \
        aarch64) sum=9f608c142e0e38a78957d0661ba1dfaf0b6fb37184cfaa4b9a0aa57af6184282 ;; \
        *) exit 1 ;; \
    esac && \
    curl --retry 5 --retry-all-errors --proto '=https' --tlsv1.2 -sSfo /tmp/rustup-init "https://static.rust-lang.org/rustup/archive/1.29.3/${arch}-unknown-linux-gnu/rustup-init" && \
    echo "${sum}  /tmp/rustup-init" | sha256sum -c --quiet && \
    chmod +x /tmp/rustup-init && \
    /tmp/rustup-init -y --no-modify-path --profile minimal --default-toolchain "${RUST_VERSION}" && \
    rm /tmp/rustup-init && \
    rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu
WORKDIR /build
COPY . .
RUN --mount=type=cache,target=/opt/cargo/registry \
    --mount=type=cache,target=/opt/cargo/git \
    --mount=type=cache,target=/build/target,id=keep-target-${TARGETARCH} \
    case "${TARGETARCH}" in \
        amd64) target=x86_64-unknown-linux-gnu ;; \
        arm64) target=aarch64-unknown-linux-gnu ;; \
        *) exit 1 ;; \
    esac && \
    cargo build --locked --release --target "${target}" -p keep-server --bin keep && \
    newest="$(grep -a -o 'GLIBC_2\.[0-9][0-9]*' "target/${target}/release/keep" | sed 's/^GLIBC_2\.//' | sort -n | tail -n 1)" && \
    { [ -n "${newest}" ] && [ "${newest}" -le "${GLIBC_MAX}" ] || { echo "keep needs GLIBC_2.${newest}, above the 2.${GLIBC_MAX} floor"; exit 1; }; } && \
    mkdir -p /artifact && \
    cp "target/${target}/release/keep" /artifact/keep

FROM scratch AS binaries
COPY --from=builder /artifact /

FROM docker.io/library/debian:trixie-slim AS image
RUN groupadd -r -g 2000 keep && \
    useradd -r -u 2000 -g 2000 -s /usr/sbin/nologin -M keep && \
    mkdir -p /etc/keep /run/keep && \
    chown keep:keep /run/keep
COPY --from=builder --chmod=0755 /artifact/keep /usr/local/bin/keep
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1 \
    KEEP_CONFIG=/etc/keep/keep.toml
USER keep
WORKDIR /run/keep
EXPOSE 7000/tcp 7100/tcp 7100/udp
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/keep"]
