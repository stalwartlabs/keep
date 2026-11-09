/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

fn main() {
    println!("cargo::rerun-if-changed=c/echo.c");
    println!("cargo::rerun-if-env-changed=NETPROBE_SKIP_C");
    println!("cargo::rustc-check-cfg=cfg(netprobe_c_echo)");
    if std::env::var_os("NETPROBE_SKIP_C").is_none() {
        cc::Build::new()
            .file("c/echo.c")
            .opt_level(2)
            .warnings(true)
            .compile("netprobe_echo");
        println!("cargo::rustc-cfg=netprobe_c_echo");
    }
}
