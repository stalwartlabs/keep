/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::error::{Context, Error, Result};
use serde::Serialize;
use std::io::{self, Write};

pub fn emit(report: &impl Serialize) -> Result<()> {
    let mut out = io::stdout().lock();
    serde_json::to_writer(&mut out, report)
        .map_err(|_| Error::Protocol("report serialization failed"))?;
    out.write_all(b"\n").context("write report")?;
    out.flush().context("flush report")
}
