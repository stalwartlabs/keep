/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::sync::Arc;

use rustls::pki_types::ServerName;

use crate::config::Tls;
use crate::conn::{Connector, TlsSetup};
use crate::error::Error;

pub fn tls_client_config(
    tls: &Tls,
    host: &str,
) -> Result<(Arc<rustls::ClientConfig>, ServerName<'static>), Error> {
    TlsSetup::new(tls)?
        .connector(host)
        .map(Connector::into_parts)
}
