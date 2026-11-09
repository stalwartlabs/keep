/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::convert::Infallible;
use std::net::SocketAddr;

#[derive(Clone, Debug)]
pub struct TlsConfig(Infallible);

pub(crate) enum Tls {}

impl TlsConfig {
    pub(crate) fn read_buffer(&self, _configured: usize) -> usize {
        match self.0 {}
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Opened {
    Open,
    Ready,
    Closed,
}

#[derive(Debug)]
pub(crate) struct Failed;

impl Tls {
    pub(crate) fn server(config: &TlsConfig) -> Option<Box<Tls>> {
        match config.0 {}
    }

    pub(crate) fn client(config: &TlsConfig, _addr: SocketAddr) -> Option<Box<Tls>> {
        match config.0 {}
    }

    pub(crate) fn backlog(&self) -> usize {
        match *self {}
    }

    pub(crate) fn unsent(&mut self) -> (&[u8], &mut usize) {
        match *self {}
    }

    pub(crate) fn settle(&mut self) -> usize {
        match *self {}
    }

    pub(crate) fn is_ready(&self) -> bool {
        match *self {}
    }

    pub(crate) fn carry_into(&self, _buf: &mut [u8]) -> usize {
        match *self {}
    }

    pub(crate) fn open(
        &mut self,
        _data: &mut [u8],
        _plain: &mut Vec<u8>,
    ) -> Result<Opened, Failed> {
        match *self {}
    }

    pub(crate) fn seal(&mut self, _out: &mut Vec<u8>) -> Result<(), Failed> {
        match *self {}
    }

    pub(crate) fn close_notify(&mut self) -> Result<(), Failed> {
        match *self {}
    }
}
