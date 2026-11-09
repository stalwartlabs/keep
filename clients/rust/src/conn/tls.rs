/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::sync::Arc;
use std::{fmt, io};

use keep_proto::keys::{KeyFileError, der_items, private_key, public_keys};
use rustls::client::AlwaysResolvesClientRawPublicKeys;
use rustls::client::Resumption;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{
    CryptoProvider, WebPkiSupportedAlgorithms, aws_lc_rs, verify_tls13_signature_with_raw_key,
};
use rustls::pki_types::{
    AlgorithmIdentifier, CertificateDer, ServerName, SubjectPublicKeyInfoDer, UnixTime,
};
use rustls::sign::CertifiedKey;
use rustls::{
    CertificateError, ClientConnection, ConfigBuilder, DigitallySignedStruct, PeerIncompatible,
    RootCertStore, SignatureScheme, WantsVerifier, version,
};
use tokio::net::TcpStream;

use super::session::Session;
use super::socket::Raw;
use crate::config::{Identity, Tls};
use crate::error::Error;

#[derive(Clone)]
pub(crate) struct TlsSetup {
    config: Arc<rustls::ClientConfig>,
    server_name: Option<Box<str>>,
}

#[derive(Clone)]
pub(crate) struct Connector {
    config: Arc<rustls::ClientConfig>,
    name: ServerName<'static>,
}

#[derive(Debug)]
struct ServerKeys {
    keys: Box<[SubjectPublicKeyInfoDer<'static>]>,
    algorithms: WebPkiSupportedAlgorithms,
}

fn tls_error(err: impl ToString) -> Error {
    Error::Tls(err.to_string().into_boxed_str())
}

fn file_error(what: &str, path: &str, reason: impl fmt::Display) -> Error {
    KeyFileError::new(what, path, reason).into()
}

fn connect_error(err: io::Error) -> Error {
    Error::Connect(err.kind())
}

fn provider() -> CryptoProvider {
    let mut provider = aws_lc_rs::default_provider();
    provider.cipher_suites = vec![
        aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256,
        aws_lc_rs::cipher_suite::TLS13_AES_256_GCM_SHA384,
        aws_lc_rs::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
    ];
    provider
}

fn key_algorithms(provider: &CryptoProvider) -> Vec<AlgorithmIdentifier> {
    provider
        .signature_verification_algorithms
        .all
        .iter()
        .map(|algorithm| algorithm.public_key_alg_id())
        .collect()
}

fn raw_public_keys(
    builder: ConfigBuilder<rustls::ClientConfig, WantsVerifier>,
    provider: &CryptoProvider,
    key: &str,
    servers: &str,
) -> Result<rustls::ClientConfig, Error> {
    let keys = public_keys("servers", servers, &key_algorithms(provider))?;
    let signing = provider
        .key_provider
        .load_private_key(private_key("key", key)?)
        .map_err(|err| file_error("key", key, err))?;
    let public = signing
        .public_key()
        .ok_or_else(|| file_error("key", key, "no public key for this key type"))?
        .as_ref()
        .to_vec();
    let certified = CertifiedKey::new(vec![CertificateDer::from(public)], signing);
    Ok(builder
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(ServerKeys {
            keys: keys.into_boxed_slice(),
            algorithms: provider.signature_verification_algorithms,
        }))
        .with_client_cert_resolver(Arc::new(AlwaysResolvesClientRawPublicKeys::new(Arc::new(
            certified,
        )))))
}

fn authority(
    builder: ConfigBuilder<rustls::ClientConfig, WantsVerifier>,
    ca: &str,
    identity: Option<&Identity>,
) -> Result<rustls::ClientConfig, Error> {
    let mut roots = RootCertStore::empty();
    for cert in der_items::<CertificateDer<'static>>("ca", ca)? {
        roots.add(cert).map_err(|err| file_error("ca", ca, err))?;
    }
    let builder = builder.with_root_certificates(roots);
    match identity {
        None => Ok(builder.with_no_client_auth()),
        Some(Identity { chain, key }) => builder
            .with_client_auth_cert(der_items("cert", chain)?, private_key("key", key)?)
            .map_err(|err| file_error("cert and key", chain, err)),
    }
}

impl TlsSetup {
    pub fn new(tls: &Tls) -> Result<TlsSetup, Error> {
        let provider = Arc::new(provider());
        let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(&[&version::TLS13])
            .map_err(tls_error)?;
        let (mut config, server_name) = match tls {
            Tls::RawPublicKeys { key, servers } => {
                (raw_public_keys(builder, &provider, key, servers)?, None)
            }
            Tls::Authority {
                ca,
                server_name,
                identity,
            } => (
                authority(builder, ca, identity.as_ref())?,
                server_name.clone(),
            ),
        };
        config.resumption = Resumption::disabled();
        Ok(TlsSetup {
            config: Arc::new(config),
            server_name,
        })
    }

    pub fn connector(&self, host: &str) -> Result<Connector, Error> {
        let host = self.server_name.as_deref().unwrap_or(host);
        let name = ServerName::try_from(host.to_owned())
            .map_err(|_| Error::Config(format!("invalid TLS server name {host}").into()))?;
        Ok(Connector {
            config: Arc::clone(&self.config),
            name,
        })
    }
}

impl ServerCertVerifier for ServerKeys {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let presented = end_entity.as_ref();
        if intermediates.is_empty() && self.keys.iter().any(|key| key.as_ref() == presented) {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::PeerIncompatible(
            PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature_with_raw_key(
            message,
            &SubjectPublicKeyInfoDer::from(cert.as_ref()),
            dss,
            &self.algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

impl Connector {
    #[cfg(keep_bench)]
    pub(crate) fn into_parts(self) -> (Arc<rustls::ClientConfig>, ServerName<'static>) {
        (self.config, self.name)
    }

    pub async fn handshake(&self, stream: &TcpStream) -> Result<Session, Error> {
        let mut tls = ClientConnection::new(Arc::clone(&self.config), self.name.clone())
            .map_err(tls_error)?;
        let mut raw = Raw(stream);
        loop {
            while tls.wants_write() {
                match tls.write_tls(&mut raw) {
                    Ok(0) => return Err(Error::Connect(io::ErrorKind::WriteZero)),
                    Ok(_) => {}
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        stream.writable().await.map_err(connect_error)?;
                    }
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                    Err(err) => return Err(connect_error(err)),
                }
            }
            if !tls.is_handshaking() {
                return Ok(Session::new(tls));
            }
            match tls.read_tls(&mut raw) {
                Ok(0) => return Err(Error::Connect(io::ErrorKind::UnexpectedEof)),
                Ok(_) => {
                    if let Err(err) = tls.process_new_packets() {
                        let _ = tls.write_tls(&mut raw);
                        return Err(tls_error(err));
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    stream.readable().await.map_err(connect_error)?;
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) => return Err(connect_error(err)),
            }
        }
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-client/conn/tls.rs"]
mod tests;
