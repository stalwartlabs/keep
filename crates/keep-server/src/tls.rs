/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::fmt;
use std::io;
use std::num::NonZeroU64;
use std::sync::Arc;

use keep_proto::keys::{KeyFileError, der_items, private_key, public_keys};
use keep_rt::tls::TlsConfig;
use rustls::client::Resumption;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{AlwaysResolvesClientRawPublicKeys, WebPkiServerVerifier};
use rustls::crypto::{
    CryptoProvider, WebPkiSupportedAlgorithms, aws_lc_rs, verify_tls13_signature_with_raw_key,
};
use rustls::pki_types::{
    AlgorithmIdentifier, CertificateDer, PrivateKeyDer, ServerName, SubjectPublicKeyInfoDer,
    UnixTime,
};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{
    AlwaysResolvesServerRawPublicKeys, NoServerSessionStorage, WebPkiClientVerifier,
};
use rustls::sign::CertifiedKey;
use rustls::{
    CertificateError, CipherSuiteCommon, ClientConfig, DigitallySignedStruct, DistinguishedName,
    PeerIncompatible, RootCertStore, ServerConfig, SignatureScheme, SupportedCipherSuite,
    Tls13CipherSuite, version,
};

#[derive(Clone, Debug)]
pub enum TlsFiles {
    RawPublicKeys {
        key: Box<str>,
        clients: Box<str>,
        servers: Option<Box<str>>,
    },
    Authority {
        cert: Box<str>,
        key: Box<str>,
        ca: Box<str>,
        peer_ca: Option<Box<str>>,
    },
}

#[derive(Clone, Debug)]
pub struct ServerTls {
    pub clients: TlsConfig,
    pub peers: Option<TlsConfig>,
}

impl ServerTls {
    pub fn load(
        files: &TlsFiles,
        cluster: bool,
        key_update_records: Option<NonZeroU64>,
    ) -> io::Result<ServerTls> {
        let provider = Arc::new(provider(key_update_records)?);
        match files {
            TlsFiles::RawPublicKeys {
                key,
                clients,
                servers,
            } => raw_public_keys(&provider, key, clients, servers.as_deref(), cluster),
            TlsFiles::Authority {
                cert,
                key,
                ca,
                peer_ca,
            } => authority(&provider, cert, key, ca, peer_ca.as_deref(), cluster),
        }
    }
}

fn startup_error(reason: impl fmt::Display) -> io::Error {
    io::Error::other(format!("TLS: {reason}"))
}

fn key_error(err: KeyFileError) -> io::Error {
    startup_error(err)
}

fn provider(key_update_records: Option<NonZeroU64>) -> io::Result<CryptoProvider> {
    let mut provider = aws_lc_rs::default_provider();
    provider.cipher_suites = match key_update_records {
        None => vec![
            aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256,
            aws_lc_rs::cipher_suite::TLS13_AES_256_GCM_SHA384,
            aws_lc_rs::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
        ],
        Some(records) => vec![limited(records)?],
    };
    Ok(provider)
}

fn limited(records: NonZeroU64) -> io::Result<SupportedCipherSuite> {
    #[allow(unreachable_patterns)]
    let base = match aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256 {
        SupportedCipherSuite::Tls13(base) => base,
        _ => return Err(startup_error("no TLS 1.3 AES-128-GCM suite")),
    };
    let suite = Tls13CipherSuite {
        common: CipherSuiteCommon {
            suite: base.common.suite,
            hash_provider: base.common.hash_provider,
            confidentiality_limit: records.get(),
        },
        hkdf_provider: base.hkdf_provider,
        aead_alg: base.aead_alg,
        quic: base.quic,
    };
    Ok(SupportedCipherSuite::Tls13(Box::leak(Box::new(suite))))
}

fn key_algorithms(provider: &CryptoProvider) -> Vec<AlgorithmIdentifier> {
    provider
        .signature_verification_algorithms
        .all
        .iter()
        .map(|algorithm| algorithm.public_key_alg_id())
        .collect()
}

fn server_builder(
    provider: &Arc<CryptoProvider>,
) -> io::Result<rustls::ConfigBuilder<ServerConfig, rustls::WantsVerifier>> {
    ServerConfig::builder_with_provider(Arc::clone(provider))
        .with_protocol_versions(&[&version::TLS13])
        .map_err(startup_error)
}

fn client_builder(
    provider: &Arc<CryptoProvider>,
) -> io::Result<rustls::ConfigBuilder<ClientConfig, rustls::WantsVerifier>> {
    ClientConfig::builder_with_provider(Arc::clone(provider))
        .with_protocol_versions(&[&version::TLS13])
        .map_err(startup_error)
}

fn finish_server(mut config: ServerConfig) -> Arc<ServerConfig> {
    config.send_tls13_tickets = 0;
    config.session_storage = Arc::new(NoServerSessionStorage {});
    config.max_early_data_size = 0;
    Arc::new(config)
}

fn finish_client(mut config: ClientConfig) -> Arc<ClientConfig> {
    config.resumption = Resumption::disabled();
    Arc::new(config)
}

fn raw_public_keys(
    provider: &Arc<CryptoProvider>,
    key: &str,
    clients: &str,
    servers: Option<&str>,
    cluster: bool,
) -> io::Result<ServerTls> {
    let supported = key_algorithms(provider);
    let signing = provider
        .key_provider
        .load_private_key(private_key("--tls-key", key).map_err(key_error)?)
        .map_err(|err| key_error(KeyFileError::new("--tls-key", key, err)))?;
    let public = signing
        .public_key()
        .ok_or_else(|| {
            key_error(KeyFileError::new(
                "--tls-key",
                key,
                "no public key for this key type",
            ))
        })?
        .as_ref()
        .to_vec();
    let certified = Arc::new(CertifiedKey::new(
        vec![CertificateDer::from(public.clone())],
        signing,
    ));
    let algorithms = provider.signature_verification_algorithms;
    let client_keys = public_keys("--tls-clients", clients, &supported).map_err(key_error)?;
    let client_port = server_builder(provider)?
        .with_client_cert_verifier(Arc::new(KnownKeys::new(client_keys, algorithms)))
        .with_cert_resolver(Arc::new(AlwaysResolvesServerRawPublicKeys::new(
            Arc::clone(&certified),
        )));
    let clients = TlsConfig::new(finish_server(client_port), None);
    if !cluster {
        return Ok(ServerTls {
            clients,
            peers: None,
        });
    }
    let servers = servers.ok_or_else(|| {
        startup_error("--tls rpk in a cluster needs --tls-servers (--tls off serves plaintext)")
    })?;
    let server_keys = public_keys("--tls-servers", servers, &supported).map_err(key_error)?;
    if !server_keys
        .iter()
        .any(|known| known.as_ref() == public.as_slice())
    {
        return Err(key_error(KeyFileError::new(
            "--tls-servers",
            servers,
            "does not list this server's own key, so every peer would refuse it",
        )));
    }
    let known = Arc::new(KnownKeys::new(server_keys, algorithms));
    let peer_port = server_builder(provider)?
        .with_client_cert_verifier(known.clone())
        .with_cert_resolver(Arc::new(AlwaysResolvesServerRawPublicKeys::new(
            Arc::clone(&certified),
        )));
    let dial = client_builder(provider)?
        .dangerous()
        .with_custom_certificate_verifier(known)
        .with_client_cert_resolver(Arc::new(AlwaysResolvesClientRawPublicKeys::new(certified)));
    Ok(ServerTls {
        clients,
        peers: Some(TlsConfig::new(
            finish_server(peer_port),
            Some(finish_client(dial)),
        )),
    })
}

fn authority(
    provider: &Arc<CryptoProvider>,
    cert: &str,
    key: &str,
    ca: &str,
    peer_ca: Option<&str>,
    cluster: bool,
) -> io::Result<ServerTls> {
    let peer_ca = match (cluster, peer_ca) {
        (true, None) => return Err(startup_error("--tls ca in a cluster needs --tls-peer-ca")),
        (true, peer_ca) => peer_ca,
        (false, _) => None,
    };
    let chain = der_items::<CertificateDer<'static>>("--tls-cert", cert).map_err(key_error)?;
    let private = private_key("--tls-key", key).map_err(key_error)?;
    let client_port = certified_port(provider, roots("--tls-ca", ca)?, cert, &chain, &private)?;
    let clients = TlsConfig::new(client_port, None);
    let Some(peer_ca) = peer_ca else {
        return Ok(ServerTls {
            clients,
            peers: None,
        });
    };
    let peer_roots = roots("--tls-peer-ca", peer_ca)?;
    let peer_port = certified_port(provider, Arc::clone(&peer_roots), cert, &chain, &private)?;
    let verifier = WebPkiServerVerifier::builder_with_provider(peer_roots, Arc::clone(provider))
        .build()
        .map_err(startup_error)?;
    let dial = client_builder(provider)?
        .with_webpki_verifier(verifier)
        .with_client_auth_cert(chain, private)
        .map_err(|err| key_error(KeyFileError::new("--tls-cert and --tls-key", cert, err)))?;
    Ok(ServerTls {
        clients,
        peers: Some(TlsConfig::new(peer_port, Some(finish_client(dial)))),
    })
}

fn roots(flag: &str, path: &str) -> io::Result<Arc<RootCertStore>> {
    let mut roots = RootCertStore::empty();
    for root in der_items::<CertificateDer<'static>>(flag, path).map_err(key_error)? {
        roots
            .add(root)
            .map_err(|err| key_error(KeyFileError::new(flag, path, err)))?;
    }
    Ok(Arc::new(roots))
}

fn certified_port(
    provider: &Arc<CryptoProvider>,
    roots: Arc<RootCertStore>,
    cert: &str,
    chain: &[CertificateDer<'static>],
    private: &PrivateKeyDer<'static>,
) -> io::Result<Arc<ServerConfig>> {
    let verifier = WebPkiClientVerifier::builder_with_provider(roots, Arc::clone(provider))
        .build()
        .map_err(startup_error)?;
    let port = server_builder(provider)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain.to_vec(), private.clone_key())
        .map_err(|err| key_error(KeyFileError::new("--tls-cert and --tls-key", cert, err)))?;
    Ok(finish_server(port))
}

#[derive(Debug)]
struct KnownKeys {
    keys: Box<[SubjectPublicKeyInfoDer<'static>]>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl KnownKeys {
    fn new(
        keys: Vec<SubjectPublicKeyInfoDer<'static>>,
        algorithms: WebPkiSupportedAlgorithms,
    ) -> KnownKeys {
        KnownKeys {
            keys: keys.into_boxed_slice(),
            algorithms,
        }
    }

    fn check(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
    ) -> Result<(), rustls::Error> {
        let presented = end_entity.as_ref();
        if intermediates.is_empty() && self.keys.iter().any(|key| key.as_ref() == presented) {
            Ok(())
        } else {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_signature(
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
}

impl ClientCertVerifier for KnownKeys {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.check(end_entity, intermediates)
            .map(|()| ClientCertVerified::assertion())
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
        self.verify_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

impl ServerCertVerifier for KnownKeys {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.check(end_entity, intermediates)
            .map(|()| ServerCertVerified::assertion())
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
        self.verify_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}
