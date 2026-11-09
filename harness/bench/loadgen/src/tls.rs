/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use keep_client::Tls;
use keep_client::bench::tls_client_config;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, StreamOwned};
use serde::Serialize;

use crate::setup::SetupError;

pub struct TlsClients {
    tls: Tls,
    hosts: Vec<(Box<str>, TlsClient)>,
}

#[derive(Clone)]
struct TlsClient {
    config: Arc<ClientConfig>,
    name: ServerName<'static>,
}

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
pub struct TlsInfo {
    pub version: String,
    pub suite: String,
}

pub enum Stream {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl TlsClients {
    pub fn new(tls: Tls) -> Self {
        TlsClients {
            tls,
            hosts: Vec::new(),
        }
    }

    pub fn connect(&mut self, addr: &str, stream: TcpStream) -> Result<Stream, SetupError> {
        let client = self.client(host_of(addr))?;
        let mut conn = ClientConnection::new(client.config, client.name)
            .map_err(|e| SetupError::Tls(format!("{addr}: {e}")))?;
        let mut sock = stream;
        while conn.is_handshaking() {
            match conn.complete_io(&mut sock) {
                Ok((0, 0)) => {
                    return Err(SetupError::Tls(format!(
                        "{addr}: the server closed the connection during the TLS handshake"
                    )));
                }
                Ok(_) => {}
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
                    ) =>
                {
                    return Err(SetupError::Tls(format!(
                        "{addr}: the server closed the connection during the TLS handshake"
                    )));
                }
                Err(e) => return Err(SetupError::Tls(format!("{addr}: handshake: {e}"))),
            }
        }
        Ok(Stream::Tls(Box::new(StreamOwned::new(conn, sock))))
    }

    fn client(&mut self, host: &str) -> Result<TlsClient, SetupError> {
        if let Some((_, client)) = self.hosts.iter().find(|(known, _)| &**known == host) {
            return Ok(client.clone());
        }
        let (config, name) = tls_client_config(&self.tls, host)
            .map_err(|e| SetupError::Tls(format!("client config for {host}: {e}")))?;
        let client = TlsClient { config, name };
        self.hosts.push((host.into(), client.clone()));
        Ok(client)
    }
}

fn host_of(addr: &str) -> &str {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    host.strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host)
}

impl Stream {
    pub fn tcp(&self) -> &TcpStream {
        match self {
            Stream::Plain(stream) => stream,
            Stream::Tls(stream) => &stream.sock,
        }
    }

    pub fn tls_info(&self) -> Option<TlsInfo> {
        let Stream::Tls(stream) = self else {
            return None;
        };
        let conn = &stream.conn;
        Some(TlsInfo {
            version: conn
                .protocol_version()
                .map(|v| format!("{v:?}"))
                .unwrap_or_default(),
            suite: conn
                .negotiated_cipher_suite()
                .map(|s| format!("{:?}", s.suite()))
                .unwrap_or_default(),
        })
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(stream) => stream.read(buf),
            Stream::Tls(stream) => stream.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(stream) => stream.write(buf),
            Stream::Tls(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Plain(stream) => stream.flush(),
            Stream::Tls(stream) => stream.flush(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::process;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    use keep_client::Tls;
    use rcgen::{CertificateParams, KeyPair, PKCS_ED25519};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::{ServerConfig, ServerConnection, StreamOwned};

    use super::{Stream, TlsClients, TlsInfo, host_of};
    use crate::setup::SetupError;

    static FILES: AtomicU64 = AtomicU64::new(0);

    fn temp_file(what: &str, contents: &str) -> PathBuf {
        let path = env::temp_dir().join(format!(
            "loadgen-tls-{}-{}-{what}",
            process::id(),
            FILES.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&path, contents).expect("write");
        path
    }

    fn ca_server(name: &str) -> (Arc<ServerConfig>, PathBuf) {
        let key = KeyPair::generate_for(&PKCS_ED25519).expect("key");
        let cert = CertificateParams::new(vec![name.to_owned()])
            .expect("params")
            .self_signed(&key)
            .expect("cert");
        let ca = temp_file(name, &cert.pem());
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from(cert.der().to_vec())],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
            )
            .expect("server config");
        (Arc::new(config), ca)
    }

    fn echo_once(listener: TcpListener, config: Arc<ServerConfig>) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let (sock, _) = listener.accept().expect("accept");
            let conn = ServerConnection::new(config).expect("server connection");
            let mut stream = StreamOwned::new(conn, sock);
            let mut buf = [0u8; 5];
            if stream.read_exact(&mut buf).is_ok() {
                let _ = stream.write_all(&buf);
                let _ = stream.flush();
            }
        })
    }

    #[test]
    fn ca_mode_handshakes_and_carries_bytes() {
        let (config, ca) = ca_server("keep.test");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        let server = echo_once(listener, config);
        let mut clients = TlsClients::new(Tls::Authority {
            ca: ca.to_str().expect("utf-8").into(),
            server_name: Some("keep.test".into()),
            identity: None,
        });
        let mut stream = clients
            .connect(&addr, TcpStream::connect(&addr).expect("connect"))
            .expect("handshake");
        assert_eq!(
            stream.tls_info(),
            Some(TlsInfo {
                version: String::from("TLSv1_3"),
                suite: String::from("TLS13_AES_128_GCM_SHA256"),
            })
        );
        stream.write_all(b"hello").expect("write");
        stream.flush().expect("flush");
        let mut reply = [0u8; 5];
        stream.read_exact(&mut reply).expect("read");
        assert_eq!(&reply, b"hello");
        assert!(matches!(stream, Stream::Tls(_)));
        server.join().expect("server");
    }

    #[test]
    fn wrong_name_fails_the_handshake() {
        let (config, ca) = ca_server("keep.test");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        let server = echo_once(listener, config);
        let mut clients = TlsClients::new(Tls::Authority {
            ca: ca.to_str().expect("utf-8").into(),
            server_name: Some("other.test".into()),
            identity: None,
        });
        let result = clients.connect(&addr, TcpStream::connect(&addr).expect("connect"));
        assert!(matches!(result, Err(SetupError::Tls(_))));
        drop(result);
        server.join().expect("server");
    }

    #[test]
    fn server_closing_mid_handshake_is_a_tls_error() {
        let (_, ca) = ca_server("keep.test");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        let server = thread::spawn(move || {
            let (mut sock, _) = listener.accept().expect("accept");
            let mut hello = [0u8; 5];
            sock.read_exact(&mut hello).expect("client hello");
        });
        let mut clients = TlsClients::new(Tls::Authority {
            ca: ca.to_str().expect("utf-8").into(),
            server_name: None,
            identity: None,
        });
        let result = clients.connect(&addr, TcpStream::connect(&addr).expect("connect"));
        server.join().expect("server");
        let Err(SetupError::Tls(message)) = result else {
            panic!("expected a TLS setup error");
        };
        assert!(
            message.contains("closed the connection during the TLS handshake"),
            "{message}"
        );
    }

    #[test]
    fn host_of_strips_port_and_brackets() {
        assert_eq!(host_of("10.0.0.4:7000"), "10.0.0.4");
        assert_eq!(host_of("[::1]:7000"), "::1");
        assert_eq!(host_of("keep.local:7000"), "keep.local");
        assert_eq!(host_of("keep.local"), "keep.local");
    }
}
