/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::io;
use std::path::Path;

use rcgen::{KeyPair, PKCS_ED25519};

const SERVERS: &str = "servers.pem";
const CLIENTS: &str = "clients.pem";
const CLIENT_KEY: &str = "client.key";

#[derive(Clone, Debug)]
pub struct RpkFiles {
    dir: Box<str>,
}

impl RpkFiles {
    pub fn generate(dir: &Path, slots: u8) -> io::Result<RpkFiles> {
        let files = RpkFiles {
            dir: dir
                .to_str()
                .ok_or_else(|| io::Error::other(format!("{} is not a UTF-8 path", dir.display())))?
                .into(),
        };
        std::fs::create_dir_all(dir)?;
        let mut servers = String::new();
        for id in 1..=slots {
            let key = new_key()?;
            std::fs::write(files.server_key(id), key.serialize_pem())?;
            servers.push_str(&key.public_key_pem());
        }
        std::fs::write(files.servers(), servers)?;
        let client = new_key()?;
        std::fs::write(files.client_key(), client.serialize_pem())?;
        std::fs::write(files.clients(), client.public_key_pem())?;
        Ok(files)
    }

    pub fn server_key(&self, id: u8) -> String {
        format!("{}/n{id}.key", self.dir)
    }

    pub fn servers(&self) -> String {
        format!("{}/{SERVERS}", self.dir)
    }

    pub fn clients(&self) -> String {
        format!("{}/{CLIENTS}", self.dir)
    }

    pub fn client_key(&self) -> String {
        format!("{}/{CLIENT_KEY}", self.dir)
    }
}

fn new_key() -> io::Result<KeyPair> {
    KeyPair::generate_for(&PKCS_ED25519).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_slot_gets_a_key_and_a_line_in_the_server_list() {
        // one private key per slot, every public key in servers.pem, one client key pair
        let dir = std::env::temp_dir().join(format!("keep-jepsen-rpk-{}", std::process::id()));
        let files = RpkFiles::generate(&dir, 3).expect("generate");
        for id in 1..=3 {
            let key = std::fs::read_to_string(files.server_key(id)).expect("server key");
            assert!(key.starts_with("-----BEGIN PRIVATE KEY-----"));
        }
        let servers = std::fs::read_to_string(files.servers()).expect("servers");
        assert_eq!(servers.matches("-----BEGIN PUBLIC KEY-----").count(), 3);
        let clients = std::fs::read_to_string(files.clients()).expect("clients");
        assert_eq!(clients.matches("-----BEGIN PUBLIC KEY-----").count(), 1);
        assert!(!servers.contains(clients.as_str()));
        assert!(std::fs::read_to_string(files.client_key()).is_ok());
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }
}
