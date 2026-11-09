/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::{error, fmt, fs};

use memchr::memmem;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{AlgorithmIdentifier, PrivateKeyDer, SubjectPublicKeyInfoDer};

const PEM_MARKER: &[u8] = b"-----BEGIN";
const DER_SEQUENCE: u8 = 0x30;
const DER_BIT_STRING: u8 = 0x03;

/// A key or certificate file that cannot be used: what it is for, its path and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyFileError(Box<str>);

impl KeyFileError {
    /// An error about the file `path` used as `what`.
    pub fn new(what: &str, path: &str, reason: impl fmt::Display) -> KeyFileError {
        KeyFileError(format!("{what} {path}: {reason}").into_boxed_str())
    }

    /// The error message.
    pub fn into_message(self) -> Box<str> {
        self.0
    }
}

impl fmt::Display for KeyFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl error::Error for KeyFileError {}

fn read(what: &str, path: &str) -> Result<Vec<u8>, KeyFileError> {
    fs::read(path).map_err(|err| KeyFileError::new(what, path, err))
}

fn is_pem(bytes: &[u8]) -> bool {
    memmem::find(bytes, PEM_MARKER).is_some()
}

/// Every PEM item of type `T` in the file, or the whole file as one DER
/// item.
pub fn der_items<T: PemObject + From<Vec<u8>>>(
    what: &str,
    path: &str,
) -> Result<Vec<T>, KeyFileError> {
    let bytes = read(what, path)?;
    let items = if is_pem(&bytes) {
        T::pem_slice_iter(&bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| KeyFileError::new(what, path, err))?
    } else {
        vec![T::from(bytes)]
    };
    if items.is_empty() {
        return Err(KeyFileError::new(what, path, "nothing found"));
    }
    Ok(items)
}

/// A private key, PEM (PKCS#8, SEC1 or PKCS#1) or DER.
pub fn private_key(what: &str, path: &str) -> Result<PrivateKeyDer<'static>, KeyFileError> {
    let bytes = read(what, path)?;
    if is_pem(&bytes) {
        PrivateKeyDer::from_pem_slice(&bytes).map_err(|err| KeyFileError::new(what, path, err))
    } else {
        PrivateKeyDer::try_from(bytes).map_err(|err| KeyFileError::new(what, path, err))
    }
}

/// The public keys of a key list (raw public keys, RFC 7250), each of an
/// algorithm in `supported`, the provider's signature algorithms.
pub fn public_keys(
    what: &str,
    path: &str,
    supported: &[AlgorithmIdentifier],
) -> Result<Vec<SubjectPublicKeyInfoDer<'static>>, KeyFileError> {
    let keys = der_items::<SubjectPublicKeyInfoDer<'static>>(what, path)?;
    for key in &keys {
        let known = spki_algorithm(key)
            .is_some_and(|algorithm| supported.iter().any(|known| known.as_ref() == algorithm));
        if !known {
            return Err(KeyFileError::new(
                what,
                path,
                "not a public key of a supported algorithm",
            ));
        }
    }
    Ok(keys)
}

fn der_item(input: &[u8], tag: u8) -> Option<(&[u8], &[u8])> {
    let (&found, rest) = input.split_first()?;
    if found != tag {
        return None;
    }
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = match first {
        0..=0x7f => (usize::from(first), rest),
        0x81 => rest
            .split_first()
            .map(|(&len, rest)| (usize::from(len), rest))?,
        0x82 => rest
            .split_first_chunk()
            .map(|(&len, rest)| (usize::from(u16::from_be_bytes(len)), rest))?,
        _ => return None,
    };
    rest.split_at_checked(len)
}

fn spki_algorithm(spki: &[u8]) -> Option<&[u8]> {
    let (body, []) = der_item(spki, DER_SEQUENCE)? else {
        return None;
    };
    let (algorithm, key) = der_item(body, DER_SEQUENCE)?;
    match der_item(key, DER_BIT_STRING)? {
        ([0, _, ..], []) => Some(algorithm),
        _ => None,
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-proto/keys.rs"]
mod tests;
