/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::{KvError, model::IncrTtl};

#[cfg(feature = "keep")]
pub use linked::KeepKv;
#[cfg(not(feature = "keep"))]
pub use seam::KeepKv;

#[cfg(feature = "keep")]
mod linked {
    use keep_client::{Client, Ttl};

    use super::{IncrTtl, KvError};

    pub struct KeepKv {
        client: Client,
    }

    fn ttl(secs: u64) -> Result<Ttl, KvError> {
        u32::try_from(secs)
            .ok()
            .and_then(Ttl::from_secs)
            .ok_or(KvError::Unsupported(
                "Keep needs a TTL between 1 s and 2^32 - 1 s",
            ))
    }

    fn optional_ttl(secs: Option<u64>) -> Result<Option<Ttl>, KvError> {
        secs.filter(|&secs| secs > 0).map(ttl).transpose()
    }

    impl KeepKv {
        pub async fn connect(url: &str) -> Result<KeepKv, KvError> {
            Client::connect_url(url)
                .await
                .map(|client| KeepKv { client })
                .map_err(KvError::Keep)
        }

        pub fn incr_ttl(&self) -> Option<IncrTtl> {
            Some(IncrTtl::OnCreate)
        }

        pub fn stats(&self) -> keep_client::Stats {
            self.client.stats()
        }

        pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
            self.client.get(key).await.map_err(KvError::Keep)
        }

        pub async fn exists(&self, key: &[u8]) -> Result<bool, KvError> {
            self.client.exists(key).await.map_err(KvError::Keep)
        }

        pub async fn set(&self, key: &[u8], value: &[u8], ttl: Option<u64>) -> Result<(), KvError> {
            self.client
                .set(key, value, optional_ttl(ttl)?)
                .await
                .map_err(KvError::Keep)
        }

        pub async fn delete(&self, key: &[u8]) -> Result<(), KvError> {
            self.client
                .delete(key)
                .await
                .map(|_| ())
                .map_err(KvError::Keep)
        }

        pub async fn unlock(&self, key: &[u8]) -> Result<(), KvError> {
            self.client.remove_lock(key).await.map_err(KvError::Keep)
        }

        pub async fn incr(&self, key: &[u8], delta: i64, ttl: Option<u64>) -> Result<i64, KvError> {
            self.client
                .incr(key, delta, optional_ttl(ttl)?)
                .await
                .map_err(KvError::Keep)
        }

        pub async fn try_lock(&self, key: &[u8], secs: u64) -> Result<bool, KvError> {
            self.client
                .try_lock(key, ttl(secs)?)
                .await
                .map_err(KvError::Keep)
        }

        pub async fn renew(&self, key: &[u8], secs: u64) -> Result<(), KvError> {
            self.client
                .renew_lock(key, ttl(secs)?)
                .await
                .map_err(KvError::Keep)
        }

        pub async fn try_lease(&self, key: &[u8], token: u64, secs: u64) -> Result<bool, KvError> {
            self.client
                .try_lease(key, token, ttl(secs)?)
                .await
                .map_err(KvError::Keep)
        }

        pub async fn renew_lease(
            &self,
            key: &[u8],
            token: u64,
            secs: u64,
        ) -> Result<bool, KvError> {
            self.client
                .renew_lease(key, token, ttl(secs)?)
                .await
                .map_err(KvError::Keep)
        }

        pub async fn release_lease(&self, key: &[u8], token: u64) -> Result<bool, KvError> {
            self.client
                .release_lease(key, token)
                .await
                .map_err(KvError::Keep)
        }

        pub async fn set_many(
            &self,
            items: &[(&[u8], &[u8])],
            ttl: Option<u64>,
        ) -> Result<(), KvError> {
            self.client
                .set_many(items, optional_ttl(ttl)?)
                .await
                .map_err(KvError::Keep)
        }

        pub async fn delete_prefix(&self, prefix: &[u8]) -> Result<(), KvError> {
            self.client
                .delete_prefix(prefix)
                .await
                .map_err(KvError::Keep)
        }

        pub async fn flush(&self) -> Result<(), KvError> {
            for first in 0..=u8::MAX {
                self.delete_prefix(&[first]).await?;
            }
            Ok(())
        }
    }
}

#[cfg(not(feature = "keep"))]
mod seam {
    use super::{IncrTtl, KvError};

    pub enum KeepKv {}

    impl KeepKv {
        pub async fn connect(_url: &str) -> Result<KeepKv, KvError> {
            Err(KvError::Unsupported(
                "no Keep client in this build (build with --features keep)",
            ))
        }

        pub fn incr_ttl(&self) -> Option<IncrTtl> {
            match *self {}
        }

        pub async fn unlock(&self, _key: &[u8]) -> Result<(), KvError> {
            match *self {}
        }

        pub async fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
            match *self {}
        }

        pub async fn exists(&self, _key: &[u8]) -> Result<bool, KvError> {
            match *self {}
        }

        pub async fn set(
            &self,
            _key: &[u8],
            _value: &[u8],
            _ttl: Option<u64>,
        ) -> Result<(), KvError> {
            match *self {}
        }

        pub async fn delete(&self, _key: &[u8]) -> Result<(), KvError> {
            match *self {}
        }

        pub async fn incr(
            &self,
            _key: &[u8],
            _delta: i64,
            _ttl: Option<u64>,
        ) -> Result<i64, KvError> {
            match *self {}
        }

        pub async fn try_lock(&self, _key: &[u8], _ttl: u64) -> Result<bool, KvError> {
            match *self {}
        }

        pub async fn renew(&self, _key: &[u8], _ttl: u64) -> Result<(), KvError> {
            match *self {}
        }

        pub async fn try_lease(
            &self,
            _key: &[u8],
            _token: u64,
            _ttl: u64,
        ) -> Result<bool, KvError> {
            match *self {}
        }

        pub async fn renew_lease(
            &self,
            _key: &[u8],
            _token: u64,
            _ttl: u64,
        ) -> Result<bool, KvError> {
            match *self {}
        }

        pub async fn release_lease(&self, _key: &[u8], _token: u64) -> Result<bool, KvError> {
            match *self {}
        }

        pub async fn set_many(
            &self,
            _items: &[(&[u8], &[u8])],
            _ttl: Option<u64>,
        ) -> Result<(), KvError> {
            match *self {}
        }

        pub async fn delete_prefix(&self, _prefix: &[u8]) -> Result<(), KvError> {
            match *self {}
        }

        pub async fn flush(&self) -> Result<(), KvError> {
            match *self {}
        }
    }
}
