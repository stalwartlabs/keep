/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use keep_proto::{Command, Key, LockToken, SetMode, Status, Ttl, TtlReport, Value};

use crate::answer::{Answer, AnswerValue, Expect};
use crate::client::{Client, Safety};
use crate::error::Error;

fn key_of(key: &[u8]) -> Result<Key<'_>, Error> {
    Key::new(key).ok_or(Error::BadKey(key.len()))
}

fn value_of(value: &[u8]) -> Result<Value<'_>, Error> {
    Value::new(value).ok_or(Error::ValueTooLarge(value.len()))
}

fn flag(answer: &Answer, yes: Status, no: Status) -> Result<bool, Error> {
    match answer.status {
        status if status == yes => Ok(true),
        status if status == no => Ok(false),
        _ => Err(Error::UnexpectedReply),
    }
}

fn held(answer: &Answer) -> Result<bool, Error> {
    match answer.status {
        Status::Ok => Ok(true),
        Status::NotFound | Status::Conflict => Ok(false),
        _ => Err(Error::UnexpectedReply),
    }
}

impl Client {
    async fn keyed(
        &self,
        key: Key<'_>,
        command: Command<'_>,
        expect: Expect,
        safety: Safety,
    ) -> Result<Answer, Error> {
        self.execute(Some(key.as_bytes()), &command, expect, safety)
            .await
    }

    /// Returns the value of `key`, or `None` when it is absent or expired.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let key = key_of(key)?;
        let command = Command::Get {
            key,
            ttl: TtlReport::Omit,
        };
        let answer = self
            .keyed(key, command, Expect::Bytes, Safety::Repeatable)
            .await?;
        Ok(match (answer.status, answer.value) {
            (Status::Ok, AnswerValue::Bytes(value)) => Some(value),
            _ => None,
        })
    }

    /// Whether `key` holds a live value.
    pub async fn exists(&self, key: &[u8]) -> Result<bool, Error> {
        let key = key_of(key)?;
        let answer = self
            .keyed(
                key,
                Command::Exists { key },
                Expect::Status,
                Safety::Repeatable,
            )
            .await?;
        flag(&answer, Status::Ok, Status::NotFound)
    }

    /// Not repeated after an unknown outcome: a repeat could overwrite a
    /// write another client made after the first attempt took effect.
    pub async fn set(&self, key: &[u8], value: &[u8], ttl: Option<Ttl>) -> Result<(), Error> {
        let key = key_of(key)?;
        let command = Command::Set {
            key,
            value: value_of(value)?,
            ttl,
            mode: SetMode::Always,
        };
        self.keyed(key, command, Expect::Status, Safety::Once)
            .await
            .map(|_| ())
    }

    /// Stores only when the key is absent. Not repeated after an unknown
    /// outcome: a repeat would report the caller's own write as a conflict.
    pub async fn set_if_absent(
        &self,
        key: &[u8],
        value: &[u8],
        ttl: Option<Ttl>,
    ) -> Result<bool, Error> {
        let key = key_of(key)?;
        let command = Command::Set {
            key,
            value: value_of(value)?,
            ttl,
            mode: SetMode::IfAbsent,
        };
        let answer = self
            .keyed(key, command, Expect::Status, Safety::Once)
            .await?;
        flag(&answer, Status::Ok, Status::Conflict)
    }

    /// Not repeated after an unknown outcome: a repeat could delete a write
    /// another client made after the first attempt took effect.
    pub async fn delete(&self, key: &[u8]) -> Result<bool, Error> {
        let key = key_of(key)?;
        let answer = self
            .keyed(key, Command::Delete { key }, Expect::Status, Safety::Once)
            .await?;
        flag(&answer, Status::Ok, Status::NotFound)
    }

    /// Adds `delta`; the TTL applies when the counter is created. Never
    /// repeated after an unknown outcome.
    pub async fn incr(&self, key: &[u8], delta: i64, ttl: Option<Ttl>) -> Result<i64, Error> {
        let key = key_of(key)?;
        let command = Command::Incr { key, delta, ttl };
        match self
            .keyed(key, command, Expect::Counter, Safety::Once)
            .await?
            .value
        {
            AnswerValue::Counter(value) => Ok(value),
            AnswerValue::None | AnswerValue::Bytes(_) => Err(Error::UnexpectedReply),
        }
    }

    /// Acquires `key` for `token`, or reacquires it when `token` holds it.
    pub async fn try_lock_with(
        &self,
        key: &[u8],
        ttl: Ttl,
        token: LockToken,
    ) -> Result<bool, Error> {
        let key = key_of(key)?;
        let command = Command::TryLock { key, ttl, token };
        let answer = self
            .keyed(key, command, Expect::Status, Safety::Repeatable)
            .await?;
        flag(&answer, Status::Ok, Status::Conflict)
    }

    /// Deletes `key` if `token` holds it.
    pub async fn unlock(&self, key: &[u8], token: LockToken) -> Result<bool, Error> {
        let key = key_of(key)?;
        let answer = self
            .keyed(
                key,
                Command::Unlock { key, token },
                Expect::Status,
                Safety::Removal,
            )
            .await?;
        held(&answer)
    }

    /// Extends `key` to `ttl` from now: unconditionally without a token, else
    /// only while `token` holds it. Repeated after an unknown outcome only
    /// with a token, which keeps a repeat from extending another holder's key.
    pub async fn renew(
        &self,
        key: &[u8],
        ttl: Ttl,
        token: Option<LockToken>,
    ) -> Result<bool, Error> {
        let key = key_of(key)?;
        let safety = match token {
            Some(_) => Safety::Repeatable,
            None => Safety::Once,
        };
        let answer = self
            .keyed(
                key,
                Command::Renew { key, ttl, token },
                Expect::Status,
                safety,
            )
            .await?;
        held(&answer)
    }

    /// Deletes every key under `prefix` on every server; the purge completes
    /// in bounded steps after this returns.
    pub async fn delete_prefix(&self, prefix: &[u8]) -> Result<(), Error> {
        let prefix = key_of(prefix)?;
        self.everywhere(&Command::DeletePrefix { prefix }).await
    }

    /// Round-trips a ping to a server.
    pub async fn ping(&self) -> Result<(), Error> {
        self.execute(None, &Command::Ping, Expect::Status, Safety::Repeatable)
            .await
            .map(|_| ())
    }

    /// Takes a lease: TryLock with the lease token.
    pub async fn try_lease(&self, key: &[u8], token: u64, ttl: Ttl) -> Result<bool, Error> {
        self.try_lock_with(key, ttl, LockToken::from_u64(token))
            .await
    }

    /// Renews a lease: Renew with the lease token.
    pub async fn renew_lease(&self, key: &[u8], token: u64, ttl: Ttl) -> Result<bool, Error> {
        self.renew(key, ttl, Some(LockToken::from_u64(token))).await
    }

    /// Releases a lease: Unlock with the lease token.
    pub async fn release_lease(&self, key: &[u8], token: u64) -> Result<bool, Error> {
        self.unlock(key, LockToken::from_u64(token)).await
    }

    /// Takes a lock: TryLock with a fresh token, which a retry of
    /// this call resends.
    pub async fn try_lock(&self, key: &[u8], ttl: Ttl) -> Result<bool, Error> {
        self.try_lock_with(key, ttl, self.fresh_token()).await
    }

    /// Extends whatever lock is live under `key`, another holder's included, and re-creates
    /// it with a fresh token when it expired; `HeldByOther` if another holder took it meanwhile.
    pub async fn renew_lock(&self, key: &[u8], ttl: Ttl) -> Result<(), Error> {
        if self.renew(key, ttl, None).await? {
            return Ok(());
        }
        if self.try_lock(key, ttl).await? {
            Ok(())
        } else {
            Err(Error::HeldByOther)
        }
    }

    /// Removes a lock: an unconditional Delete, never repeated
    /// after an unknown outcome: a repeat could delete the lock another
    /// holder took after the first Delete ran. The lock then ends by its TTL.
    pub async fn remove_lock(&self, key: &[u8]) -> Result<(), Error> {
        let key = key_of(key)?;
        self.keyed(key, Command::Delete { key }, Expect::Status, Safety::Once)
            .await
            .map(|_| ())
    }

    /// One pipelined batch of Sets, none repeated
    /// after an unknown outcome.
    pub async fn set_many<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        items: &[(K, V)],
        ttl: Option<Ttl>,
    ) -> Result<(), Error> {
        let requests = items
            .iter()
            .map(|(key, value)| {
                let key = key.as_ref();
                Ok((
                    key,
                    Command::Set {
                        key: key_of(key)?,
                        value: value_of(value.as_ref())?,
                        ttl,
                        mode: SetMode::Always,
                    },
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        self.execute_all(&requests).await
    }

    /// One pipelined batch of Gets.
    pub async fn get_many<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let requests = keys
            .iter()
            .map(|key| {
                let key = key.as_ref();
                Ok((
                    key,
                    Command::Get {
                        key: key_of(key)?,
                        ttl: TtlReport::Omit,
                    },
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        self.fetch_all(&requests).await
    }

    /// One pipelined batch of Deletes, none
    /// repeated after an unknown outcome.
    pub async fn delete_many<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<(), Error> {
        let requests = keys
            .iter()
            .map(|key| {
                let key = key.as_ref();
                Ok((key, Command::Delete { key: key_of(key)? }))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        self.execute_all(&requests).await
    }

    async fn execute_all(&self, requests: &[(&[u8], Command<'_>)]) -> Result<(), Error> {
        self.execute_many(requests, Expect::Status, Safety::Once)
            .await
            .into_iter()
            .try_for_each(|result| result.map(|_| ()))
    }

    async fn fetch_all(
        &self,
        requests: &[(&[u8], Command<'_>)],
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        self.execute_many(requests, Expect::Bytes, Safety::Repeatable)
            .await
            .into_iter()
            .map(|result| {
                result.map(|answer| match (answer.status, answer.value) {
                    (Status::Ok, AnswerValue::Bytes(value)) => Some(value),
                    _ => None,
                })
            })
            .collect()
    }
}
