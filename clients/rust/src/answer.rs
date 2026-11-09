/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use keep_proto::{Response, Status};

use crate::error::Error;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Expect {
    Status,
    Counter,
    Bytes,
}

#[derive(Debug)]
pub(crate) struct Answer {
    pub status: Status,
    pub value: AnswerValue,
}

#[derive(Debug)]
pub(crate) enum AnswerValue {
    None,
    Counter(i64),
    Bytes(Vec<u8>),
}

impl Answer {
    pub fn decode(expect: Expect, response: &Response<'_>) -> Result<Answer, Error> {
        if response.status == Status::Moved {
            return <[u8; 8]>::try_from(response.value.as_bytes())
                .map(|epoch| Err(Error::Moved(u64::from_le_bytes(epoch))))
                .unwrap_or(Err(Error::UnexpectedReply));
        }
        if response.status.is_error() {
            return Err(Error::Server {
                status: response.status,
                message: String::from_utf8_lossy(response.value.as_bytes()).into(),
            });
        }
        let value = match (expect, response.status) {
            (Expect::Counter, Status::Ok) => {
                AnswerValue::Counter(response.counter().ok_or(Error::UnexpectedReply)?)
            }
            (Expect::Bytes, Status::Ok) => AnswerValue::Bytes(response.value.to_vec()),
            _ => AnswerValue::None,
        };
        Ok(Answer {
            status: response.status,
            value,
        })
    }
}
