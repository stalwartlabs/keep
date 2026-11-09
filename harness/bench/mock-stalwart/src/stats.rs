/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::hist::AtomicHistogram;
use hdrhistogram::{CreationError, Histogram};
use std::sync::atomic::{AtomicU64, Ordering};

macro_rules! counters {
    ($name:ident, $snapshot:ident { $($field:ident),+ $(,)? }) => {
        #[derive(Default)]
        pub struct $name {
            $(pub $field: std::sync::atomic::AtomicU64),+
        }

        #[derive(Debug, Default, Clone, Copy, serde::Serialize)]
        pub struct $snapshot {
            $(pub $field: u64),+
        }

        impl $name {
            pub fn snapshot(&self) -> $snapshot {
                $snapshot {
                    $($field: self.$field.load(std::sync::atomic::Ordering::Relaxed)),+
                }
            }
        }
    };
}

pub(crate) use counters;

#[inline]
pub fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn add(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

#[derive(Debug)]
pub struct Tagged {
    pub tag: String,
    pub histogram: Histogram<u64>,
}

impl Tagged {
    pub fn of(tag: impl Into<String>, histogram: &AtomicHistogram) -> Result<Self, CreationError> {
        Ok(Tagged {
            tag: tag.into(),
            histogram: histogram.to_histogram()?,
        })
    }
}
