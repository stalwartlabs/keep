/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::time::Duration;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Nanos(pub u64);

impl Nanos {
    pub const ZERO: Nanos = Nanos(0);
    pub const MAX: Nanos = Nanos(u64::MAX);

    #[inline]
    pub fn plus(self, span: Duration) -> Nanos {
        Nanos(
            self.0
                .saturating_add(u64::try_from(span.as_nanos()).unwrap_or(u64::MAX)),
        )
    }

    #[inline]
    pub fn minus(self, span: Duration) -> Nanos {
        Nanos(
            self.0
                .saturating_sub(u64::try_from(span.as_nanos()).unwrap_or(u64::MAX)),
        )
    }

    #[inline]
    pub fn since(self, earlier: Nanos) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }
}
