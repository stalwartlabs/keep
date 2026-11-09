/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

#[cfg(keep_observe)]
pub(crate) use on::Probe;

#[cfg(not(keep_observe))]
pub(crate) use off::Probe;

#[cfg(keep_observe)]
mod on {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};
    use std::time::{Duration, Instant};

    use crate::error::Error;

    const NONE: u64 = u64::MAX;

    #[derive(Clone)]
    pub(crate) struct Probe(Option<Arc<Earliest>>);

    struct Earliest {
        base: Instant,
        sent: AtomicU64,
        server: Mutex<Option<SocketAddr>>,
    }

    pub(crate) struct Sent<'a> {
        pending: Option<&'a Earliest>,
        at: u64,
        server: Option<SocketAddr>,
    }

    impl Probe {
        pub const UNOBSERVED: Probe = Probe(None);

        pub fn fresh() -> Probe {
            Probe(Some(Arc::new(Earliest {
                base: Instant::now(),
                sent: AtomicU64::new(NONE),
                server: Mutex::new(None),
            })))
        }

        pub fn stamp(&self) -> Sent<'_> {
            self.sent(None)
        }

        pub fn stamp_at(&self, server: SocketAddr) -> Sent<'_> {
            self.sent(Some(server))
        }

        fn sent(&self, server: Option<SocketAddr>) -> Sent<'_> {
            let pending = self.0.as_deref();
            let at = pending.map_or(NONE, |earliest| {
                u64::try_from(earliest.base.elapsed().as_nanos()).unwrap_or(NONE - 1)
            });
            Sent {
                pending,
                at,
                server,
            }
        }

        pub fn take_server(&self) -> Option<SocketAddr> {
            self.0
                .as_ref()?
                .server
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
        }

        pub fn take(&self) -> Option<Instant> {
            let earliest = self.0.as_ref()?;
            match earliest.sent.swap(NONE, Ordering::Relaxed) {
                NONE => None,
                sent => Some(earliest.base + Duration::from_nanos(sent)),
            }
        }
    }

    impl Sent<'_> {
        pub fn observe(&mut self, error: Option<&Error>) {
            if let Some(earliest) = self.pending
                && error.is_none_or(|error| !error.not_executed())
            {
                self.record(earliest);
            }
        }

        fn record(&self, earliest: &Earliest) {
            earliest.sent.fetch_min(self.at, Ordering::Relaxed);
            if let Some(server) = self.server {
                *earliest
                    .server
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = Some(server);
            }
        }

        pub fn settle(mut self) {
            self.pending = None;
        }

        pub fn finish(mut self, error: Option<&Error>) {
            self.observe(error);
            self.settle();
        }
    }

    impl Drop for Sent<'_> {
        fn drop(&mut self) {
            if let Some(earliest) = self.pending {
                self.record(earliest);
            }
        }
    }
}

#[cfg(all(test, keep_tests, keep_observe))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-client/observe.rs"]
mod tests;

#[cfg(not(keep_observe))]
mod off {
    use std::net::SocketAddr;

    use crate::error::Error;

    #[derive(Clone, Copy)]
    pub(crate) struct Probe;

    pub(crate) struct Sent;

    impl Probe {
        pub const UNOBSERVED: Probe = Probe;

        pub fn stamp(&self) -> Sent {
            Sent
        }

        pub fn stamp_at(&self, _: SocketAddr) -> Sent {
            Sent
        }
    }

    impl Sent {
        pub fn observe(&mut self, _: Option<&Error>) {}

        pub fn settle(self) {}

        pub fn finish(self, _: Option<&Error>) {}
    }
}
