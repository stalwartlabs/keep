/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: Apache-2.0 OR MIT
 */

use std::mem;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::Waker;
use std::time::Instant;

use foldhash::HashSet;
use keep_proto::{Command, MAX_SUBSCRIPTIONS, Mark, ReplyMode, Request, Status, Topic};

use super::{Conn, State};
use crate::answer::Expect;
use crate::stream::{Kind, LINGER, RETRY, Shared};

pub(crate) struct Route {
    shared: Arc<Shared>,
    lane: u16,
    state: RouteState,
    again: bool,
    unwanted_since: Option<Instant>,
    retry_at: Option<Instant>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RouteState {
    Down,
    Pending(u32),
    Live,
    Closing(u32),
}

#[derive(Default)]
pub(crate) struct Routes {
    list: Vec<Route>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Want {
    Keep,
    Linger,
    Drop,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Settled {
    Yes,
    Later,
}

impl Route {
    fn delivers(&self, topic: Topic) -> bool {
        self.shared.topic == topic
            && matches!(self.state, RouteState::Live | RouteState::Closing(_))
    }

    fn lost(&mut self) -> Option<Waker> {
        let waker = {
            let mut inbox = self.shared.lock();
            inbox.down(self.lane);
            inbox.mark(self.lane, Kind::Lost, 0)
        };
        self.again = true;
        self.shared.changed.notify_waiters();
        waker
    }

    fn is_live(&self) -> bool {
        self.state == RouteState::Live
    }
}

impl Routes {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    pub fn has_live(&self) -> bool {
        self.list.iter().any(Route::is_live)
    }

    pub fn deliver(&self, topic: Topic) -> Option<(&Arc<Shared>, u16)> {
        let route = self.list.iter().find(|route| route.delivers(topic))?;
        Some((&route.shared, route.lane))
    }

    pub fn heartbeat(&mut self, mark: Mark) -> Option<Option<Waker>> {
        let route = self.list.iter().find(|route| route.delivers(mark.topic))?;
        Some(
            route
                .shared
                .lock()
                .mark(route.lane, Kind::Heartbeat, mark.last_seq),
        )
    }

    pub fn end(&mut self, mark: Mark) -> Option<Option<Waker>> {
        let index = self
            .list
            .iter()
            .position(|route| route.delivers(mark.topic))?;
        let route = self.list.get_mut(index)?;
        let waker = {
            let mut inbox = route.shared.lock();
            inbox.down(route.lane);
            inbox.mark(route.lane, Kind::End, mark.last_seq)
        };
        route.shared.changed.notify_waiters();
        if let RouteState::Closing(_) = route.state {
            self.list.swap_remove(index);
        } else {
            route.state = RouteState::Down;
            route.again = true;
        }
        Some(waker)
    }

    pub fn response(&mut self, id: u32, status: Status) -> Option<Option<Waker>> {
        let index = self.list.iter().position(|route| {
            matches!(route.state, RouteState::Pending(request) | RouteState::Closing(request) if request == id)
        })?;
        let route = self.list.get_mut(index)?;
        let waker = match (route.state, status) {
            (RouteState::Pending(_), Status::Ok | Status::Conflict) => {
                route.state = RouteState::Live;
                let waker = route.shared.lock().live(route.lane, route.again);
                route.again = true;
                route.shared.changed.notify_waiters();
                waker
            }
            (RouteState::Pending(_), _) => {
                route.state = RouteState::Down;
                route.retry_at = Some(Instant::now() + RETRY);
                route.shared.changed.notify_waiters();
                None
            }
            (RouteState::Closing(_), Status::Ok) => None,
            (RouteState::Closing(_), Status::NotFound) => {
                let waker = route.lost();
                self.list.swap_remove(index);
                waker
            }
            (RouteState::Closing(_), _) => {
                route.state = RouteState::Live;
                None
            }
            (RouteState::Down | RouteState::Live, _) => None,
        };
        Some(waker)
    }

    pub fn fail(&mut self, wakers: &mut Vec<Waker>) -> bool {
        let had = !self.list.is_empty();
        self.list.retain_mut(|route| {
            if matches!(route.state, RouteState::Live | RouteState::Closing(_)) {
                wakers.extend(route.lost());
            }
            if let RouteState::Closing(_) = route.state {
                return false;
            }
            route.state = RouteState::Down;
            route.shared.changed.notify_waiters();
            true
        });
        had
    }
}

impl Conn {
    pub(super) fn resubscribe(&self, st: &mut State) {
        let mut routes = mem::take(&mut st.routes.list);
        let mut held = routes
            .iter()
            .filter(|route| route.state != RouteState::Down)
            .count();
        routes.retain_mut(|route| {
            if route.shared.is_dropped() {
                return route.state != RouteState::Down;
            }
            if route.state == RouteState::Down
                && held < MAX_SUBSCRIPTIONS
                && let Some(id) = self.send(
                    st,
                    Command::Subscribe {
                        topic: route.shared.topic,
                    },
                )
            {
                route.state = RouteState::Pending(id);
                route.retry_at = None;
                held += 1;
            }
            true
        });
        st.routes.list = routes;
    }

    fn send(&self, st: &mut State, command: Command<'_>) -> Option<u32> {
        if !st.accepting() {
            return None;
        }
        let id = st.slots.insert(Expect::Status)?;
        st.slots.abandon(id);
        Request {
            id,
            reply: ReplyMode::Always,
            command,
        }
        .encode(&mut st.wbuf);
        st.stats.frames_out += 1;
        st.queued_frames += 1;
        Some(id)
    }

    pub fn reconcile(
        &self,
        server: SocketAddr,
        wanted: &[Arc<Shared>],
        want: impl Fn(&Shared) -> Want,
        now: Instant,
    ) -> Settled {
        let mut st = self.lock();
        let mut present: Vec<*const Shared> = st
            .routes
            .list
            .iter()
            .map(|route| Arc::as_ptr(&route.shared))
            .collect();
        present.sort_unstable();
        for shared in wanted {
            if present.binary_search(&Arc::as_ptr(shared)).is_err()
                && let Some(lane) = shared.lane(server)
            {
                st.routes.list.push(Route {
                    shared: Arc::clone(shared),
                    lane,
                    state: RouteState::Down,
                    again: false,
                    unwanted_since: None,
                    retry_at: None,
                });
            }
        }
        let mut routes = mem::take(&mut st.routes.list);
        let mut busy: HashSet<Topic> = routes
            .iter()
            .filter(|route| route.state != RouteState::Down)
            .map(|route| route.shared.topic)
            .collect();
        let mut settled = Settled::Yes;
        routes.retain_mut(|route| {
            let topic = route.shared.topic;
            let want = match route.shared.is_dropped() {
                true => Want::Drop,
                false => want(&route.shared),
            };
            if want == Want::Keep {
                route.unwanted_since = None;
            }
            match (want, route.state) {
                (Want::Keep, RouteState::Down) => {
                    if route.retry_at.is_some_and(|at| at > now) {
                        settled = Settled::Later;
                    } else if busy.len() < MAX_SUBSCRIPTIONS
                        && !busy.contains(&topic)
                        && let Some(id) = self.send(&mut st, Command::Subscribe { topic })
                    {
                        route.state = RouteState::Pending(id);
                        route.retry_at = None;
                        busy.insert(topic);
                    }
                    true
                }
                (Want::Keep, _) => true,
                (_, RouteState::Down) => false,
                (_, RouteState::Pending(_) | RouteState::Closing(_)) => {
                    settled = Settled::Later;
                    true
                }
                (Want::Linger, RouteState::Live)
                    if now < *route.unwanted_since.get_or_insert(now) + LINGER =>
                {
                    settled = Settled::Later;
                    true
                }
                (_, RouteState::Live) => {
                    if let Some(id) = self.send(&mut st, Command::Unsubscribe { topic }) {
                        route.state = RouteState::Closing(id);
                    }
                    settled = Settled::Later;
                    true
                }
            }
        });
        st.routes.list = routes;
        self.kick(st);
        settled
    }

    pub fn routeless(&self) -> bool {
        self.lock().routes.is_empty()
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-client/conn/routes.rs"]
mod tests;
