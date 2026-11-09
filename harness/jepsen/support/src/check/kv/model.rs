/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum State {
    Absent,
    Value(u32),
    Counter(i64),
    Present,
    Any,
}

const _: () = assert!(size_of::<State>() <= 16);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Action {
    Get(Option<u32>),
    Exists(bool),
    Set(u32),
    SetNx { value: u32, set: Option<bool> },
    Delete(Option<bool>),
    Incr { delta: i64, count: Option<i64> },
    IncrRefused { delta: i64, overflow: bool },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ValueInfo {
    pub counter: Option<i64>,
    pub first_write: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Pins<'a> {
    pub values: &'a [ValueInfo],
    pub since: u64,
}

impl Pins<'_> {
    fn info(self, value: u32) -> Option<ValueInfo> {
        self.values.get(usize::try_from(value).ok()?).copied()
    }

    fn pin(self, value: u32) -> Option<State> {
        let info = self.info(value)?;
        match info.first_write {
            first if first < self.since => Some(State::Value(value)),
            u64::MAX => info.counter.map(State::Counter),
            _ => None,
        }
    }
}

impl Action {
    pub fn reads(self) -> bool {
        matches!(self, Action::Get(_) | Action::Exists(_))
    }

    pub fn creates(self) -> bool {
        matches!(
            self,
            Action::Set(_)
                | Action::SetNx {
                    set: Some(true) | None,
                    ..
                }
                | Action::Incr { .. }
        )
    }

    pub fn written(self) -> Option<u32> {
        match self {
            Action::Set(value)
            | Action::SetNx {
                value,
                set: Some(true) | None,
            } => Some(value),
            _ => None,
        }
    }

    pub fn saw_present(self) -> bool {
        matches!(
            self,
            Action::Get(Some(_))
                | Action::Exists(true)
                | Action::SetNx {
                    set: Some(false),
                    ..
                }
                | Action::Delete(Some(true))
                | Action::IncrRefused { .. }
        )
    }

    pub fn step(self, state: State, pins: Pins<'_>) -> Option<State> {
        let unpinned = matches!(state, State::Present | State::Any);
        match self {
            Action::Get(None) | Action::Exists(false) => match state {
                State::Absent | State::Any => Some(State::Absent),
                _ => None,
            },
            Action::Exists(true) => match state {
                State::Absent => None,
                State::Any => Some(State::Present),
                _ => Some(state),
            },
            Action::Get(Some(read)) => match state {
                State::Value(value) => (value == read).then_some(state),
                State::Absent => None,
                State::Counter(count) => (pins.info(read)?.counter == Some(count)).then_some(state),
                State::Present | State::Any => pins.pin(read),
            },
            Action::Set(value) => Some(State::Value(value)),
            Action::SetNx { value, set } => match (state, set) {
                (State::Absent | State::Any, Some(true) | None) => Some(State::Value(value)),
                (State::Absent, Some(false)) => None,
                (State::Any, Some(false)) => Some(State::Present),
                (_, Some(false) | None) => Some(state),
                (_, Some(true)) => None,
            },
            Action::Delete(removed) => match (state, removed) {
                (State::Any, _) | (State::Absent, Some(false) | None) => Some(State::Absent),
                (State::Absent, Some(true)) => None,
                (_, Some(true) | None) => Some(State::Absent),
                (_, Some(false)) => None,
            },
            Action::IncrRefused { delta, overflow } => match state {
                State::Present | State::Any => Some(State::Present),
                State::Value(_) if !overflow => Some(state),
                State::Counter(old) if overflow && old.checked_add(delta).is_none() => Some(state),
                _ => None,
            },
            Action::Incr { delta, count } if unpinned => match count {
                Some(count) => count.checked_sub(delta).map(|_| State::Counter(count)),
                None => Some(State::Present),
            },
            Action::Incr { delta, count } => {
                let next = match state {
                    State::Absent => Some(delta),
                    State::Counter(old) => old.checked_add(delta),
                    _ => None,
                };
                match (next, count) {
                    (Some(next), Some(count)) => (next == count).then_some(State::Counter(next)),
                    (Some(next), None) => Some(State::Counter(next)),
                    (None, Some(_)) => None,
                    (None, None) => Some(state),
                }
            }
        }
    }
}
