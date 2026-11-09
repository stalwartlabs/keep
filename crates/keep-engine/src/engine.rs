/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::collections::VecDeque;

use crate::cap::{Cap, Measured};
use crate::entry::{Compact, LongSlot, Stored};
use crate::hash::KeyHasher;
use crate::key::{Key, KeyClass};
use crate::table::{LineTable, Room};
use crate::time::{Deadline, Time};
use crate::value::{LARGE_MIN, Values};

pub type KeepEngine = Engine;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Hit<'a> {
    pub value: &'a [u8],
    pub deadline: Deadline,
}

pub struct Engine {
    pub(crate) hasher: KeyHasher,
    pub(crate) k8: LineTable<Compact<8>, 5, 8>,
    pub(crate) k16: LineTable<Compact<16>, 4, 8>,
    pub(crate) k24: LineTable<Compact<24>, 3, 8>,
    pub(crate) k32: LineTable<Compact<32>, 4, 8>,
    pub(crate) k40: LineTable<Compact<40>, 4, 8>,
    pub(crate) k48: LineTable<Compact<48>, 4, 8>,
    pub(crate) long: LineTable<LongSlot, 3, 8>,
    pub(crate) values: Values,
    pub(crate) purges: VecDeque<crate::purge::PendingPurge>,
    pub(crate) cap: Option<Cap>,
    pub(crate) room: Room,
    pub(crate) measured: Measured,
    pub(crate) large_evicted: u64,
    pub(crate) large_orphaned: u64,
    pub(crate) cold_from: usize,
    pub(crate) credit: usize,
}

macro_rules! with_table {
    ($engine:expr, $class:expr, |$table:ident| $body:expr) => {
        match $class {
            $crate::key::KeyClass::K8 => {
                let $table = &mut $engine.k8;
                $body
            }
            $crate::key::KeyClass::K16 => {
                let $table = &mut $engine.k16;
                $body
            }
            $crate::key::KeyClass::K24 => {
                let $table = &mut $engine.k24;
                $body
            }
            $crate::key::KeyClass::K32 => {
                let $table = &mut $engine.k32;
                $body
            }
            $crate::key::KeyClass::K40 => {
                let $table = &mut $engine.k40;
                $body
            }
            $crate::key::KeyClass::K48 => {
                let $table = &mut $engine.k48;
                $body
            }
            $crate::key::KeyClass::Long => {
                let $table = &mut $engine.long;
                $body
            }
        }
    };
}

macro_rules! with_table_ref {
    ($engine:expr, $class:expr, |$table:ident| $body:expr) => {
        match $class {
            $crate::key::KeyClass::K8 => {
                let $table = &$engine.k8;
                $body
            }
            $crate::key::KeyClass::K16 => {
                let $table = &$engine.k16;
                $body
            }
            $crate::key::KeyClass::K24 => {
                let $table = &$engine.k24;
                $body
            }
            $crate::key::KeyClass::K32 => {
                let $table = &$engine.k32;
                $body
            }
            $crate::key::KeyClass::K40 => {
                let $table = &$engine.k40;
                $body
            }
            $crate::key::KeyClass::K48 => {
                let $table = &$engine.k48;
                $body
            }
            $crate::key::KeyClass::Long => {
                let $table = &$engine.long;
                $body
            }
        }
    };
}

pub(crate) use {with_table, with_table_ref};

impl Engine {
    pub fn new(seed: u64) -> Self {
        let hasher = KeyHasher::with_seed(seed);
        Engine {
            k8: LineTable::new(hasher.clone()),
            k16: LineTable::new(hasher.clone()),
            k24: LineTable::new(hasher.clone()),
            k32: LineTable::new(hasher.clone()),
            k40: LineTable::new(hasher.clone()),
            k48: LineTable::new(hasher.clone()),
            long: LineTable::new(hasher.clone()),
            hasher,
            values: Values::default(),
            purges: VecDeque::new(),
            cap: None,
            room: Room::Free,
            measured: Measured::default(),
            large_evicted: 0,
            large_orphaned: 0,
            cold_from: LARGE_MIN,
            credit: 0,
        }
    }

    pub fn set_cap(&mut self, cap: Option<Cap>) {
        self.cap = cap;
        self.room = Room::Free;
        self.cold_from = LARGE_MIN;
        if let Some(cap) = cap {
            self.measure(cap);
        }
    }

    pub(crate) fn footprint(&self) -> usize {
        self.values.memory()
            + self.k8.memory()
            + self.k16.memory()
            + self.k24.memory()
            + self.k32.memory()
            + self.k40.memory()
            + self.k48.memory()
            + self.long.memory()
    }

    pub fn len(&self) -> usize {
        self.k8.len()
            + self.k16.len()
            + self.k24.len()
            + self.k32.len()
            + self.k40.len()
            + self.k48.len()
            + self.long.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn memory(&self) -> usize {
        self.footprint()
    }

    pub fn class_len(&self, class: KeyClass) -> usize {
        with_table_ref!(self, class, |table| table.len())
    }

    #[inline]
    pub fn prefetch(&self, key: Key<'_>) {
        let hash = self.hasher.hash(&key);
        with_table_ref!(self, key.class(), |table| table.prefetch(hash))
    }

    pub fn for_each_live(&self, now: Time, mut visit: impl FnMut(&[u8], &[u8], Deadline)) {
        let values = &self.values;
        let mut each = |key: &[u8], value: &crate::value::InlineValue, deadline: Deadline| {
            if !deadline.is_expired(now) {
                visit(key, value.get(values), deadline);
            }
        };
        self.k8
            .for_each(&mut |e| each(e.key(), e.value(), e.deadline()));
        self.k16
            .for_each(&mut |e| each(e.key(), e.value(), e.deadline()));
        self.k24
            .for_each(&mut |e| each(e.key(), e.value(), e.deadline()));
        self.k32
            .for_each(&mut |e| each(e.key(), e.value(), e.deadline()));
        self.k40
            .for_each(&mut |e| each(e.key(), e.value(), e.deadline()));
        self.k48
            .for_each(&mut |e| each(e.key(), e.value(), e.deadline()));
        self.long
            .for_each(&mut |e| each(e.key(), e.value(), e.deadline()));
    }
}
