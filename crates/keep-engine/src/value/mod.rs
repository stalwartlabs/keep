/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

mod arena;
mod large;

use std::mem;

use crate::lock::Token;

use arena::{Arena, Place};
use large::{LargeQueue, NodeId};

pub use arena::{ARENA_MIN, LARGE_MIN, slot_size};

pub const INLINE_VALUE_LEN: usize = 8;

const MALLOC_HEADER: usize = 8;
const MALLOC_ALIGN: usize = 16;

#[inline(always)]
pub const fn heap_size(len: usize) -> usize {
    (len + MALLOC_HEADER + MALLOC_ALIGN - 1) & !(MALLOC_ALIGN - 1)
}

const OUT_OF_LINE: u8 = u8::MAX;
const COUNTER: u8 = u8::MAX - 1;
const TOKEN: u8 = u8::MAX - 2;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ValueKind {
    Bytes,
    Counter,
    Token,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
#[repr(C)]
pub struct InlineValue {
    len: u8,
    bytes: [u8; INLINE_VALUE_LEN],
}

impl InlineValue {
    pub fn store(value: &[u8], values: &mut Values) -> InlineValue {
        match <[u8; INLINE_VALUE_LEN]>::try_from(value) {
            Ok(bytes) => InlineValue {
                len: INLINE_VALUE_LEN as u8,
                bytes,
            },
            Err(_) if value.len() < INLINE_VALUE_LEN => {
                let mut bytes = [0; INLINE_VALUE_LEN];
                bytes
                    .iter_mut()
                    .zip(value)
                    .for_each(|(slot, &byte)| *slot = byte);
                InlineValue {
                    len: value.len() as u8,
                    bytes,
                }
            }
            Err(_) => InlineValue::out_of_line_at(values.insert(value)),
        }
    }

    pub fn large(key: &[u8], value: &[u8], values: &mut Values) -> InlineValue {
        InlineValue::out_of_line_at(values.insert_large(key, value))
    }

    #[inline]
    fn out_of_line_at(id: u32) -> InlineValue {
        let [a, b, c, d] = id.to_le_bytes();
        InlineValue {
            len: OUT_OF_LINE,
            bytes: [a, b, c, d, 0, 0, 0, 0],
        }
    }

    #[inline]
    pub fn counter(counter: i64) -> InlineValue {
        InlineValue {
            len: COUNTER,
            bytes: counter.to_le_bytes(),
        }
    }

    #[inline]
    pub fn token(token: Token) -> InlineValue {
        InlineValue {
            len: TOKEN,
            bytes: token.bytes(),
        }
    }

    pub fn of_kind(kind: ValueKind, value: &[u8], values: &mut Values) -> InlineValue {
        match (kind, <[u8; INLINE_VALUE_LEN]>::try_from(value)) {
            (ValueKind::Counter, Ok(bytes)) => InlineValue::counter(i64::from_le_bytes(bytes)),
            (ValueKind::Token, Ok(bytes)) => InlineValue::token(Token::new(bytes)),
            _ => InlineValue::store(value, values),
        }
    }

    #[inline]
    pub fn get<'a>(&'a self, values: &'a Values) -> &'a [u8] {
        match self.out_of_line() {
            Some(id) => values.get(id),
            None => {
                let len = usize::from(self.len).min(INLINE_VALUE_LEN);
                self.bytes.get(..len).unwrap_or_default()
            }
        }
    }

    #[inline]
    pub fn kind(&self) -> ValueKind {
        match self.len {
            COUNTER => ValueKind::Counter,
            TOKEN => ValueKind::Token,
            _ => ValueKind::Bytes,
        }
    }

    #[inline]
    pub fn as_counter(&self) -> Option<i64> {
        (self.len == COUNTER).then(|| i64::from_le_bytes(self.bytes))
    }

    #[inline]
    pub fn holds(&self, token: Token) -> bool {
        self.len == TOKEN && self.bytes == token.bytes()
    }

    pub fn replace(&mut self, kind: ValueKind, value: &[u8], values: &mut Values) {
        self.release(values);
        *self = InlineValue::of_kind(kind, value, values);
    }

    #[inline]
    pub fn release(&mut self, values: &mut Values) {
        if let Some(id) = self.out_of_line() {
            values.remove(id);
            *self = InlineValue::default();
        }
    }

    #[inline]
    fn out_of_line(&self) -> Option<u32> {
        let [a, b, c, d, ..] = self.bytes;
        (self.len == OUT_OF_LINE).then_some(u32::from_le_bytes([a, b, c, d]))
    }
}

#[derive(Clone, Copy)]
struct ArenaValue {
    place: Place,
    len: u32,
    node: Option<NodeId>,
}

enum Blob {
    Heap(Box<[u8]>),
    Arena(ArenaValue),
}

const _: () = assert!(size_of::<Blob>() == 24);

#[derive(Default)]
pub struct Values {
    slots: Vec<Blob>,
    free: Vec<u32>,
    heap_bytes: usize,
    arena: Arena,
    queue: LargeQueue,
}

impl Values {
    #[inline(always)]
    fn insert(&mut self, value: &[u8]) -> u32 {
        if value.len() >= ARENA_MIN
            && let Some(place) = self.arena.store(value)
        {
            return self.occupy(Blob::Arena(ArenaValue {
                place,
                len: value.len() as u32,
                node: None,
            }));
        }
        self.heap_bytes += heap_size(value.len());
        self.occupy(Blob::Heap(value.into()))
    }

    #[inline]
    fn occupy(&mut self, blob: Blob) -> u32 {
        match self.free.pop() {
            Some(id) => {
                if let Some(slot) = self.slots.get_mut(id as usize) {
                    mem::forget(mem::replace(slot, blob));
                }
                id
            }
            None => {
                self.slots.push(blob);
                (self.slots.len() - 1) as u32
            }
        }
    }

    #[inline]
    fn get(&self, id: u32) -> &[u8] {
        match self.slots.get(id as usize) {
            Some(Blob::Heap(bytes)) => bytes,
            Some(Blob::Arena(stored)) => self.arena.get(stored.place, stored.len as usize),
            None => &[],
        }
    }

    fn remove(&mut self, id: u32) {
        if let Some(slot) = self.slots.get_mut(id as usize) {
            match mem::replace(slot, Blob::Heap(Box::default())) {
                Blob::Heap(bytes) => self.heap_bytes -= heap_size(bytes.len()),
                Blob::Arena(stored) => match stored.node {
                    None => self.arena.free(stored.place),
                    Some(node) => self.remove_large(stored.place, node),
                },
            }
            self.free.push(id);
        }
    }

    pub fn frees(&self, value: &InlineValue, class: Option<u16>) -> (usize, bool) {
        match value
            .out_of_line()
            .and_then(|id| self.slots.get(id as usize))
        {
            Some(Blob::Heap(bytes)) => (heap_size(bytes.len()), false),
            Some(Blob::Arena(stored)) if stored.node.is_some() => {
                (self.arena.slot_bytes(stored.place), false)
            }
            Some(Blob::Arena(stored)) => (0, Some(stored.place.class) == class),
            None => (0, false),
        }
    }

    pub fn fresh_class(&self, len: usize) -> Option<u16> {
        self.arena.fresh_class(len)
    }

    pub fn maintain(&mut self, populate: bool) {
        if populate {
            self.arena.maintain();
        } else {
            self.arena.release();
        }
    }

    pub fn warm_bytes(&self) -> usize {
        self.arena.warm_bytes()
    }

    pub fn stuck_warm(&self) -> usize {
        self.arena.stuck_warm()
    }

    pub fn retained(&self) -> usize {
        self.arena.retain_cap()
    }

    pub fn memory(&self) -> usize {
        self.heap_bytes
            + self.arena.memory()
            + self.queue.memory()
            + self.slots.capacity() * size_of::<Blob>()
            + self.free.capacity() * size_of::<u32>()
    }
}
