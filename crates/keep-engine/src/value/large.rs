/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::mem;
use std::num::NonZeroU32;

use super::arena::Place;
use super::{ArenaValue, Blob, Values};

pub(super) type NodeId = NonZeroU32;

const INLINE_KEY_LEN: usize = 54;

pub(crate) enum NodeKey {
    Inline {
        len: u8,
        bytes: [u8; INLINE_KEY_LEN],
    },
    Heap(Box<[u8]>),
}

impl Default for NodeKey {
    fn default() -> NodeKey {
        NodeKey::Inline {
            len: 0,
            bytes: [0; INLINE_KEY_LEN],
        }
    }
}

impl NodeKey {
    fn new(key: &[u8]) -> NodeKey {
        let mut bytes = [0; INLINE_KEY_LEN];
        match bytes.get_mut(..key.len()) {
            Some(head) => {
                head.copy_from_slice(key);
                NodeKey::Inline {
                    len: key.len() as u8,
                    bytes,
                }
            }
            None => NodeKey::Heap(key.into()),
        }
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        match self {
            NodeKey::Inline { len, bytes } => bytes.get(..usize::from(*len)).unwrap_or_default(),
            NodeKey::Heap(bytes) => bytes,
        }
    }

    fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }
}

#[derive(Default)]
struct Node {
    older: Option<NodeId>,
    newer: Option<NodeId>,
    key: NodeKey,
}

const _: () = assert!(size_of::<Node>() == 64);

#[derive(Default)]
pub(super) struct LargeQueue {
    nodes: Vec<Node>,
    free: Vec<NodeId>,
    oldest: Option<NodeId>,
    newest: Option<NodeId>,
    bytes: usize,
}

impl LargeQueue {
    fn push(&mut self, key: &[u8]) -> Option<NodeId> {
        let node = Node {
            older: self.newest,
            newer: None,
            key: NodeKey::new(key),
        };
        let id = match self.free.pop() {
            Some(id) => {
                *self.node_mut(Some(id))? = node;
                id
            }
            None => {
                self.nodes.push(node);
                NodeId::new(u32::try_from(self.nodes.len()).ok()?)?
            }
        };
        match self.node_mut(self.newest) {
            Some(newest) => newest.newer = Some(id),
            None => self.oldest = Some(id),
        }
        self.newest = Some(id);
        Some(id)
    }

    fn remove(&mut self, id: NodeId) -> NodeKey {
        let Some(node) = self.node_mut(Some(id)) else {
            return NodeKey::default();
        };
        let Node { older, newer, key } = mem::take(node);
        match self.node_mut(older) {
            Some(entry) => entry.newer = newer,
            None => self.oldest = newer,
        }
        match self.node_mut(newer) {
            Some(entry) => entry.older = older,
            None => self.newest = older,
        }
        self.free.push(id);
        key
    }

    fn node_mut(&mut self, id: Option<NodeId>) -> Option<&mut Node> {
        self.nodes.get_mut(id?.get() as usize - 1)
    }

    pub(super) fn memory(&self) -> usize {
        self.nodes.capacity() * size_of::<Node>() + self.free.capacity() * size_of::<NodeId>()
    }
}

impl Values {
    pub(super) fn insert_large(&mut self, key: &[u8], value: &[u8]) -> u32 {
        let Some(place) = self.arena.store(value) else {
            return self.insert(value);
        };
        let node = self.queue.push(key);
        self.queue.bytes += self.arena.slot_bytes(place);
        self.occupy(Blob::Arena(ArenaValue {
            place,
            len: value.len() as u32,
            node,
        }))
    }

    #[cold]
    #[inline(never)]
    pub(super) fn remove_large(&mut self, place: Place, node: NodeId) {
        self.queue.remove(node);
        self.queue.bytes -= self.arena.slot_bytes(place);
        self.arena.free(place);
    }

    pub(crate) fn take_oldest_key(&mut self) -> Option<NodeKey> {
        let oldest = self.queue.oldest;
        Some(mem::take(&mut self.queue.node_mut(oldest)?.key))
    }

    pub fn drop_orphan(&mut self) -> bool {
        let Some(id) = self.queue.oldest else {
            return false;
        };
        if self
            .queue
            .node_mut(Some(id))
            .is_none_or(|node| !node.key.is_empty())
        {
            return false;
        }
        self.queue.remove(id);
        true
    }

    pub fn large_bytes(&self) -> usize {
        self.queue.bytes
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../../keep-tests/unit/keep-engine/value/large.rs"]
mod tests;
