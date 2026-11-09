/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::sync::atomic::{AtomicUsize, Ordering};

use foldhash::{HashSet, HashSetExt};

use super::model::{Action, Pins, State};

pub struct Budget(AtomicUsize);

impl Budget {
    const CHUNK: usize = 4_096;

    pub fn new(states: usize) -> Budget {
        Budget(AtomicUsize::new(states))
    }

    fn draw(&self) -> usize {
        self.0
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                (left > 0).then(|| left.saturating_sub(Self::CHUNK))
            })
            .map_or(0, |left| left.min(Self::CHUNK))
    }

    fn give(&self, states: usize) {
        self.0.fetch_add(states, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Call {
    pub record: usize,
    pub call: u64,
    pub ret: u64,
    pub action: Action,
    pub required: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Verdict {
    Linearizable,
    Violated { record: usize, linearized: usize },
    Undecided { states: usize },
}

const NONE: u32 = u32::MAX;
const HEAD: u32 = 0;

#[derive(Clone, Copy)]
struct Link {
    prev: u32,
    next: u32,
    op: u32,
    returns: bool,
}

#[derive(Clone, Copy)]
struct Placed {
    call: u32,
    ret: u32,
}

struct Frame {
    op: u32,
    prev: State,
    position: usize,
}

pub struct Search<'a> {
    calls: &'a [Call],
    pins: Pins<'a>,
    links: Vec<Link>,
    placed: Vec<Placed>,
    keys: Vec<u128>,
    candidates: Vec<u32>,
    twins: HashSet<Action>,
}

impl<'a> Search<'a> {
    pub fn new(calls: &'a [Call], pins: Pins<'a>) -> Search<'a> {
        let mut events: Vec<(u64, bool, u32)> = Vec::with_capacity(2 * calls.len());
        for (op, call) in (0u32..).zip(calls) {
            events.push((call.call, false, op));
            if call.ret != u64::MAX {
                events.push((call.ret, true, op));
            }
        }
        events.sort_unstable();
        let mut links = Vec::with_capacity(events.len() + 2);
        links.push(Link {
            prev: NONE,
            next: 1,
            op: NONE,
            returns: false,
        });
        let mut placed = vec![
            Placed {
                call: NONE,
                ret: NONE
            };
            calls.len()
        ];
        for (index, &(_, returns, op)) in (1u32..).zip(&events) {
            links.push(Link {
                prev: index - 1,
                next: index + 1,
                op,
                returns,
            });
            if let Some(slot) = placed.get_mut(op as usize) {
                if returns {
                    slot.ret = index;
                } else {
                    slot.call = index;
                }
            }
        }
        let end = u32::try_from(links.len()).unwrap_or(NONE);
        links.push(Link {
            prev: end - 1,
            next: NONE,
            op: NONE,
            returns: true,
        });
        let keys = (0u64..calls.len() as u64).map(zobrist).collect();
        Search {
            calls,
            pins,
            links,
            placed,
            keys,
            candidates: Vec::new(),
            twins: HashSet::new(),
        }
    }

    pub fn run(self, start: State, budget: &Budget) -> Verdict {
        let mut held = 0;
        let verdict = self.explore(start, budget, &mut held);
        budget.give(held);
        verdict
    }

    fn explore(mut self, start: State, budget: &Budget, held: &mut usize) -> Verdict {
        let mut remaining = self.calls.iter().filter(|call| call.required).count();
        let mut state = start;
        let mut set_key = 0u128;
        let mut seen: HashSet<(u128, State)> = HashSet::default();
        let mut stack: Vec<Frame> = Vec::new();
        let mut deepest = (0usize, self.blocking());
        self.collect();
        let mut position = 0;
        loop {
            if remaining == 0 {
                return Verdict::Linearizable;
            }
            if seen.len() >= *held {
                match budget.draw() {
                    0 => return Verdict::Undecided { states: seen.len() },
                    drawn => *held += drawn,
                }
            }
            let mut moved = false;
            while let Some(&op) = self.candidates.get(position) {
                let call = self.call(op);
                if let Some(next) = call.action.step(state, self.pins) {
                    let key = set_key ^ self.key(op);
                    let dominated = !call.required && next == state;
                    if !dominated && seen.insert((key, next)) {
                        stack.push(Frame {
                            op,
                            prev: state,
                            position,
                        });
                        self.lift(op);
                        set_key = key;
                        state = next;
                        remaining -= usize::from(call.required);
                        if stack.len() > deepest.0 {
                            deepest = (stack.len(), self.blocking());
                        }
                        self.collect();
                        position = 0;
                        moved = true;
                        break;
                    }
                }
                position += 1;
            }
            if moved {
                continue;
            }
            let Some(frame) = stack.pop() else {
                return Verdict::Violated {
                    record: deepest.1,
                    linearized: deepest.0,
                };
            };
            self.unlift(frame.op);
            set_key ^= self.key(frame.op);
            state = frame.prev;
            remaining += usize::from(self.call(frame.op).required);
            self.collect();
            position = frame.position + 1;
        }
    }

    fn call(&self, op: u32) -> Call {
        self.calls[op as usize]
    }

    fn key(&self, op: u32) -> u128 {
        self.keys[op as usize]
    }

    fn collect(&mut self) {
        self.candidates.clear();
        let mut optional = 0;
        let mut at = self.links[HEAD as usize].next;
        while let Some(link) = self.links.get(at as usize) {
            if link.returns {
                break;
            }
            if self.calls[link.op as usize].required {
                self.candidates.push(link.op);
            } else {
                optional += 1;
            }
            at = link.next;
        }
        if optional == 0 {
            return;
        }
        self.twins.clear();
        let mut at = self.links[HEAD as usize].next;
        while let Some(link) = self.links.get(at as usize) {
            if link.returns {
                break;
            }
            let call = self.calls[link.op as usize];
            if !call.required && self.twins.insert(call.action) {
                self.candidates.push(link.op);
            }
            at = link.next;
        }
    }

    fn blocking(&self) -> usize {
        let mut at = self.links[HEAD as usize].next;
        let mut first_required = None;
        while let Some(link) = self.links.get(at as usize) {
            if link.op == NONE {
                break;
            }
            let call = self.calls[link.op as usize];
            if link.returns {
                return call.record;
            }
            if call.required && first_required.is_none() {
                first_required = Some(call.record);
            }
            at = link.next;
        }
        first_required.unwrap_or_default()
    }

    fn lift(&mut self, op: u32) {
        let Placed { call, ret } = self.placed[op as usize];
        self.remove(call);
        if ret != NONE {
            self.remove(ret);
        }
    }

    fn unlift(&mut self, op: u32) {
        let Placed { call, ret } = self.placed[op as usize];
        if ret != NONE {
            self.restore(ret);
        }
        self.restore(call);
    }

    fn remove(&mut self, at: u32) {
        let Link { prev, next, .. } = self.links[at as usize];
        self.links[prev as usize].next = next;
        self.links[next as usize].prev = prev;
    }

    fn restore(&mut self, at: u32) {
        let Link { prev, next, .. } = self.links[at as usize];
        self.links[prev as usize].next = at;
        self.links[next as usize].prev = at;
    }
}

fn zobrist(op: u64) -> u128 {
    (u128::from(mix(2 * op)) << 64) | u128::from(mix(2 * op + 1))
}

fn mix(index: u64) -> u64 {
    let mut z = index.wrapping_add(1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
