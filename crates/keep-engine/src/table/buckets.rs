/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::iter;

pub struct Buckets<B>(Box<[B]>);

impl<B> Buckets<B> {
    pub fn new(len: usize, make: impl FnMut() -> B) -> Self {
        Buckets(iter::repeat_with(make).take(len).collect())
    }

    pub fn none() -> Self {
        Buckets(Box::default())
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn as_slice(&self) -> &[B] {
        &self.0
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [B] {
        &mut self.0
    }

    pub fn into_vec(self) -> Vec<B> {
        self.0.into_vec()
    }
}
