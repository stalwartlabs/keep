/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::io;
use std::ptr::{NonNull, null_mut};
use std::slice;

pub const HUGE_PAGE: usize = 2 << 20;
pub const PAGE: usize = 4096;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Pages {
    Base,
    Huge,
}

pub struct Region {
    ptr: NonNull<u8>,
    len: usize,
}

unsafe impl Send for Region {}

impl Region {
    pub fn map(len: usize, pages: Pages) -> io::Result<Region> {
        let len = len.max(1).div_ceil(HUGE_PAGE) * HUGE_PAGE;
        let span = len + HUGE_PAGE;
        let raw = unsafe {
            libc::mmap(
                null_mut(),
                span,
                libc::PROT_READ | libc::PROT_WRITE,
                MAP_FLAGS,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let start = raw as usize;
        let aligned = start.next_multiple_of(HUGE_PAGE);
        let head = aligned - start;
        let tail = span - head - len;
        unsafe {
            if head > 0 {
                libc::munmap(raw, head);
            }
            if tail > 0 {
                libc::munmap((aligned + len) as *mut libc::c_void, tail);
            }
        }
        let ptr = NonNull::new(aligned as *mut u8).ok_or_else(io::Error::last_os_error)?;
        let region = Region { ptr, len };
        region.advise(0, len, pages.advice());
        Ok(region)
    }

    #[inline]
    pub fn bytes(&self, offset: usize, len: usize) -> &[u8] {
        match offset.checked_add(len) {
            Some(end) if end <= self.len => unsafe {
                slice::from_raw_parts(self.ptr.as_ptr().add(offset), len)
            },
            _ => &[],
        }
    }

    #[inline]
    pub fn bytes_mut(&mut self, offset: usize, len: usize) -> &mut [u8] {
        match offset.checked_add(len) {
            Some(end) if end <= self.len => unsafe {
                slice::from_raw_parts_mut(self.ptr.as_ptr().add(offset), len)
            },
            _ => &mut [],
        }
    }

    pub fn populate(&mut self, offset: usize, len: usize) {
        if !self.advise(offset, len, Advice::Populate) {
            for page in self.bytes_mut(offset, len).iter_mut().step_by(PAGE) {
                *page = 0;
            }
        }
    }

    pub fn whole_pages(offset: usize, len: usize) -> (usize, usize) {
        let start = offset.next_multiple_of(PAGE);
        let end = offset.saturating_add(len) / PAGE * PAGE;
        (start, end.saturating_sub(start))
    }

    pub fn release(&mut self, offset: usize, len: usize) -> usize {
        let (start, len) = Region::whole_pages(offset, len);
        if len > 0 && self.advise(start, len, Advice::Release) {
            len
        } else {
            0
        }
    }

    fn advise(&self, offset: usize, len: usize, advice: Advice) -> bool {
        let Some(code) = advice.code() else {
            return false;
        };
        if offset.checked_add(len).is_none_or(|end| end > self.len) {
            return false;
        }
        let at = unsafe { self.ptr.as_ptr().add(offset) };
        unsafe { libc::madvise(at.cast(), len, code) == 0 }
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.len);
        }
    }
}

impl Pages {
    pub fn size(self) -> usize {
        match self {
            Pages::Base => PAGE,
            Pages::Huge => HUGE_PAGE,
        }
    }

    fn advice(self) -> Advice {
        match self {
            Pages::Base => Advice::Base,
            Pages::Huge => Advice::Huge,
        }
    }
}

#[derive(Clone, Copy)]
enum Advice {
    Base,
    Huge,
    Populate,
    Release,
}

#[cfg(target_os = "linux")]
const MAP_FLAGS: libc::c_int = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE;

#[cfg(not(target_os = "linux"))]
const MAP_FLAGS: libc::c_int = libc::MAP_PRIVATE | libc::MAP_ANON;

impl Advice {
    #[cfg(target_os = "linux")]
    fn code(self) -> Option<libc::c_int> {
        Some(match self {
            Advice::Base => libc::MADV_NOHUGEPAGE,
            Advice::Huge => libc::MADV_HUGEPAGE,
            Advice::Populate => libc::MADV_POPULATE_WRITE,
            Advice::Release => libc::MADV_DONTNEED,
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn code(self) -> Option<libc::c_int> {
        match self {
            Advice::Base | Advice::Huge | Advice::Populate => None,
            Advice::Release => Some(libc::MADV_FREE),
        }
    }
}

#[cfg(all(test, keep_tests))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-engine/region.rs"]
mod tests;

#[cfg(all(test, keep_tests, target_os = "linux"))]
#[rustfmt::skip]
#[path = "../../../../keep-tests/unit/keep-engine/region/test_support.rs"]
mod test_support;
