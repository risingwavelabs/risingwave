// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::RangeBounds;

#[inline(always)]
pub fn align_up(v: usize, align: usize) -> usize {
    (v + align - 1) & !(align - 1)
}

#[inline(always)]
pub fn align_down(v: usize, align: usize) -> usize {
    v & !(align - 1)
}

pub struct AlignedBuffer<const ALIGN: usize, const SMOOTH: usize, const DEFAULT: usize> {
    ptr: *mut u8,
    len: usize,
    capacity: usize,
}

impl<const ALIGN: usize, const SMOOTH: usize, const DEFAULT: usize>
    AlignedBuffer<ALIGN, SMOOTH, DEFAULT>
{
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = Self::align_v_up(capacity);
        let buffer = unsafe {
            std::alloc::alloc_zeroed(std::alloc::Layout::from_size_align_unchecked(
                capacity, ALIGN,
            ))
        };

        Self {
            ptr: buffer,
            len: 0,
            capacity,
        }
    }

    pub fn with_size(len: usize) -> Self {
        let mut ret = Self::with_capacity(len);
        ret.resize(len);
        ret
    }

    #[inline(always)]
    pub fn align(&self) -> usize {
        ALIGN
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn aligned_len(&self) -> usize {
        align_up(self.len, ALIGN)
    }

    #[inline(always)]
    pub fn alignments(&self) -> usize {
        self.aligned_len() / ALIGN
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn remains(&self) -> usize {
        self.capacity - self.len
    }

    pub fn write_at(&mut self, src: &[u8], offset: usize) {
        let len = src.len();
        if offset + len > self.capacity {
            self.grow_at(offset + len)
        }
        unsafe {
            let dst = std::slice::from_raw_parts_mut(self.ptr.add(offset), len);
            dst.copy_from_slice(src);
        }
        self.len = std::cmp::max(self.len, offset + len);
    }

    pub fn append(&mut self, src: &[u8]) {
        self.write_at(src, self.len)
    }

    pub fn reserve(&mut self, capacity: usize) {
        if capacity > self.capacity {
            self.grow_at(capacity);
        }
    }

    pub fn resize(&mut self, len: usize) {
        if len > self.capacity {
            self.grow_at(len);
        }
        self.len = len;
    }

    pub fn align_up(&mut self) {
        let len = Self::align_v_up(self.len);
        self.resize(len);
    }

    pub fn align_up_to(&mut self, align: usize) {
        let len = align_up(self.len, align);
        self.resize(len);
    }

    pub fn is_aligend(&self) -> bool {
        self.len % ALIGN == 0
    }

    pub fn is_aligend_to(&self, align: usize) -> bool {
        self.len % align == 0
    }

    pub fn slice(&self, range: impl RangeBounds<usize>) -> &[u8] {
        let (start, end) = self.bounds(range);
        let slice = unsafe { std::slice::from_raw_parts(self.ptr.add(start), end - start) };
        slice
    }

    pub fn slice_mut(&mut self, range: impl RangeBounds<usize>) -> &mut [u8] {
        let (start, end) = self.bounds(range);
        let slice = unsafe { std::slice::from_raw_parts_mut(self.ptr.add(start), end - start) };
        slice
    }
}

impl<const ALIGN: usize, const SMOOTH: usize, const DEFAULT: usize>
    AlignedBuffer<ALIGN, SMOOTH, DEFAULT>
{
    fn align_v_up(v: usize) -> usize {
        align_up(v, ALIGN)
    }

    #[allow(dead_code)]
    fn align_v_down(v: usize) -> usize {
        align_down(v, ALIGN)
    }

    fn bounds(&self, range: impl RangeBounds<usize>) -> (usize, usize) {
        let start = match range.start_bound() {
            std::ops::Bound::Included(start) => *start,
            std::ops::Bound::Excluded(start) => *start + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(end) => *end + 1,
            std::ops::Bound::Excluded(end) => *end,
            std::ops::Bound::Unbounded => self.len,
        };
        if end > self.len {
            panic!(
                "out of range: [capacity: {}] [len: {}] [given: {}]",
                self.capacity, self.len, end,
            );
        }
        (start, end)
    }

    fn grow_at(&mut self, size: usize) {
        let capacity = Self::grow_size_at(self.capacity, size);
        unsafe {
            let buffer = std::alloc::alloc_zeroed(std::alloc::Layout::from_size_align_unchecked(
                capacity, ALIGN,
            ));

            let src = std::slice::from_raw_parts(self.ptr, self.len);
            let dist = std::slice::from_raw_parts_mut(buffer, self.len);
            dist.copy_from_slice(src);

            std::alloc::dealloc(
                self.ptr,
                std::alloc::Layout::from_size_align_unchecked(self.capacity, ALIGN),
            );

            self.ptr = buffer;
        }
        self.capacity = capacity;
    }

    fn grow_size_at(origin: usize, size: usize) -> usize {
        let size = Self::align_v_up(size);
        let mut capacity = origin;
        while capacity < size {
            if capacity > SMOOTH {
                capacity += SMOOTH;
            } else if capacity > SMOOTH / 4 * 3 {
                capacity = SMOOTH * 2;
            } else if capacity > SMOOTH / 2 {
                capacity = SMOOTH;
            } else {
                capacity *= 2;
            }
        }
        capacity
    }
}

impl<const ALIGN: usize, const SMOOTH: usize, const DEFAULT: usize> std::fmt::Debug
    for AlignedBuffer<ALIGN, SMOOTH, DEFAULT>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = unsafe { std::slice::from_raw_parts(self.ptr, self.len) };
        f.debug_struct("AlignedBuffer")
            .field("ptr", &self.ptr)
            .field("align", &ALIGN)
            .field("capacity", &self.capacity)
            .field("len", &self.len)
            .field("data", &data)
            .finish()
    }
}

impl<const ALIGN: usize, const SMOOTH: usize, const DEFAULT: usize, R: RangeBounds<usize>>
    core::ops::Index<R> for AlignedBuffer<ALIGN, SMOOTH, DEFAULT>
{
    type Output = [u8];

    fn index(&self, index: R) -> &Self::Output {
        self.slice(index)
    }
}

impl<const ALIGN: usize, const SMOOTH: usize, const DEFAULT: usize, R: RangeBounds<usize>>
    core::ops::IndexMut<R> for AlignedBuffer<ALIGN, SMOOTH, DEFAULT>
{
    fn index_mut(&mut self, index: R) -> &mut Self::Output {
        self.slice_mut(index)
    }
}

impl<const ALIGN: usize, const SMOOTH: usize, const DEFAULT: usize> Default
    for AlignedBuffer<ALIGN, SMOOTH, DEFAULT>
{
    fn default() -> Self {
        Self::with_capacity(DEFAULT)
    }
}

impl<const ALIGN: usize, const SMOOTH: usize, const DEFAULT: usize> Drop
    for AlignedBuffer<ALIGN, SMOOTH, DEFAULT>
{
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(
                self.ptr,
                std::alloc::Layout::from_size_align_unchecked(self.capacity, ALIGN),
            )
        }
    }
}

unsafe impl<const ALIGN: usize, const SMOOTH: usize, const DEFAULT: usize> Send
    for AlignedBuffer<ALIGN, SMOOTH, DEFAULT>
{
}

unsafe impl<const ALIGN: usize, const SMOOTH: usize, const DEFAULT: usize> Sync
    for AlignedBuffer<ALIGN, SMOOTH, DEFAULT>
{
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_grow_capacity() {
        assert_eq!(AlignedBuffer::<1, 64, 0>::grow_size_at(128, 192), 192);
        assert_eq!(AlignedBuffer::<1, 64, 0>::grow_size_at(64, 128), 128);
        assert_eq!(AlignedBuffer::<1, 64, 0>::grow_size_at(49, 128), 128);
        assert_eq!(AlignedBuffer::<1, 64, 0>::grow_size_at(48, 64), 64);
        assert_eq!(AlignedBuffer::<1, 64, 0>::grow_size_at(32, 64), 64);
        assert_eq!(AlignedBuffer::<1, 64, 0>::grow_size_at(31, 62), 62);
    }

    #[test]
    fn test_aligned_buffer() {
        let mut buf = AlignedBuffer::<512, 1073741824 /* 1 GiB */, 0>::with_capacity(65500);

        assert_eq!(buf.capacity(), 65536);

        buf.append(&[b'x'; 1024]);
        assert_eq!(&buf[0..1024], &[b'x'; 1024]);

        buf.write_at(&[b'x'; 1024], 1024);
        assert_eq!(&buf[0..2048], &[b'x'; 2048]);

        buf.append(&[b'a'; 1024]);
        (&mut buf[2048..3072]).copy_from_slice(&[b'x'; 1024]);
        assert_eq!(&buf[0..3072], &[b'x'; 3072]);

        drop(buf);
    }

    #[test]
    fn test_growth() {
        let mut buf = AlignedBuffer::<8, 64, 0>::with_capacity(7);
        assert_eq!(buf.capacity(), 8);

        buf.append(&[b'x'; 7]);
        assert_eq!(buf.len(), 7);
        assert_eq!(buf.capacity(), 8);

        buf.append(&[b'x'; 8]);
        assert_eq!(buf.len(), 15);
        assert_eq!(buf.capacity(), 16);

        buf.write_at(&[b'x'; 1], 62);
        assert_eq!(buf.len(), 63);
        assert_eq!(buf.capacity(), 64);

        buf.write_at(&[b'x'; 1], 190);
        assert_eq!(buf.len(), 191);
        assert_eq!(buf.capacity(), 192);
    }
}
