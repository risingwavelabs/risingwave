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

#![feature(allocator_api)]

use std::alloc::Allocator;
use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic, Arc};

pub struct StatsAlloc<T> {
    bytes_in_use: AtomicUsize,

    inner: T,
}

impl<T> StatsAlloc<T> {
    pub fn new(inner: T) -> Self {
        Self {
            bytes_in_use: AtomicUsize::new(0),
            inner,
        }
    }

    pub fn bytes_in_use(&self) -> usize {
        self.bytes_in_use.load(atomic::Ordering::Relaxed)
    }

    pub fn shared(self) -> SharedStatsAlloc<T> {
        SharedStatsAlloc(Arc::new(self))
    }
}

unsafe impl<T> Allocator for StatsAlloc<T>
where
    T: Allocator,
{
    #[inline(always)]
    fn allocate(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        self.bytes_in_use
            .fetch_add(layout.size(), atomic::Ordering::Relaxed);
        self.inner.allocate(layout)
    }

    #[inline(always)]
    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        self.bytes_in_use
            .fetch_sub(layout.size(), atomic::Ordering::Relaxed);
        self.inner.deallocate(ptr, layout)
    }

    #[inline(always)]
    fn allocate_zeroed(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        self.bytes_in_use
            .fetch_add(layout.size(), atomic::Ordering::Relaxed);
        self.inner.allocate_zeroed(layout)
    }

    #[inline(always)]
    unsafe fn grow(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        self.bytes_in_use
            .fetch_add(new_layout.size(), atomic::Ordering::Relaxed);
        self.bytes_in_use
            .fetch_sub(old_layout.size(), atomic::Ordering::Relaxed);
        self.inner.grow(ptr, old_layout, new_layout)
    }

    #[inline(always)]
    unsafe fn grow_zeroed(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        self.bytes_in_use
            .fetch_add(new_layout.size(), atomic::Ordering::Relaxed);
        self.bytes_in_use
            .fetch_sub(old_layout.size(), atomic::Ordering::Relaxed);
        self.inner.grow_zeroed(ptr, old_layout, new_layout)
    }

    #[inline(always)]
    unsafe fn shrink(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        self.bytes_in_use
            .fetch_add(new_layout.size(), atomic::Ordering::Relaxed);
        self.bytes_in_use
            .fetch_sub(old_layout.size(), atomic::Ordering::Relaxed);
        self.inner.shrink(ptr, old_layout, new_layout)
    }
}

pub struct SharedStatsAlloc<T>(Arc<StatsAlloc<T>>);

impl<T> Clone for SharedStatsAlloc<T> {
    fn clone(&self) -> Self {
        SharedStatsAlloc(self.0.clone())
    }
}

impl<T> Deref for SharedStatsAlloc<T> {
    type Target = StatsAlloc<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe impl<T: Allocator> Allocator for SharedStatsAlloc<T> {
    fn allocate(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        self.0.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        self.0.deallocate(ptr, layout)
    }

    fn allocate_zeroed(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        self.0.allocate_zeroed(layout)
    }

    unsafe fn grow(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        self.0.grow(ptr, old_layout, new_layout)
    }

    unsafe fn grow_zeroed(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        self.0.grow_zeroed(ptr, old_layout, new_layout)
    }

    unsafe fn shrink(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        self.0.shrink(ptr, old_layout, new_layout)
    }
}
