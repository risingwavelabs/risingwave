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

use std::alloc::{Allocator, Global};
use std::cell::UnsafeCell;

use tokio::task_local;

pub struct LocalStatsAlloc<T> {
    bytes_in_use: UnsafeCell<usize>,

    inner: T,
}

impl<T> LocalStatsAlloc<T> {
    pub fn new(inner: T) -> Self {
        Self {
            bytes_in_use: UnsafeCell::new(0),
            inner,
        }
    }

    pub fn bytes_in_use(&self) -> usize {
        unsafe { *self.bytes_in_use.get() }
    }
}

unsafe impl<T> Allocator for LocalStatsAlloc<T>
where
    T: Allocator,
{
    #[inline(always)]
    fn allocate(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        unsafe {
            *self.bytes_in_use.get() += layout.size();
        }
        self.inner.allocate(layout)
    }

    #[inline(always)]
    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        *self.bytes_in_use.get() = (*self.bytes_in_use.get()).saturating_sub(layout.size());
        self.inner.deallocate(ptr, layout)
    }

    #[inline(always)]
    fn allocate_zeroed(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        unsafe {
            *self.bytes_in_use.get() += layout.size();
        }
        self.inner.allocate_zeroed(layout)
    }

    #[inline(always)]
    unsafe fn grow(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        *self.bytes_in_use.get() += new_layout.size();
        *self.bytes_in_use.get() = (*self.bytes_in_use.get()).saturating_sub(old_layout.size());
        self.inner.grow(ptr, old_layout, new_layout)
    }

    #[inline(always)]
    unsafe fn grow_zeroed(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        *self.bytes_in_use.get() += new_layout.size();
        *self.bytes_in_use.get() = (*self.bytes_in_use.get()).saturating_sub(old_layout.size());
        self.inner.grow_zeroed(ptr, old_layout, new_layout)
    }

    #[inline(always)]
    unsafe fn shrink(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        *self.bytes_in_use.get() += new_layout.size();
        *self.bytes_in_use.get() = (*self.bytes_in_use.get()).saturating_sub(old_layout.size());
        self.inner.shrink(ptr, old_layout, new_layout)
    }
}

task_local! {
    pub static TASK_LOCAL_ALLOC: LocalStatsAlloc<Global>;
}

#[derive(Clone, Copy)]
pub struct TaskLocalAllocator;

unsafe impl Allocator for TaskLocalAllocator {
    #[inline(always)]
    fn allocate(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        TASK_LOCAL_ALLOC
            .try_with(|alloc| alloc.allocate(layout))
            .unwrap_or_else(|_| Global.allocate(layout))
    }

    #[inline(always)]
    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        TASK_LOCAL_ALLOC
            .try_with(|alloc| alloc.deallocate(ptr, layout))
            .unwrap_or_else(|_| Global.deallocate(ptr, layout))
    }

    #[inline(always)]
    fn allocate_zeroed(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        TASK_LOCAL_ALLOC
            .try_with(|alloc| alloc.allocate_zeroed(layout))
            .unwrap_or_else(|_| Global.allocate_zeroed(layout))
    }

    #[inline(always)]
    unsafe fn grow(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        TASK_LOCAL_ALLOC
            .try_with(|alloc| alloc.grow(ptr, old_layout, new_layout))
            .unwrap_or_else(|_| Global.grow(ptr, old_layout, new_layout))
    }

    #[inline(always)]
    unsafe fn grow_zeroed(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        TASK_LOCAL_ALLOC
            .try_with(|alloc| alloc.grow_zeroed(ptr, old_layout, new_layout))
            .unwrap_or_else(|_| Global.grow_zeroed(ptr, old_layout, new_layout))
    }

    #[inline(always)]
    unsafe fn shrink(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        TASK_LOCAL_ALLOC
            .try_with(|alloc| alloc.shrink(ptr, old_layout, new_layout))
            .unwrap_or_else(|_| Global.shrink(ptr, old_layout, new_layout))
    }
}
