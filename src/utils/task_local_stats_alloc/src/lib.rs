// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(allocator_api)]
#![feature(lint_reasons)]

use std::alloc::{Allocator, Global};
use std::future::Future;
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;

use atomic::Ordering;

#[derive(Clone, Default)]
pub struct StatsAlloc;

impl StatsAlloc {
    pub fn new() -> Self {
        Self {}
    }

    pub fn bytes_in_use(&self) -> usize {
        TASK_LOCAL_ALLOCATED_BYTES
            .try_with(|bytes| bytes.load(Ordering::Relaxed))
            .expect("TASK_LOCAL_ALLOCATED_BYTES not found")
    }
}

unsafe impl Allocator for StatsAlloc {
    #[inline(always)]
    fn allocate(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        TASK_LOCAL_ALLOCATED_BYTES
            .try_with(|bytes| bytes.fetch_add(layout.size(), Ordering::Relaxed))
            .expect("TASK_LOCAL_ALLOCATED_BYTES not found");
        Global.allocate(layout)
    }

    #[inline(always)]
    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        TASK_LOCAL_ALLOCATED_BYTES
            .try_with(|bytes| bytes.fetch_sub(layout.size(), Ordering::Relaxed))
            .expect("TASK_LOCAL_ALLOCATED_BYTES not found");
        Global.deallocate(ptr, layout)
    }

    #[inline(always)]
    fn allocate_zeroed(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        TASK_LOCAL_ALLOCATED_BYTES
            .try_with(|bytes| bytes.fetch_add(layout.size(), Ordering::Relaxed))
            .expect("TASK_LOCAL_ALLOCATED_BYTES not found");
        Global.allocate_zeroed(layout)
    }

    #[inline(always)]
    unsafe fn grow(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        TASK_LOCAL_ALLOCATED_BYTES
            .try_with(|bytes| {
                bytes.fetch_sub(old_layout.size(), Ordering::Relaxed);
                bytes.fetch_add(new_layout.size(), Ordering::Relaxed)
            })
            .expect("TASK_LOCAL_ALLOCATED_BYTES not found");
        Global.grow(ptr, old_layout, new_layout)
    }

    #[inline(always)]
    unsafe fn grow_zeroed(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        TASK_LOCAL_ALLOCATED_BYTES
            .try_with(|bytes| {
                bytes.fetch_sub(old_layout.size(), Ordering::Relaxed);
                bytes.fetch_add(new_layout.size(), Ordering::Relaxed)
            })
            .expect("TASK_LOCAL_ALLOCATED_BYTES not found");
        Global.grow_zeroed(ptr, old_layout, new_layout)
    }

    #[inline(always)]
    unsafe fn shrink(
        &self,
        ptr: std::ptr::NonNull<u8>,
        old_layout: std::alloc::Layout,
        new_layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        TASK_LOCAL_ALLOCATED_BYTES
            .try_with(|bytes| {
                bytes.fetch_sub(old_layout.size(), Ordering::Relaxed);
                bytes.fetch_add(new_layout.size(), Ordering::Relaxed)
            })
            .expect("TASK_LOCAL_ALLOCATED_BYTES not found");
        Global.shrink(ptr, old_layout, new_layout)
    }
}

tokio::task_local! {
    static TASK_LOCAL_ALLOCATED_BYTES: AtomicUsize;
}

/// Provides the given size in the task local storage for the scope of the given future.
pub async fn scope<F>(size: AtomicUsize, f: F) -> F::Output
where
    F: Future,
{
    TASK_LOCAL_ALLOCATED_BYTES.scope(size, f).await
}

/// Retrieve the allocated bytes from the task local storage.
pub fn allocated_bytes() -> usize {
    TASK_LOCAL_ALLOCATED_BYTES
        .try_with(|bytes| bytes.load(Ordering::Relaxed))
        .expect("TASK_LOCAL_ALLOCATED_BYTES not found")
}
