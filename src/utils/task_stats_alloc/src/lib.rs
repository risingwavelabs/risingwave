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
#![feature(atomic_mut_ptr)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::future::Future;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::task_local;

/// If you change the code in this struct, pls re-run the `tests/loom.rs` test locally.
#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct TaskLocalBytesAllocated(Option<NonNull<AtomicUsize>>);

impl Default for TaskLocalBytesAllocated {
    fn default() -> Self {
        Self(Some(
            NonNull::new(Box::into_raw(Box::new_in(0.into(), System))).unwrap(),
        ))
    }
}

// Need this otherwise the NonNull is not Send and can not be used in future.
unsafe impl Send for TaskLocalBytesAllocated {}

impl TaskLocalBytesAllocated {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an invalid counter.
    pub const fn invalid() -> Self {
        Self(None)
    }

    /// Adds to the current counter.
    #[inline(always)]
    pub fn add(&self, val: usize) {
        if let Some(bytes) = self.0 {
            let bytes_ref = unsafe { bytes.as_ref() };
            bytes_ref.fetch_add(val, Ordering::Relaxed);
        }
    }

    /// Adds to the current counter without validity check.
    ///
    /// # Safety
    /// The caller must ensure that `self` is valid.
    #[inline(always)]
    unsafe fn add_unchecked(&self, val: usize) {
        let bytes = self.0.unwrap_unchecked();
        let bytes_ref = unsafe { bytes.as_ref() };
        bytes_ref.fetch_add(val, Ordering::Relaxed);
    }

    /// Subtracts from the counter value, and `drop` the counter while the count reaches zero.
    #[inline(always)]
    pub fn sub(&self, val: usize) -> bool {
        if let Some(bytes) = self.0 {
            // Use `Relaxed` order as we don't need to sync read/write with other memory addresses.
            // Accesses to the counter itself are serialized by atomic operations.
            let bytes_ref = unsafe { bytes.as_ref() };
            let old_bytes = bytes_ref.fetch_sub(val, Ordering::Relaxed);
            // If the counter reaches zero, delete the counter. Note that we've ensured there's no
            // zero deltas in `wrap_layout`, so there'll be no more uses of the counter.
            if old_bytes == val {
                // No fence here, this is different from ref counter impl in https://www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html#boost_atomic.usage_examples.example_reference_counters.
                // As here, T is the exactly Counter and they have same memory address, so there
                // should not happen out-of-order commit.
                unsafe { Box::from_raw_in(bytes.as_ptr(), System) };
                return true;
            }
        }
        false
    }

    #[inline(always)]
    pub fn val(&self) -> usize {
        let bytes_ref = self.0.as_ref().expect("bytes is invalid");
        let bytes_ref = unsafe { bytes_ref.as_ref() };
        bytes_ref.load(Ordering::Relaxed)
    }
}

task_local! {
    pub static BYTES_ALLOCATED: TaskLocalBytesAllocated;
}

pub async fn allocation_stat<Fut, T, F>(future: Fut, interval: Duration, mut report: F) -> T
where
    Fut: Future<Output = T>,
    F: FnMut(usize),
{
    BYTES_ALLOCATED
        .scope(TaskLocalBytesAllocated::new(), async move {
            // The guard has the same lifetime as the counter so that the counter will keep positive
            // in the whole scope. When the scope exits, the guard is released, so the counter can
            // reach zero eventually and then `drop` itself.
            let _guard = Box::new(114514);
            let monitor = async move {
                let mut interval = tokio::time::interval(interval);
                loop {
                    interval.tick().await;
                    BYTES_ALLOCATED.with(|bytes| report(bytes.val()));
                }
            };
            let output = tokio::select! {
                biased;
                _ = monitor => unreachable!(),
                output = future => output,
            };
            output
        })
        .await
}

#[inline(always)]
fn wrap_layout(layout: Layout) -> (Layout, usize) {
    debug_assert_ne!(layout.size(), 0, "the size of layout must be non-zero");

    let (wrapped_layout, offset) = Layout::new::<TaskLocalBytesAllocated>()
        .extend(layout)
        .expect("wrapping layout overflow");
    let wrapped_layout = wrapped_layout.pad_to_align();

    (wrapped_layout, offset)
}

pub struct TaskLocalAlloc<A>(pub A);

unsafe impl<A> GlobalAlloc for TaskLocalAlloc<A>
where
    A: GlobalAlloc,
{
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let (wrapped_layout, offset) = wrap_layout(layout);

        BYTES_ALLOCATED
            .try_with(|&bytes| {
                bytes.add_unchecked(layout.size());
                let ptr = self.0.alloc(wrapped_layout);
                *ptr.cast() = bytes;
                ptr.wrapping_add(offset)
            })
            .unwrap_or_else(|_| {
                let ptr = self.0.alloc(wrapped_layout);
                *ptr.cast() = TaskLocalBytesAllocated::invalid();
                ptr.wrapping_add(offset)
            })
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let (wrapped_layout, offset) = wrap_layout(layout);
        let ptr = ptr.wrapping_sub(offset);

        let bytes: TaskLocalBytesAllocated = *ptr.cast();
        bytes.sub(layout.size());

        self.0.dealloc(ptr, wrapped_layout);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let (wrapped_layout, offset) = wrap_layout(layout);

        BYTES_ALLOCATED
            .try_with(|&bytes| {
                bytes.add_unchecked(layout.size());
                let ptr = self.0.alloc_zeroed(wrapped_layout);
                *ptr.cast() = bytes;
                ptr.wrapping_add(offset)
            })
            .unwrap_or_else(|_| {
                let ptr = self.0.alloc_zeroed(wrapped_layout);
                *ptr.cast() = TaskLocalBytesAllocated::invalid();
                ptr.wrapping_add(offset)
            })
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let (wrapped_layout, offset) = wrap_layout(layout);
        // SAFETY: the caller must ensure that the `new_size` does not overflow.
        let (new_wrapped_layout, new_offset) =
            wrap_layout(Layout::from_size_align_unchecked(new_size, layout.align()));
        let new_wrapped_size = new_wrapped_layout.size();

        let ptr = ptr.wrapping_sub(offset);

        let bytes: TaskLocalBytesAllocated = *ptr.cast();
        bytes.sub(layout.size());
        bytes.add(new_size);

        let ptr = self.0.realloc(ptr, wrapped_layout, new_wrapped_size);
        if ptr.is_null() {
            ptr
        } else {
            *ptr.cast() = bytes;
            ptr.wrapping_add(new_offset)
        }
    }
}
