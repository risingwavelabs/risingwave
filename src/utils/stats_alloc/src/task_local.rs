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

// FIXME: This is a false-positive clippy test, remove this while bumping toolchain.
// https://github.com/tokio-rs/tokio/issues/4836
// https://github.com/rust-lang/rust-clippy/issues/8493
#![expect(clippy::declare_interior_mutable_const)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::future::Future;
use std::sync::atomic::{fence, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::task_local;

#[repr(transparent)]
#[derive(Clone, Copy, Debug)]
pub struct TaskLocalBytesAllocated(Option<&'static AtomicUsize>);

impl Default for TaskLocalBytesAllocated {
    fn default() -> Self {
        Self(Some(Box::leak(Box::new_in(0.into(), System))))
    }
}

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
    fn add(&self, val: usize) {
        if let Some(bytes) = self.0 {
            bytes.fetch_add(val, Ordering::Relaxed);
        }
    }

    /// Adds to the current counter without validity check.
    ///
    /// # Safety
    /// The caller must ensure that `self` is valid.
    #[inline(always)]
    unsafe fn add_unchecked(&self, val: usize) {
        self.0.unwrap_unchecked().fetch_add(val, Ordering::Relaxed);
    }

    /// Subtracts from the counter value, and `drop` the counter while the count reaches zero.
    #[inline(always)]
    fn sub(&self, val: usize) {
        if let Some(bytes) = self.0 {
            // Use Release to synchronize with the below deletion.
            let old_bytes = bytes.fetch_sub(val, Ordering::Release);
            // If the counter reaches zero, delete the counter. Note that we've ensured there's no
            // zero deltas in `wrap_layout`, so there'll be no more uses of the counter.
            if old_bytes == val {
                // This fence is needed to prevent reordering of use of the counter and deletion of
                // the counter. Because it is marked `Release`, the decreasing of the counter
                // synchronizes with this `Acquire` fence. This means that use of the counter
                // happens before decreasing the counter, which happens before this fence, which
                // happens before the deletion of the counter.
                fence(Ordering::Acquire);
                unsafe { Box::from_raw_in(bytes.as_mut_ptr(), System) };
            }
        }
    }

    #[inline(always)]
    pub fn val(&self) -> usize {
        self.0
            .as_ref()
            .expect("bytes is invalid")
            .load(Ordering::Relaxed)
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
        let ptr = ptr.wrapping_sub(offset);

        let bytes: TaskLocalBytesAllocated = *ptr.cast();
        bytes.add(new_size);
        bytes.sub(layout.size());

        let ptr = self.0.realloc(ptr, wrapped_layout, new_size + offset);
        if ptr.is_null() {
            ptr
        } else {
            *ptr.cast() = bytes;
            ptr.wrapping_add(offset)
        }
    }
}
