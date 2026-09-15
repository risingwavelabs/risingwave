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

use std::alloc::{AllocError, Allocator, Global, Layout};
use std::ptr::NonNull;

use allocator_api2::alloc::{AllocError as AllocErrorApi2, Allocator as AllocatorApi2};

use crate::memory::MemoryContext;

pub type MonitoredGlobalAlloc = MonitoredAlloc<Global>;

pub struct MonitoredAlloc<A: Allocator> {
    ctx: MemoryContext,
    alloc: A,
}

impl<A: Allocator> MonitoredAlloc<A> {
    pub fn new(ctx: MemoryContext, alloc: A) -> Self {
        Self { ctx, alloc }
    }
}

impl MonitoredGlobalAlloc {
    pub fn with_memory_context(ctx: MemoryContext) -> Self {
        Self { ctx, alloc: Global }
    }

    pub fn for_test() -> Self {
        Self::with_memory_context(MemoryContext::none())
    }
}

unsafe impl<A: Allocator> Allocator for MonitoredAlloc<A> {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let ret = self.alloc.allocate(layout)?;
        // Ordinary collection operations may abort on AllocError. Do not turn a budget overrun
        // into allocation failure, but always record the successful allocation for a matching
        // deallocation. Higher-level callers decide whether to spill or fail the query.
        self.ctx.add_unchecked(layout.size() as i64);
        Ok(ret)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            self.alloc.deallocate(ptr, layout);
            self.ctx.add_unchecked(-(layout.size() as i64));
        }
    }
}

unsafe impl<A: Allocator> AllocatorApi2 for MonitoredAlloc<A> {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocErrorApi2> {
        let ret = self.alloc.allocate(layout).map_err(|_| AllocErrorApi2)?;
        // As in the std allocator path, record every successful allocation even over budget.
        self.ctx.add_unchecked(layout.size() as i64);
        Ok(ret)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            self.alloc.deallocate(ptr, layout);
            self.ctx.add_unchecked(-(layout.size() as i64));
        }
    }
}

impl<A: Allocator + Clone> Clone for MonitoredAlloc<A> {
    fn clone(&self) -> Self {
        Self {
            ctx: self.ctx.clone(),
            alloc: self.alloc.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use prometheus::core::Atomic;

    use super::*;
    use crate::metrics::TrAdderAtomic;

    fn allocate<A: Allocator>(
        alloc: &MonitoredAlloc<A>,
        layout: Layout,
        use_api2: bool,
    ) -> Result<NonNull<[u8]>, AllocError> {
        if use_api2 {
            AllocatorApi2::allocate(alloc, layout).map_err(|_| AllocError)
        } else {
            Allocator::allocate(alloc, layout)
        }
    }

    struct FailingAllocator;

    // SAFETY: No allocation succeeds, so there is no valid pointer to deallocate.
    unsafe impl Allocator for FailingAllocator {
        fn allocate(&self, _: Layout) -> Result<NonNull<[u8]>, AllocError> {
            Err(AllocError)
        }

        unsafe fn deallocate(&self, _: NonNull<u8>, _: Layout) {
            panic!("the failing allocator cannot have a live allocation");
        }
    }

    fn check_allocator_accounting(use_api2: bool) {
        let parent = MemoryContext::root(TrAdderAtomic::new(0), 48);
        let child =
            MemoryContext::new_with_mem_limit(Some(parent.clone()), TrAdderAtomic::new(0), 32);
        // Model another owner whose charge must not be erased by this allocator's releases.
        assert!(parent.add(16));
        let alloc = MonitoredGlobalAlloc::with_memory_context(child.clone());
        let first_layout = Layout::from_size_align(64, 8).unwrap();
        let second_layout = Layout::from_size_align(32, 8).unwrap();
        let first = allocate(&alloc, first_layout, use_api2).unwrap();
        assert_eq!(child.get_bytes_used(), 64);
        assert_eq!(parent.get_bytes_used(), 80);
        assert!(!child.check_memory_usage());
        assert!(!parent.check_memory_usage());
        assert!(!child.add(1));
        assert_eq!(child.get_bytes_used(), 64);
        assert_eq!(parent.get_bytes_used(), 80);

        let second = allocate(&alloc, second_layout, use_api2).unwrap();
        assert_eq!(child.get_bytes_used(), 96);
        assert_eq!(parent.get_bytes_used(), 112);
        // SAFETY: These are live allocations from this allocator, using their original layouts.
        unsafe {
            if use_api2 {
                AllocatorApi2::deallocate(&alloc, second.cast(), second_layout);
            } else {
                Allocator::deallocate(&alloc, second.cast(), second_layout);
            }
        }
        // Both contexts are still over budget; the release must nevertheless be recorded.
        assert_eq!(child.get_bytes_used(), 64);
        assert_eq!(parent.get_bytes_used(), 80);
        // SAFETY: The first allocation is still live and has not previously been deallocated.
        unsafe {
            if use_api2 {
                AllocatorApi2::deallocate(&alloc, first.cast(), first_layout);
            } else {
                Allocator::deallocate(&alloc, first.cast(), first_layout);
            }
        }
        assert_eq!(child.get_bytes_used(), 0);
        assert_eq!(parent.get_bytes_used(), 16);
        drop(alloc);
        drop(child);
        assert_eq!(parent.get_bytes_used(), 16);
        assert!(parent.add(-16));
        assert_eq!(parent.get_bytes_used(), 0);

        // An underlying allocation failure must not change either context's usage.
        let parent = MemoryContext::root(TrAdderAtomic::new(0), 100);
        let child = MemoryContext::new(Some(parent.clone()), TrAdderAtomic::new(0));
        assert!(parent.add(16));
        let failing_alloc = MonitoredAlloc::new(child.clone(), FailingAllocator);
        assert!(
            allocate(
                &failing_alloc,
                Layout::from_size_align(64, 8).unwrap(),
                use_api2
            )
            .is_err()
        );
        assert_eq!(child.get_bytes_used(), 0);
        assert_eq!(parent.get_bytes_used(), 16);
        drop(failing_alloc);
        drop(child);
        assert_eq!(parent.get_bytes_used(), 16);
        assert!(parent.add(-16));
    }

    /// Verifies that both allocator interfaces count successful allocations even above budget and
    /// subtract those bytes when freed, without changing other users' counts. Failed allocations
    /// must not change any count.
    #[test]
    fn test_allocator_accounting() {
        check_allocator_accounting(false);
        check_allocator_accounting(true);
    }
}
