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
        self.ctx.add(layout.size() as i64);
        Ok(ret)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        self.alloc.deallocate(ptr, layout);
        self.ctx.add(-(layout.size() as i64))
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
