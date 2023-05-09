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

use std::alloc::{AllocError, Allocator, Layout};
use std::ptr::NonNull;

use crate::memory::MemoryContextRef;

struct MonitoredAlloc<A: Allocator> {
    ctx: MemoryContextRef,
    alloc: A,
}

impl<A: Allocator> MonitoredAlloc<A> {
    #[allow(dead_code)]
    pub fn new(ctx: MemoryContextRef, alloc: A) -> Self {
        Self { ctx, alloc }
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
