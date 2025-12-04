// Copyright 2025 RisingWave Labs
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

/// If <https://github.com/tikv/jemallocator/issues/22> is resolved, we may inline this
#[macro_export]
macro_rules! enable_jemalloc {
    () => {
        static JEMALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

        #[cfg(unix)]
        #[global_allocator]
        static DEBUG_JEMALLOC: DebugJemallocAllocator = DebugJemallocAllocator;

        pub struct DebugJemallocAllocator;

        unsafe impl std::alloc::GlobalAlloc for DebugJemallocAllocator {
            unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
                if layout.size() >= 2 * 1024 * 1024 * 1024 {
                    let backtrace = std::backtrace::Backtrace::capture();
                    tracing::info!(
                        backtrace = format!("{backtrace}"),
                        size = layout.size(),
                        "allocating a large memory block",
                    );
                }

                unsafe { JEMALLOC.alloc(layout) }
            }

            unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
                unsafe { JEMALLOC.dealloc(ptr, layout) }
            }

            unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
                unsafe { JEMALLOC.alloc_zeroed(layout) }
            }

            unsafe fn realloc(
                &self,
                ptr: *mut u8,
                layout: std::alloc::Layout,
                new_size: usize,
            ) -> *mut u8 {
                unsafe { JEMALLOC.realloc(ptr, layout, new_size) }
            }
        }
    };
}
