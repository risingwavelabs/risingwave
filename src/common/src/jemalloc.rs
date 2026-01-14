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

        const DEFAULT_BACKTRACE_THRESHOLD: usize = 1 * 1024 * 1024 * 1024;

        thread_local! {
            static IN_BACKTRACE: std::cell::Cell<bool> = std::cell::Cell::new(false);
        }

        static BACKTRACE_THRESHOLD: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

        struct BacktraceGuard(bool);

        impl BacktraceGuard {
            fn enter() -> Option<Self> {
                let entered = IN_BACKTRACE.with(|flag| {
                    if flag.get() {
                        false
                    } else {
                        flag.set(true);
                        true
                    }
                });

                if entered { Some(Self(true)) } else { None }
            }
        }

        impl Drop for BacktraceGuard {
            fn drop(&mut self) {
                if self.0 {
                    IN_BACKTRACE.with(|flag| flag.set(false));
                }
            }
        }

        fn backtrace_threshold() -> usize {
            *BACKTRACE_THRESHOLD.get_or_init(|| {
                match std::env::var("RW_JEMALLOC_BACKTRACE_THRESHOLD") {
                    Ok(value) => value.parse().unwrap_or(DEFAULT_BACKTRACE_THRESHOLD),
                    Err(_) => DEFAULT_BACKTRACE_THRESHOLD,
                }
            })
        }

        fn log_large_alloc(size: usize, new_size: Option<usize>) {
            if size < backtrace_threshold() {
                return;
            }

            let _guard = match BacktraceGuard::enter() {
                Some(g) => g,
                None => return,
            };

            let backtrace = std::backtrace::Backtrace::force_capture();
            match new_size {
                Some(new_size) => {
                    eprintln!(
                        "jemalloc realloc over threshold: size={}, new_size={}\n{backtrace}",
                        size, new_size
                    );
                }
                None => {
                    eprintln!("jemalloc alloc over threshold: size={}\n{backtrace}", size);
                }
            }
        }

        #[cfg(unix)]
        #[global_allocator]
        static DEBUG_JEMALLOC: DebugJemallocAllocator = DebugJemallocAllocator;

        pub struct DebugJemallocAllocator;

        unsafe impl std::alloc::GlobalAlloc for DebugJemallocAllocator {
            unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
                log_large_alloc(layout.size(), None);
                unsafe { JEMALLOC.alloc(layout) }
            }

            unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
                unsafe { JEMALLOC.dealloc(ptr, layout) }
            }

            unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
                log_large_alloc(layout.size(), None);
                unsafe { JEMALLOC.alloc_zeroed(layout) }
            }

            unsafe fn realloc(
                &self,
                ptr: *mut u8,
                layout: std::alloc::Layout,
                new_size: usize,
            ) -> *mut u8 {
                log_large_alloc(layout.size(), Some(new_size));
                unsafe { JEMALLOC.realloc(ptr, layout, new_size) }
            }
        }
    };
}
