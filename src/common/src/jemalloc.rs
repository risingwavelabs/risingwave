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

/// If <https://github.com/tikv/jemallocator/issues/22> is resolved, we may inline this
#[macro_export]
macro_rules! enable_jemalloc_on_unix {
    () => {
        #[cfg(unix)]
        #[global_allocator]
        static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
    };
}

#[macro_export]
macro_rules! enable_task_local_jemalloc_on_unix {
    () => {
        #[cfg(unix)]
        #[global_allocator]
        static GLOBAL: task_stats_alloc::TaskLocalAlloc<tikv_jemallocator::Jemalloc> =
            task_stats_alloc::TaskLocalAlloc(tikv_jemallocator::Jemalloc);
    };
}
