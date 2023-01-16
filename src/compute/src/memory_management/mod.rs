// Copyright 2023 Singularity Data
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

#[cfg(target_os = "linux")]
mod memory_manager;

// FIXME: remove such limitation after #7180
/// Jemalloc is not supported on Windows, because of tikv-jemalloc's own reasons.
/// See the comments for the macro `enable_jemalloc_on_linux!()`
#[cfg(not(target_os = "linux"))]
mod fake_global_memory_manager;
#[cfg(not(target_os = "linux"))]
pub use fake_global_memory_manager::GlobalMemoryManager;
#[cfg(target_os = "linux")]
use memory_manager::{GlobalMemoryManager, GlobalMemoryManagerRef};
