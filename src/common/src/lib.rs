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

#![allow(rustdoc::private_intra_doc_links)]
#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(is_sorted)]
#![feature(fn_traits)]
#![feature(type_alias_impl_trait)]
#![feature(test)]
#![feature(trusted_len)]
#![feature(allocator_api)]
#![feature(lint_reasons)]
#![feature(generators)]
#![feature(map_try_insert)]
#![feature(once_cell)]
#![feature(let_chains)]
#![feature(error_generic_member_access)]
#![feature(provide_any)]

#[macro_use]
pub mod error;
#[macro_use]
pub mod array;
#[macro_use]
pub mod util;
pub mod buffer;
pub mod cache;
pub mod catalog;
pub mod collection;
pub mod config;
pub mod field_generator;
pub mod hash;
pub mod monitor;
pub mod session_config;
#[cfg(test)]
pub mod test_utils;
pub mod types;

pub mod test_prelude {
    pub use super::array::{DataChunkTestExt, StreamChunkTestExt};
    pub use super::catalog::test_utils::ColumnDescTestExt;
}
