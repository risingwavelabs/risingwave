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

#![allow(rustdoc::private_intra_doc_links)]
#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(is_sorted)]
#![feature(type_alias_impl_trait)]
#![feature(test)]
#![feature(trusted_len)]
#![feature(allocator_api)]
#![feature(lint_reasons)]
#![feature(generators)]
#![feature(map_try_insert)]
#![feature(lazy_cell)]
#![feature(error_generic_member_access)]
#![feature(provide_any)]
#![feature(let_chains)]
#![feature(return_position_impl_trait_in_trait)]
#![feature(portable_simd)]
#![feature(array_chunks)]
#![feature(inline_const_pat)]
#![allow(incomplete_features)]
#![feature(iterator_try_collect)]
#![feature(round_ties_even)]
#![feature(iter_order_by)]
#![feature(exclusive_range_pattern)]
#![feature(binary_heap_into_iter_sorted)]
#![feature(impl_trait_in_assoc_type)]

#[macro_use]
pub mod jemalloc;
#[macro_use]
pub mod error;
#[macro_use]
pub mod array;
#[macro_use]
pub mod util;
pub mod buffer;
pub mod cache;
pub mod cast;
pub mod catalog;
pub mod config;
pub mod constants;
pub mod estimate_size;
pub mod field_generator;
pub mod hash;
pub mod memory;
pub mod monitor;
pub mod row;
pub mod session_config;
pub mod system_param;
pub mod telemetry;

pub mod metrics;
pub mod test_utils;
pub mod types;

pub mod test_prelude {
    pub use super::array::{DataChunkTestExt, StreamChunkTestExt};
    pub use super::catalog::test_utils::ColumnDescTestExt;
}

pub const RW_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Placeholder for unknown git sha.
pub const UNKNOWN_GIT_SHA: &str = "unknown";

#[macro_export]
macro_rules! git_sha {
    ($env:literal) => {
        match option_env!($env) {
            Some(v) if !v.is_empty() => v,
            _ => $crate::UNKNOWN_GIT_SHA,
        }
    };
}

// FIXME: We expand `unwrap_or` since it's unavailable in const context now.
// `const_option_ext` was broken by https://github.com/rust-lang/rust/pull/110393
// Tracking issue: https://github.com/rust-lang/rust/issues/91930
pub const GIT_SHA: &str = git_sha!("GIT_SHA");
