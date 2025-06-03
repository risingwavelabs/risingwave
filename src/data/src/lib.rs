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

#![expect(
    refining_impl_trait,
    reason = "Some of the Row::iter() implementations returns ExactSizeIterator. Is this reasonable?"
)]
#![feature(trait_alias)]
#![feature(type_alias_impl_trait)]
#![feature(test)]
#![feature(trusted_len)]
#![feature(allocator_api)]
#![feature(coroutines)]
#![feature(map_try_insert)]
#![feature(error_generic_member_access)]
#![feature(let_chains)]
#![feature(portable_simd)]
#![feature(array_chunks)]
#![feature(inline_const_pat)]
#![allow(incomplete_features)]
#![feature(iterator_try_collect)]
#![feature(iter_order_by)]
#![feature(binary_heap_into_iter_sorted)]
#![feature(impl_trait_in_assoc_type)]
#![feature(negative_impls)]
#![feature(register_tool)]
#![feature(btree_cursors)]
#![feature(assert_matches)]
#![feature(anonymous_lifetime_in_impl_trait)]
#![register_tool(rw)]

#[cfg_attr(not(test), allow(unused_extern_crates))]
extern crate self as risingwave_data;

// Re-export all macros from `risingwave_error` crate for code compatibility
#[macro_use]
extern crate risingwave_error;

#[macro_use]
pub mod array;
pub mod row;
pub mod types;

pub mod test_prelude {
    pub use super::array::{DataChunkTestExt, StreamChunkTestExt};
}