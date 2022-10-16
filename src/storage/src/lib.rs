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

#![feature(allocator_api)]
#![feature(binary_heap_drain_sorted)]
#![feature(bound_as_ref)]
#![feature(bound_map)]
#![feature(build_hasher_simple_hash_one)]
#![feature(custom_test_frameworks)]
#![feature(drain_filter)]
#![feature(generators)]
#![feature(hash_drain_filter)]
#![feature(let_chains)]
#![feature(lint_reasons)]
#![feature(proc_macro_hygiene)]
#![feature(result_option_inspect)]
#![feature(stmt_expr_attributes)]
#![feature(strict_provenance)]
#![feature(test)]
#![feature(trait_alias)]
#![feature(type_alias_impl_trait)]
#![feature(type_changing_struct_update)]
#![test_runner(risingwave_test_runner::test_runner::run_failpont_tests)]
#![feature(assert_matches)]
#![feature(is_sorted)]
#![feature(btree_drain_filter)]
#![feature(exact_size_is_empty)]
#![feature(once_cell)]
#![cfg_attr(coverage, feature(no_coverage))]
#![feature(error_generic_member_access)]
#![feature(provide_any)]
#![expect(clippy::result_large_err, reason = "FIXME: HummockError is large")]

pub mod hummock;
pub mod keyspace;
pub mod memory;
pub mod monitor;
pub mod panic_store;
pub mod row_serde;
pub mod storage_value;
#[macro_use]
pub mod store;
pub mod error;
pub mod store_impl;
pub mod table;
pub mod write_batch;

#[cfg(test)]
#[cfg(feature = "failpoints")]
mod storage_failpoints;

pub use keyspace::Keyspace;
pub use store::{StateStore, StateStoreIter};
pub use store_impl::StateStoreImpl;

pub enum TableScanOptions {
    SequentialScan,
    SparseIndexScan,
}
