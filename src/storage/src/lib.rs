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

#![warn(clippy::dbg_macro)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::unused_async)]
#![warn(clippy::map_flatten)]
#![warn(clippy::no_effect_underscore_binding)]
#![warn(clippy::await_holding_lock)]
#![deny(unused_must_use)]
#![deny(rustdoc::broken_intra_doc_links)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]
#![feature(drain_filter)]
#![feature(bound_map)]
#![feature(backtrace)]
#![feature(map_first_last)]
#![feature(type_alias_impl_trait)]
#![feature(let_chains)]
#![feature(test)]
#![feature(custom_test_frameworks)]
#![feature(result_option_inspect)]
#![feature(generators)]
#![feature(lint_reasons)]
#![test_runner(risingwave_test_runner::test_runner::run_failpont_tests)]

pub mod cell_based_row_deserializer;
pub mod cell_based_row_serializer;
pub mod hummock;
pub mod keyspace;
pub mod memory;
pub mod monitor;
pub mod panic_store;
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
extern crate test;
pub use store::{StateStore, StateStoreIter};
pub use store_impl::StateStoreImpl;

pub enum TableScanOptions {
    SequentialScan,
    SparseIndexScan,
}
