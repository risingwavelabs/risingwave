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

#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(trait_alias)]
#![feature(exact_size_is_empty)]
#![feature(type_alias_impl_trait)]
#![feature(coverage_attribute)]
#![feature(coroutines)]
#![feature(proc_macro_hygiene, stmt_expr_attributes)]
#![feature(iterator_try_collect)]
#![recursion_limit = "256"]
#![feature(let_chains)]
#![feature(int_roundings)]
#![feature(allocator_api)]
#![feature(impl_trait_in_assoc_type)]
#![feature(assert_matches)]
#![feature(error_generic_member_access)]
#![feature(map_try_insert)]
#![feature(iter_from_coroutine)]
#![feature(used_with_arg)]

pub mod error;
pub mod exchange_source;
pub mod execution;
pub mod executor;
pub mod monitor;
pub mod rpc;
pub mod spill;
pub mod task;
pub mod worker_manager;

#[macro_use]
extern crate tracing;
#[macro_use]
extern crate risingwave_common;

extern crate self as risingwave_batch;
