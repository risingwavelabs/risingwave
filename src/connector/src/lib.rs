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

#![expect(dead_code)]
#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(generators)]
#![feature(proc_macro_hygiene)]
#![feature(stmt_expr_attributes)]
#![feature(box_patterns)]
#![feature(trait_alias)]
#![feature(lint_reasons)]
#![feature(lazy_cell)]
#![feature(result_option_inspect)]
#![feature(let_chains)]
#![feature(box_into_inner)]
#![feature(type_alias_impl_trait)]
#![feature(return_position_impl_trait_in_trait)]
#![feature(async_fn_in_trait)]
#![feature(associated_type_defaults)]
#![feature(impl_trait_in_assoc_type)]
#![feature(iter_from_generator)]
#![feature(if_let_guard)]
#![feature(iterator_try_collect)]

pub use risingwave_connector_common::*;

pub mod error;
mod macros;

pub mod source;

pub use paste::paste;
