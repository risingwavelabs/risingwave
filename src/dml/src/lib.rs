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
#![feature(coroutines)]
#![feature(type_alias_impl_trait)]
#![feature(box_patterns)]
#![feature(stmt_expr_attributes)]
#![feature(error_generic_member_access)]

pub use table::*;

pub mod dml_manager;
pub mod error;
mod table;
mod txn_channel;
