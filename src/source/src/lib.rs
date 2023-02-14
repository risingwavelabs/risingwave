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

#![allow(clippy::derive_partial_eq_without_eq)]
#![allow(rustdoc::private_intra_doc_links)]
#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(lint_reasons)]
#![feature(result_option_inspect)]
#![feature(generators)]
#![feature(hash_drain_filter)]
#![feature(type_alias_impl_trait)]
#![feature(box_patterns)]

pub use table::*;

pub mod dml_manager;

mod common;
pub mod connector_source;
pub mod source_desc;
pub use source_desc::test_utils as connector_test_utils;
pub mod fs_connector_source;
pub mod row_id;
mod table;
