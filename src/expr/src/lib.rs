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
#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(binary_heap_into_iter_sorted)]
#![feature(is_sorted)]
#![feature(fn_traits)]
#![feature(assert_matches)]
#![feature(let_chains)]
#![feature(lint_reasons)]
#![feature(type_alias_impl_trait)]
#![feature(generators)]
#![feature(iterator_try_collect)]

pub mod error;
pub mod expr;
pub mod table_function;
pub mod vector_op;

pub use error::ExprError;
pub use risingwave_common::{bail, ensure};
pub type Result<T> = std::result::Result<T, ExprError>;
