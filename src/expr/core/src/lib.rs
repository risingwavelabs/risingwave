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

#![allow(non_snake_case)] // for `ctor` generated code
#![feature(let_chains)]
#![feature(lint_reasons)]
#![feature(iterator_try_collect)]
#![feature(lazy_cell)]
#![feature(coroutines)]
#![feature(arc_unwrap_or_clone)]
#![feature(never_type)]
#![feature(error_generic_member_access)]

extern crate self as risingwave_expr;

pub mod aggregate;
#[doc(hidden)]
pub mod codegen;
mod error;
pub mod expr;
pub mod expr_context;
pub mod scalar;
pub mod sig;
pub mod table_function;
pub mod window_function;

pub use error::{ContextUnavailable, ExprError, Result};
pub use risingwave_common::{bail, ensure};
pub use risingwave_expr_macro::*;
