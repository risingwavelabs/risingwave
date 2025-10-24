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

#![feature(iterator_try_collect)]
#![feature(coroutines)]
#![feature(never_type)]
#![feature(error_generic_member_access)]
#![feature(used_with_arg)]
#![feature(ascii_char)]

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
