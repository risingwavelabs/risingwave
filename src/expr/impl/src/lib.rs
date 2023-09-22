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

//! Function implementations.
//!
//! To enable functions in this crate, add the following line to your code:
//!
//! ```
//! risingwave_expr_impl::enable!();
//! ```

#![allow(non_snake_case)] // for `ctor` generated code
#![feature(let_chains)]
#![feature(assert_matches)]
#![feature(lint_reasons)]
#![feature(iterator_try_collect)]
#![feature(exclusive_range_pattern)]
#![feature(lazy_cell)]
#![feature(round_ties_even)]
#![feature(generators)]
#![feature(test)]
#![feature(arc_unwrap_or_clone)]

mod aggregate;
mod scalar;
mod table_function;

/// Enable functions in this crate.
#[macro_export]
macro_rules! enable {
    () => {
        use risingwave_expr_impl as _;
    };
}
