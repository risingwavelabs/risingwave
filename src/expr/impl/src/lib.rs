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
#![feature(iterator_try_collect)]
#![feature(coroutines)]
#![feature(test)]
#![feature(iter_array_chunks)]
#![feature(used_with_arg)]
#![feature(coverage_attribute)]

mod aggregate;
mod scalar;
mod table_function;
mod udf;
mod window_function;

/// Enable functions in this crate.
#[macro_export]
macro_rules! enable {
    () => {
        use risingwave_expr_impl as _;
    };
}
