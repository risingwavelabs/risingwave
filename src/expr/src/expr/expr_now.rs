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

use risingwave_common::types::DataType;
use risingwave_expr_macro::build_function;

use super::{BoxedExpression, Result};

#[build_function("now(timestamptz) -> timestamptz")]
fn build_now_expr(_: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    // there should be exact 1 child containing a timestamp literal
    Ok(children.into_iter().next().unwrap())
}
