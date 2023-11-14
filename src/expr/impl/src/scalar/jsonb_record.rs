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

use risingwave_common::types::{JsonbRef, StructValue};
use risingwave_expr::expr::Context;
use risingwave_expr::{function, ExprError, Result};

/// Expands the top-level JSON object to a row having the composite type defined by an AS clause.
/// The output record is filled from fields of the JSON object, in the same way as described above
/// for `jsonb_populate_record`. Since there is no input record value, unmatched columns are always
/// filled with nulls.
///
/// # Examples
///
/// ```slt
/// query T
/// select jsonb_to_record('{"a":1,"b":[1,2,3],"c":[1,2,3],"e":"bar","r": {"a": 123, "b": "a b c"}}')
/// :: struct<a int, b text, c int[], d text, r struct<a int, b int>>;
/// ----
/// 1 [1,2,3] {1,2,3} NULL (123,"a b c")
/// ```
#[function("jsonb_to_record(jsonb) -> struct", type_infer = "panic")]
fn jsonb_to_record(jsonb: JsonbRef<'_>, ctx: &Context) -> Result<StructValue> {
    let output_type = ctx.return_type.as_struct();
    jsonb
        .to_struct(output_type)
        .map_err(|e| ExprError::Parse(e.into()))
}
