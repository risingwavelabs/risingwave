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

use risingwave_common::types::{JsonbRef, JsonbVal};
use risingwave_expr::function;

/// Concatenates the two jsonbs.
///
/// Examples:
///
/// ```slt
/// # concat
/// query T
/// SELECT '[1,2]'::jsonb || '[3,4]'::jsonb;
/// ----
/// [1, 2, 3, 4]
///
/// query T
/// SELECT '{"a": 1}'::jsonb || '{"b": 2}'::jsonb;
/// ----
/// {"a": 1, "b": 2}
///
/// query T
/// SELECT '[1,2]'::jsonb || '{"a": 1}'::jsonb;
/// ----
/// [1, 2, {"a": 1}]
///
/// query T
/// SELECT '1'::jsonb || '2'::jsonb;
/// ----
/// [1, 2]
///
/// query T
/// SELECT '[1,2]'::jsonb || 'null'::jsonb;
/// ----
/// [1, 2, null]
///
/// query T
/// SELECT 'null'::jsonb || '[1,2]'::jsonb;
/// ----
/// [null, 1, 2]
///
/// query T
/// SELECT 'null'::jsonb || '1'::jsonb;
/// ----
/// [null, 1]
/// ```
#[function("jsonb_cat(jsonb, jsonb) -> jsonb")]
pub fn jsonb_cat(left: JsonbRef<'_>, right: JsonbRef<'_>) -> JsonbVal {
    left.concat(right)
}
