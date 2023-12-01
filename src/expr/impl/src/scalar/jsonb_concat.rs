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

use jsonbb::{Value, ValueRef};
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
#[function("jsonb_concat(jsonb, jsonb) -> jsonb")]
pub fn jsonb_concat(left: JsonbRef<'_>, right: JsonbRef<'_>) -> JsonbVal {
    match (left.into(), right.into()) {
        // left and right are object based.
        // This would have left:{'a':1}, right:{'b':2} -> {'a':1,'b':2}
        (ValueRef::Object(left), ValueRef::Object(right)) => {
            JsonbVal::from(Value::object(left.iter().chain(right.iter())))
        }

        // left and right are array-based.
        // This would merge both arrays into one array.
        // This would have left:[1,2], right:[3,4] -> [1,2,3,4]
        (ValueRef::Array(left), ValueRef::Array(right)) => {
            JsonbVal::from(Value::array(left.iter().chain(right.iter())))
        }

        // One operand is an array, and the other is a single element.
        // This would insert the non-array value as another element into the array
        // Eg left:[1,2] right: {'a':1} -> [1,2,{'a':1}]
        (ValueRef::Array(left), value) => JsonbVal::from(Value::array(left.iter().chain([value]))),

        // One operand is an array, and the other is a single element.
        // This would insert the non-array value as another element into the array
        // Eg left:{'a':1} right:[1,2] -> [{'a':1},1,2]
        (value, ValueRef::Array(right)) => {
            JsonbVal::from(Value::array([value].into_iter().chain(right.iter())))
        }

        // Both are non-array inputs.
        // Both elements would be placed together in an array
        // Eg left:1 right: 2 -> [1,2]
        (left, right) => JsonbVal::from(Value::array([left, right])),
    }
}
