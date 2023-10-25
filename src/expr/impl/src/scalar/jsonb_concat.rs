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
use serde_json::{json, Value};

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
    let left_val = left.value().clone();
    let right_val = right.value().clone();
    match (left_val, right_val) {
        // left and right are object based.
        // This would have left:{'a':1}, right:{'b':2} -> {'a':1,'b':2}
        (Value::Object(mut left_map), Value::Object(right_map)) => {
            left_map.extend(right_map);
            JsonbVal::from(Value::Object(left_map))
        }

        // left and right are array-based.
        // This would merge both arrays into one array.
        // This would have left:[1,2], right:[3,4] -> [1,2,3,4]
        (Value::Array(mut left_arr), Value::Array(right_arr)) => {
            left_arr.extend(right_arr);
            JsonbVal::from(Value::Array(left_arr))
        }

        // One operand is an array, and the other is a single element.
        // This would insert the non-array value as another element into the array
        // Eg left:[1,2] right: {'a':1} -> [1,2,{'a':1}]
        (Value::Array(mut left_arr), single_val) => {
            left_arr.push(single_val);
            JsonbVal::from(Value::Array(left_arr))
        }

        // One operand is an array, and the other is a single element.
        // This would insert the non-array value as another element into the array
        // Eg left:{'a':1} right:[1,2] -> [{'a':1},1,2]
        (single_val, Value::Array(mut right_arr)) => {
            right_arr.insert(0, single_val);
            JsonbVal::from(Value::Array(right_arr))
        }

        // Both are non-array inputs.
        // Both elements would be placed together in an array
        // Eg left:1 right: 2 -> [1,2]
        (left, right) => JsonbVal::from(json!([left, right])),
    }
}
