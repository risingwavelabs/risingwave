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

use risingwave_common::array::{ListRef, ListValue};
use risingwave_expr::expr::Context;
use risingwave_expr::{ExprError, Result, function};

/// Flattens a nested array by concatenating the inner arrays into a single array.
/// Only the outermost level of nesting is removed. For deeper nested arrays, call
/// `array_flatten` multiple times.
///
/// Examples:
///
/// ```slt
/// query T
/// select array_flatten(array[array[1, 2], array[3, 4]]);
/// ----
/// {1,2,3,4}
///
/// query T
/// select array_flatten(array[array[1, 2], array[]::int[], array[3, 4]]);
/// ----
/// {1,2,3,4}
///
/// query T
/// select array_flatten(array[array[1, 2], null, array[3, 4]]);
/// ----
/// {1,2,3,4}
///
/// query T
/// select array_flatten(array[array[array[1], array[2, null]], array[array[3, 4], null::int[]]]);
/// ----
/// {{1},{2,NULL},{3,4},NULL}
///
/// query T
/// select array_flatten(array[[]]::int[][]);
/// ----
/// {}
///
/// query T
/// select array_flatten(array[[null, 1]]::int[][]);
/// ----
/// {NULL,1}
///
/// query T
/// select array_flatten(array[]::int[][]);
/// ----
/// {}
///
/// query T
/// select array_flatten(null::int[][]);
/// ----
/// NULL
/// ```
#[function("array_flatten(anyarray) -> anyarray")]
fn array_flatten(array: ListRef<'_>, ctx: &Context) -> Result<ListValue> {
    // The elements of the array must be arrays themselves
    let outer_type = &ctx.arg_types[0];
    let inner_type = if outer_type.is_array() {
        outer_type.as_list_element_type()
    } else {
        return Err(ExprError::InvalidParam {
            name: "array_flatten",
            reason: Box::from("expected the argument to be an array of arrays"),
        });
    };
    if !inner_type.is_array() {
        return Err(ExprError::InvalidParam {
            name: "array_flatten",
            reason: Box::from("expected the argument to be an array of arrays"),
        });
    }
    let inner_elem_type = inner_type.as_list_element_type();

    // Collect all inner array elements and flatten them into a single array
    Ok(ListValue::from_datum_iter(
        inner_elem_type,
        array
            .iter()
            // Filter out NULL inner arrays
            .flatten()
            // Flatten all inner arrays
            .flat_map(|inner_array| inner_array.into_list().iter()),
    ))
}
