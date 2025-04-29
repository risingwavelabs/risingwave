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

use itertools::Either;
use risingwave_common::array::{ListRef, ListValue};
use risingwave_common::types::ScalarRefImpl;
use risingwave_expr::{Result, function};

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
fn array_flatten(array: ListRef<'_>) -> Result<ListValue> {
    // The elements of the array must be arrays themselves
    // Create a new list by flattening all inner arrays
    let array_data_type = array.data_type();
    let inner_type = array_data_type.as_list();

    // Collect all inner array elements and flatten them into a single array
    Ok(ListValue::from_datum_iter(
        inner_type,
        array
            .iter()
            // Filter out NULL inner arrays
            .flatten()
            // Flatten all inner arrays
            .flat_map(|inner_array| {
                if let ScalarRefImpl::List(inner_list) = inner_array {
                    Either::Left(inner_list.iter())
                } else {
                    // This shouldn't happen but handle it gracefully
                    Either::Right(std::iter::empty())
                }
            }),
    ))
}
