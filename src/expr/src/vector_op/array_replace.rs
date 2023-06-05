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

use risingwave_common::array::{ListRef, ListValue};
use risingwave_common::types::{ScalarRef, ToOwnedDatum};
use risingwave_expr_macro::function;

/// Replaces each array element equal to the second argument with the third argument.
///
/// Examples:
///
/// ```slt
/// query T
/// select array_replace(array[7, null, 8, null], null, 0.5);
/// ----
/// {7,0.5,8,0.5}
///
/// query T
/// select array_replace(null, 1, 5);
/// ----
/// NULL
///
/// query T
/// select array_replace(null, null, null);
/// ----
/// NULL
///
/// statement error
/// select array_replace(array[3, null, 4], true, false);
///
/// -- Replacing `int` in multidimensional array is not supported yet. (OK in PostgreSQL)
/// statement error
/// select array_replace(array[array[array[0, 1], array[2, 3]], array[array[4, 5], array[6, 7]]], 3, 9);
///
/// -- Unlike PostgreSQL, it is okay to replace `int[][]` inside `int[][][]`.
/// query T
/// select array_replace(array[array[array[0, 1], array[2, 3]], array[array[4, 5], array[6, 7]]], array[array[4, 5], array[6, 7]], array[array[2, 3], array[4, 5]]);
/// ----
/// {{{0,1},{2,3}},{{2,3},{4,5}}}
///
/// -- Replacing `int[]` inside `int[][][]` is not supported by either PostgreSQL or RisingWave.
/// -- This may or may not be supported later, whichever makes the `int` support above simpler.
/// statement error
/// select array_replace(array[array[array[0, 1], array[2, 3]], array[array[4, 5], array[6, 7]]], array[4, 5], array[8, 9]);
/// ```
#[function("array_replace(list, boolean, boolean) -> list")]
#[function("array_replace(list, int16, int16) -> list")]
#[function("array_replace(list, int32, int32) -> list")]
#[function("array_replace(list, int64, int64) -> list")]
#[function("array_replace(list, decimal, decimal) -> list")]
#[function("array_replace(list, float32, float32) -> list")]
#[function("array_replace(list, float64, float64) -> list")]
#[function("array_replace(list, varchar, varchar) -> list")]
#[function("array_replace(list, bytea, bytea) -> list")]
#[function("array_replace(list, time, time) -> list")]
#[function("array_replace(list, interval, interval) -> list")]
#[function("array_replace(list, date, date) -> list")]
#[function("array_replace(list, timestamp, timestamp) -> list")]
#[function("array_replace(list, timestamptz, timestamptz) -> list")]
#[function("array_replace(list, list, list) -> list")]
#[function("array_replace(list, struct, struct) -> list")]
#[function("array_replace(list, jsonb, jsonb) -> list")]
#[function("array_replace(list, int256, int256) -> list")]
fn array_replace<'a, T: ScalarRef<'a>>(
    arr: Option<ListRef<'_>>,
    elem_from: Option<T>,
    elem_to: Option<T>,
) -> Option<ListValue> {
    arr.map(|arr| {
        ListValue::new(
            arr.iter()
                .map(|x| match x == elem_from.map(Into::into) {
                    true => elem_to.map(Into::into).to_owned_datum(),
                    false => x.to_owned_datum(),
                })
                .collect(),
        )
    })
}
