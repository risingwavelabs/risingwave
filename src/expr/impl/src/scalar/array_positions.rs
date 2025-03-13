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

use risingwave_common::array::{I32Array, ListRef, ListValue};
use risingwave_common::types::ScalarRefImpl;
use risingwave_expr::{ExprError, Result, function};

/// Returns the subscript of the first occurrence of the second argument in the array, or `NULL` if
/// it's not present.
///
/// Examples:
///
/// ```slt
/// query I
/// select array_position(array[1, null, 2, null], null);
/// ----
/// 2
///
/// query I
/// select array_position(array[3, 4, 5], 2);
/// ----
/// NULL
///
/// query I
/// select array_position(null, 4);
/// ----
/// NULL
///
/// query I
/// select array_position(null, null);
/// ----
/// NULL
///
/// query I
/// select array_position('{yes}', true);
/// ----
/// 1
///
/// # Like in PostgreSQL, searching `int` in multidimensional array is disallowed.
/// statement error
/// select array_position(array[array[1, 2], array[3, 4]], 1);
///
/// # Unlike in PostgreSQL, it is okay to search `int[]` inside `int[][]`.
/// query I
/// select array_position(array[array[1, 2], array[3, 4]], array[3, 4]);
/// ----
/// 2
///
/// statement error
/// select array_position(array[3, 4], true);
///
/// query I
/// select array_position(array[3, 4], 4.0);
/// ----
/// 2
/// ```
#[function("array_position(anyarray, any) -> int4")]
pub(super) fn array_position(
    array: ListRef<'_>,
    element: Option<ScalarRefImpl<'_>>,
) -> Result<Option<i32>> {
    array_position_common(array, element, 0)
}

/// Returns the subscript of the first occurrence of the second argument in the array, or `NULL` if
/// it's not present. The search begins at the third argument.
///
/// Examples:
///
/// ```slt
/// statement error
/// select array_position(array[1, null, 2, null], null, false);
///
/// statement error
/// select array_position(array[1, null, 2, null], null, null::int);
///
/// query II
/// select v, array_position(array[1, null, 2, null], null, v) from generate_series(-1, 5) as t(v);
/// ----
/// -1    2
///  0    2
///  1    2
///  2    2
///  3    4
///  4    4
///  5 NULL
/// ```
#[function("array_position(anyarray, any, int4) -> int4")]
fn array_position_start(
    array: ListRef<'_>,
    element: Option<ScalarRefImpl<'_>>,
    start: Option<i32>,
) -> Result<Option<i32>> {
    let start = match start {
        None => {
            return Err(ExprError::InvalidParam {
                name: "start",
                reason: "initial position must not be null".into(),
            });
        }
        Some(start) => (start.max(1) - 1) as usize,
    };
    array_position_common(array, element, start)
}

fn array_position_common(
    array: ListRef<'_>,
    element: Option<ScalarRefImpl<'_>>,
    skip: usize,
) -> Result<Option<i32>> {
    if i32::try_from(array.len()).is_err() {
        return Err(ExprError::CastOutOfRange("invalid array length"));
    }

    Ok(array
        .iter()
        .skip(skip)
        .position(|item| item == element)
        .map(|idx| (idx + 1 + skip) as _))
}

/// Returns an array of the subscripts of all occurrences of the second argument in the array
/// given as first argument. Note the behavior is slightly different from PG.
///
/// Examples:
///
/// ```slt
/// query T
/// select array_positions(array[array[1],array[2],array[3],array[2],null], array[1]);
/// ----
/// {1}
///
/// query T
/// select array_positions(array[array[1],array[2],array[3],array[2],null], array[2]);
/// ----
/// {2,4}
///
/// query T
/// select array_positions(array[array[1],array[2],array[3],array[2],null], null);
/// ----
/// {5}
///
/// query T
/// select array_positions(array[array[1],array[2],array[3],array[2],null], array[4]);
/// ----
/// {}
///
/// query T
/// select array_positions(null, 1);
/// ----
/// NULL
///
/// query T
/// select array_positions(ARRAY[array[1],array[2],array[3],array[2],null], array[3.14]);
/// ----
/// {}
///
/// query T
/// select array_positions(array[1,NULL,NULL,3], NULL);
/// ----
/// {2,3}
///
/// statement error
/// select array_positions(array[array[1],array[2],array[3],array[2],null], 1);
///
/// statement error
/// select array_positions(array[array[1],array[2],array[3],array[2],null], array[array[3]]);
///
/// statement error
/// select array_positions(ARRAY[array[1],array[2],array[3],array[2],null], array[true]);
/// ```
#[function("array_positions(anyarray, any) -> int4[]")]
fn array_positions(
    array: Option<ListRef<'_>>,
    element: Option<ScalarRefImpl<'_>>,
) -> Result<Option<ListValue>> {
    let Some(array) = array else {
        return Ok(None);
    };
    let values = array.iter();
    if values.len() - 1 > i32::MAX as usize {
        return Err(ExprError::CastOutOfRange("invalid array length"));
    }
    Ok(Some(ListValue::new(
        values
            .enumerate()
            .filter(|(_, item)| item == &element)
            .map(|(idx, _)| idx as i32 + 1)
            .collect::<I32Array>()
            .into(),
    )))
}
