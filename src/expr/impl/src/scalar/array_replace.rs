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
use risingwave_common::types::ScalarRefImpl;
use risingwave_expr::expr::Context;
use risingwave_expr::function;

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
/// # Replacing `int` in multidimensional array is not supported yet. (OK in PostgreSQL)
/// statement error
/// select array_replace(array[array[array[0, 1], array[2, 3]], array[array[4, 5], array[6, 7]]], 3, 9);
///
/// # Unlike PostgreSQL, it is okay to replace `int[][]` inside `int[][][]`.
/// query T
/// select array_replace(array[array[array[0, 1], array[2, 3]], array[array[4, 5], array[6, 7]]], array[array[4, 5], array[6, 7]], array[array[2, 3], array[4, 5]]);
/// ----
/// {{{0,1},{2,3}},{{2,3},{4,5}}}
///
/// # Replacing `int[]` inside `int[][][]` is not supported by either PostgreSQL or RisingWave.
/// # This may or may not be supported later, whichever makes the `int` support above simpler.
/// statement error
/// select array_replace(array[array[array[0, 1], array[2, 3]], array[array[4, 5], array[6, 7]]], array[4, 5], array[8, 9]);
/// ```
#[function("array_replace(anyarray, any, any) -> anyarray")]
fn array_replace(
    array: ListRef<'_>,
    elem_from: Option<ScalarRefImpl<'_>>,
    elem_to: Option<ScalarRefImpl<'_>>,
    ctx: &Context,
) -> ListValue {
    ListValue::from_datum_iter(
        ctx.arg_types[0].as_list_elem(),
        array.iter().map(|val| match val == elem_from {
            true => elem_to,
            false => val,
        }),
    )
}
