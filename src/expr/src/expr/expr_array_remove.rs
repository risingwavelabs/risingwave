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

/// Removes all elements equal to the given value from the array.
/// Note the behavior is slightly different from PG.
///
/// Examples:
///
/// ```slt
/// query T
/// select array_remove(array[array[1],array[2],array[3],array[2],null::int[]], array[1]);
/// ----
/// {{2},{3},{2},NULL}
///
/// query T
/// select array_remove(array[array[1],array[2],array[3],array[2],null::int[]], array[2]);
/// ----
/// {{1},{3},NULL}
///
/// query T
/// select array_remove(array[array[1],array[2],array[3],array[2],null::int[]], null::int[]);
/// ----
/// {{1},{2},{3},{2}}
///
/// query T
/// select array_remove(array[array[1],array[2],array[3],array[2],null::int[]], array[4]);
/// ----
/// {{1},{2},{3},{2},NULL}
///
/// query T
/// select array_remove(null::int[], 1);
/// ----
/// NULL
///
/// query T
/// select array_remove(ARRAY[array[1],array[2],array[3],array[2],null::int[]], array[3.14]);
/// ----
/// {{1},{2},{3},{2},NULL}
///
/// query T
/// select array_remove(array[1,NULL,NULL,3], NULL::int);
/// ----
/// {1,3}
///
/// statement error
/// select array_remove(array[array[1],array[2],array[3],array[2],null::int[]], 1);
///
/// statement error
/// select array_remove(array[array[1],array[2],array[3],array[2],null::int[]], array[array[3]]);
///
/// statement error
/// select array_remove(ARRAY[array[1],array[2],array[3],array[2],null::int[]], array[true]);
/// ```
#[function("array_remove(list, *) -> list")]
fn array_remove<'a, T: ScalarRef<'a>>(
    arr: Option<ListRef<'_>>,
    elem: Option<T>,
) -> Option<ListValue> {
    arr.map(|arr| {
        ListValue::new(
            arr.iter()
                .filter(|x| x != &elem.map(Into::into))
                .map(|x| x.to_owned_datum())
                .collect(),
        )
    })
}
