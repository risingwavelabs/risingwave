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

/// Removes all elements equal to the given value from the array.
/// Note the behavior is slightly different from PG.
///
/// Examples:
///
/// ```slt
/// query T
/// select array_remove(array[array[1],array[2],array[3],array[2],null], array[1]);
/// ----
/// {{2},{3},{2},NULL}
///
/// query T
/// select array_remove(array[array[1],array[2],array[3],array[2],null], array[2]);
/// ----
/// {{1},{3},NULL}
///
/// query T
/// select array_remove(array[array[1],array[2],array[3],array[2],null], null);
/// ----
/// {{1},{2},{3},{2}}
///
/// query T
/// select array_remove(array[array[1],array[2],array[3],array[2],null], array[4]);
/// ----
/// {{1},{2},{3},{2},NULL}
///
/// query T
/// select array_remove(null, 1);
/// ----
/// NULL
///
/// query T
/// select array_remove(ARRAY[array[1],array[2],array[3],array[2],null], array[3.14]);
/// ----
/// {{1},{2},{3},{2},NULL}
///
/// query T
/// select array_remove(array[1,NULL,NULL,3], NULL);
/// ----
/// {1,3}
///
/// statement error
/// select array_remove(array[array[1],array[2],array[3],array[2],null], 1);
///
/// statement error
/// select array_remove(array[array[1],array[2],array[3],array[2],null], array[array[3]]);
///
/// statement error
/// select array_remove(ARRAY[array[1],array[2],array[3],array[2],null], array[true]);
/// ```
#[function("array_remove(anyarray, any) -> anyarray")]
fn array_remove(array: ListRef<'_>, elem: Option<ScalarRefImpl<'_>>, ctx: &Context) -> ListValue {
    ListValue::from_datum_iter(
        ctx.arg_types[0].as_list_elem(),
        array.iter().filter(|x| x != &elem),
    )
}
