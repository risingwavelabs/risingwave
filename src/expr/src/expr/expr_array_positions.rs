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
use risingwave_common::types::{ScalarImpl, ScalarRef};
use risingwave_expr_macro::function;

use crate::error::ExprError;
use crate::Result;

/// Returns an array of the subscripts of all occurrences of the second argument in the array
/// given as first argument. Note the behavior is slightly different from PG.
///
/// Examples:
///
/// ```slt
/// query T
/// select array_positions(array[array[1],array[2],array[3],array[2],null::int[]], array[1]);
/// ----
/// {1}
///
/// query T
/// select array_positions(array[array[1],array[2],array[3],array[2],null::int[]], array[2]);
/// ----
/// {2,4}
///
/// query T
/// select array_positions(array[array[1],array[2],array[3],array[2],null::int[]], null::int[]);
/// ----
/// {5}
///
/// query T
/// select array_positions(array[array[1],array[2],array[3],array[2],null::int[]], array[4]);
/// ----
/// {}
///
/// query T
/// select array_positions(null::int[], 1);
/// ----
/// NULL
///
/// query T
/// select array_positions(ARRAY[array[1],array[2],array[3],array[2],null::int[]], array[3.14]);
/// ----
/// {}
///
/// query T
/// select array_positions(array[1,NULL,NULL,3], NULL::int);
/// ----
/// {2,3}
///
/// statement error
/// select array_positions(array[array[1],array[2],array[3],array[2],null::int[]], 1);
///
/// statement error
/// select array_positions(array[array[1],array[2],array[3],array[2],null::int[]], array[array[3]]);
///
/// statement error
/// select array_positions(ARRAY[array[1],array[2],array[3],array[2],null::int[]], array[true]);
/// ```
#[function("array_positions(list, *) -> list")]
fn array_positions<'a, T: ScalarRef<'a>>(
    array: Option<ListRef<'_>>,
    element: Option<T>,
) -> Result<Option<ListValue>> {
    match array {
        Some(left) => {
            let values = left.values_ref();
            match TryInto::<i32>::try_into(values.len()) {
                Ok(_) => Ok(Some(
                    ListValue::new(
                        values
                            .into_iter()
                            .enumerate()
                            .filter(|(_, item)| item == &element.map(|x| x.into()))
                            .map(|(idx, _)| Some(ScalarImpl::Int32((idx + 1) as _)))
                            .collect(),
                    )
                    .into(),
                )),
                Err(_) => Err(ExprError::CastOutOfRange("invalid array length")),
            }
        }
        _ => Ok(None),
    }
}
