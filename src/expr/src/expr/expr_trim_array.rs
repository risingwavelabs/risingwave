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
use risingwave_common::types::ScalarRefImpl;
use risingwave_expr_macro::function;

use crate::error::ExprError;
use crate::Result;

/// Trims an array by removing the last n elements. If the array is multidimensional, only the first
/// dimension is trimmed.
///
/// Examples:
///
/// ```slt
/// query T
/// select trim_array(array[1,2,3,4,5,null::int], 4);
/// ----
/// {1,2}
///
/// query T
/// select trim_array(array[1,2,3,4,5,null::int], 0);
/// ----
/// {1,2,3,4,5,NULL}
///
/// query T
/// select trim_array(array[1,2,3,4,5,null::int], null::int);
/// ----
/// NULL
///
/// query T
/// select trim_array(array[1,2,3,4,5,null::int], 6);
/// ----
/// {}
///
/// query T
/// select trim_array(null::int[], 1);
/// ----
/// NULL
///
/// statement error
/// select trim_array(array[1,2,3,4,5,null::int], 7);
///
/// statement error
/// select trim_array(array[1,2,3,4,5,null::int], -1);
///
/// statement error
/// select trim_array(array[1,2,3,4,5,null::int], 3.14);
///
/// statement error
/// select trim_array(array[1,2,3,4,5,null::int], null::int[]);
///
/// statement error
/// select trim_array(array[1,2,3,4,5,null::int], true);
/// ```
#[function("trim_array(list, int32) -> list")]
fn trim_array(array: Option<ListRef<'_>>, n: Option<i32>) -> Result<Option<ListValue>> {
    match (array, n) {
        (Some(array), Some(n)) => {
            let values = array.values_ref();
            let len = values.len();
            match TryInto::<usize>::try_into(n) {
                Ok(_) => {
                    if len < (n as usize) {
                        Err(ExprError::CastOutOfRange("parameter n"))
                    } else {
                        Ok(Some(ListValue::new(
                            values
                                .into_iter()
                                .take(len - (n as usize))
                                .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
                                .collect(),
                        )))
                    }
                }
                Err(_) => Err(ExprError::CastOutOfRange("parameter n")),
            }
        }
        _ => Ok(None),
    }
}
