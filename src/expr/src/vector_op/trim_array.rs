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
use risingwave_common::types::ToOwnedDatum;
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
/// select trim_array(array[1,2,3,4,5,null], 4);
/// ----
/// {1,2}
///
/// query T
/// select trim_array(array[1,2,3,4,5,null], 0);
/// ----
/// {1,2,3,4,5,NULL}
///
/// query T
/// select trim_array(array[1,2,3,4,5,null], null);
/// ----
/// NULL
///
/// query T
/// select trim_array(array[1,2,3,4,5,null], null::smallint);
/// ----
/// NULL
///
/// query T
/// select trim_array(array[1,2,3,4,5,null], 6);
/// ----
/// {}
///
/// query T
/// select trim_array(null::int[], 1);
/// ----
/// NULL
///
/// statement error
/// select trim_array(array[1,2,3,4,5,null], 7);
///
/// statement error
/// select trim_array(array[1,2,3,4,5,null], -1);
///
/// statement error
/// select trim_array(array[1,2,3,4,5,null], null::bigint);
///
/// statement error
/// select trim_array(array[1,2,3,4,5,null], 3.14);
///
/// statement error
/// select trim_array(array[1,2,3,4,5,null], array[1]);
///
/// statement error
/// select trim_array(array[1,2,3,4,5,null], true);
/// ```
#[function("trim_array(list, int32) -> list")]
fn trim_array(array: ListRef<'_>, n: i32) -> Result<ListValue> {
    let values = array.iter();
    let len_to_trim: usize = n.try_into().map_err(|_| ExprError::InvalidParam {
        name: "n",
        reason: "less than zero".to_string(),
    })?;
    let len_to_retain =
        values
            .len()
            .checked_sub(len_to_trim)
            .ok_or_else(|| ExprError::InvalidParam {
                name: "n",
                reason: "more than array length".to_string(),
            })?;
    Ok(ListValue::new(
        values
            .take(len_to_retain)
            .map(|x| x.to_owned_datum())
            .collect(),
    ))
}
