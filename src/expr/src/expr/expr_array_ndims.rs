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

use risingwave_common::array::ListRef;
use risingwave_common::types::ScalarRefImpl;
use risingwave_expr_macro::function;

/// Returns the number of dimensions of the array.
///
/// ```sql
/// array_ndims ( array anyarray) → int64
/// ```
///
/// Examples:
///
/// ```slt
/// query T
/// select array_ndims(null::int[]);
/// ----
/// NULL
///
/// query T
/// select array_ndims(array[1,2,3]);
/// ----
/// 1
///
/// query T
/// select array_ndims(array[1,2,3,4,1]);
/// ----
/// 1
///
/// query T
/// select array_ndims(array[array[1, 2, 3]]);
/// ----
/// 2
///
/// query T
/// select array_ndims(array[array[array[3,4,5],array[2,2,2]],array[array[6,7,8],array[0,0,0]]]);
/// ----
/// 3
///
/// query T
/// select array_ndims(array[NULL]);
/// ----
/// 1
///
/// query error unknown type
/// select array_ndims(null);
/// ```
#[function("array_ndims(list) -> int64")]
fn array_ndims(array: ListRef<'_>) -> i64 {
    if let Some(ScalarRefImpl::List(list_ref)) = array.value_at(1).unwrap() {
        1 + array_ndims(list_ref)
    } else {
        1
    }
}
