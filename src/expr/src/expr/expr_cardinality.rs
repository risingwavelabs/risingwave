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
use risingwave_expr_macro::function;

/// Returns the total number of elements in the array.
///
/// ```sql
/// cardinality ( array anyarray) → int64
/// ```
///
/// Examples:
///
/// ```slt
/// query T
/// select cardinality(null::int[]);
/// ----
/// NULL
///
/// query T
/// select cardinality(array[1,2,3]);
/// ----
/// 3
///
/// query T
/// select cardinality(array[1,2,3,4,1]);
/// ----
/// 5
///
/// query T
/// select cardinality(array[array[1, 2, 3]]);
/// ----
/// 3
///
/// query T
/// select cardinality(array[array[array[3,4,5],array[2,2,2]],array[array[6,7,8],array[0,0,0]]]);
/// ----
/// 12
///
/// query T
/// select cardinality(array[NULL]);
/// ----
/// 1
///
/// query error unknown type
/// select cardinality(null);
/// ```
#[function("cardinality(list) -> int64")]
fn cardinality(array: ListRef<'_>) -> i64 {
    array.flatten().len() as _
}
