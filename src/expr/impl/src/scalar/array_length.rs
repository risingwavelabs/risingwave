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

use std::fmt::Write;

use risingwave_common::array::ListRef;
use risingwave_expr::{ExprError, Result, function};

/// Returns the length of an array.
///
/// ```sql
/// array_length (array anyarray) → int8
/// ```
///
/// Examples:
///
/// ```slt
/// query T
/// select array_length(null::int[]);
/// ----
/// NULL
///
/// query T
/// select array_length(array[1,2,3]);
/// ----
/// 3
///
/// query T
/// select array_length(array[1,2,3,4,1]);
/// ----
/// 5
///
/// query T
/// select array_length(array[]::int[]);
/// ----
/// 0
///
/// query T
/// select array_length(array[array[1, 2, 3]]);
/// ----
/// 1
///
/// query T
/// select array_length(array[NULL]);
/// ----
/// 1
///
/// query error Cannot implicitly cast
/// select array_length(null);
/// ```
#[function("array_length(anyarray) -> int4")]
#[function("array_length(anyarray) -> int8", deprecated)]
fn array_length<T: TryFrom<usize>>(array: ListRef<'_>) -> Result<T> {
    array
        .len()
        .try_into()
        .map_err(|_| ExprError::NumericOverflow)
}

/// Returns the length of the requested array dimension.
///
/// Examples:
///
/// ```slt
/// query I
/// select array_length(array[2,3,4], 1);
/// ----
/// 3
///
/// query I
/// select array_length(array[2,3,4], 0);
/// ----
/// NULL
///
/// query I
/// select array_length(array[2,3,4], -1);
/// ----
/// NULL
///
/// query I
/// select array_length(array[2,3,4], null);
/// ----
/// NULL
///
/// query I
/// select array_length(array[array[2,3,4],array[3,4,5]], '1');
/// ----
/// 2
///
/// statement error
/// select array_length(array[2,3,4], true);
///
/// # This one could be supported later, but at the cost of checking all subarrays, to reject the next.
/// statement error
/// select array_length(array[array[2,3,4],array[3,4,5]], 2);
///
/// statement error
/// select array_length(array[array[2,3],array[3,4,5]], 2);
///
/// # Different from PostgreSQL who treats empty `array[]` as zero dimension and returns NULL.
/// query I
/// select array_length(array[]::int[], 1);
/// ----
/// 0
///
/// query I
/// select array_length(array[]::int[][], 1);
/// ----
/// 0
///
/// # This should be NULL but it is hard to access `DataType` in current expression framework.
/// # The next should remain rejected.
/// statement error
/// select array_length(array[1,2,3], 2);
///
/// statement error
/// select array_length(array[null, array[2]], 2);
/// ```
#[function("array_length(anyarray, int4) -> int4")]
fn array_length_of_dim(array: ListRef<'_>, d: i32) -> Result<Option<i32>> {
    match d {
        ..=0 => Ok(None),
        1 => array_length(array).map(Some),
        2.. => Err(ExprError::InvalidParam {
            name: "dimension",
            reason: "array_length for dimensions greater than 1 not supported".into(),
        }),
    }
}

/// Returns a text representation of the array's dimensions.
///
/// Examples:
///
/// ```slt
/// query T
/// select array_dims(array[2,3,4]);
/// ----
/// [1:3]
///
/// query T
/// select array_dims(null::int[]);
/// ----
/// NULL
///
/// query T
/// select array_dims('{2,3,4}'::int[]);
/// ----
/// [1:3]
///
/// statement error
/// select array_dims(null);
///
/// statement error
/// select array_dims(1);
///
/// # Similar to `array_length`, higher dimension is rejected now but can be supported later in limited cases.
/// statement error
/// select array_dims(array[array[2,3,4],array[3,4,5]]);
///
/// statement error
/// select array_dims(array[array[2,3],array[3,4,5]]);
///
/// # And empty array is also different from PostgreSQL, following the same convention as `array_length`.
/// query T
/// select array_dims(array[]::int[]);
/// ----
/// [1:0]
///
/// statement error
/// select array_dims(array[]::int[][]); -- would be `[1:0][1:0]` after multidimensional support
///
/// statement error
/// select array_dims(array[array[]::int[]]); -- would be `[1:1][1:0]` after multidimensional support
/// ```
#[function("array_dims(anyarray) -> varchar")]
fn array_dims(array: ListRef<'_>, writer: &mut impl Write) {
    write!(writer, "[1:{}]", array.len()).unwrap();
}
