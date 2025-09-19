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

use itertools::Itertools;
use risingwave_common::array::*;
use risingwave_expr::expr::Context;
use risingwave_expr::function;

/// Returns a new array removing all the duplicates from the input array
///
/// ```sql
/// array_distinct (array anyarray) → array
/// ```
///
/// Examples:
///
/// ```slt
/// query T
/// select array_distinct(array[NULL]);
/// ----
/// {NULL}
///
/// query T
/// select array_distinct(array[1,2,1,1]);
/// ----
/// {1,2}
///
/// query T
/// select array_distinct(array[1,2,1,NULL]);
/// ----
/// {1,2,NULL}
///
/// query T
/// select array_distinct(null::int[]);
/// ----
/// NULL
///
/// query error polymorphic type
/// select array_distinct(null);
/// ```

#[function("array_distinct(anyarray) -> anyarray")]
pub fn array_distinct(list: ListRef<'_>, ctx: &Context) -> ListValue {
    ListValue::from_datum_iter(ctx.arg_types[0].as_list_elem(), list.iter().unique())
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{DataType, Scalar};

    use super::*;

    #[test]
    fn test_array_distinct_array_of_primitives() {
        let array = ListValue::from_iter([42, 43, 42]);
        let expected = ListValue::from_iter([42, 43]);
        let actual = array_distinct(
            array.as_scalar_ref(),
            &Context {
                arg_types: vec![DataType::Int32.list()],
                return_type: DataType::Int32.list(),
                variadic: false,
            },
        );
        assert_eq!(actual, expected);
    }

    // More test cases are in e2e tests.
}
