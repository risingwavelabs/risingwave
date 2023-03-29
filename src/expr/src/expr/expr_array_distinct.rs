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

use itertools::Itertools;
use risingwave_common::array::*;
use risingwave_common::types::ScalarRefImpl;
use risingwave_expr_macro::function;

/// Returns a new array removing all the duplicates from the input array
///
/// ```sql
/// array_distinct ( array anyarray) → array
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

#[function("array_distinct(list) -> list")]
pub fn array_distinct(list: ListRef<'_>) -> ListValue {
    ListValue::new(
        list.values_ref()
            .into_iter()
            .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
            .unique()
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::Scalar;

    use super::*;

    #[test]
    fn test_array_distinct_array_of_primitives() {
        let array = ListValue::new([42, 43, 42].into_iter().map(|x| Some(x.into())).collect());
        let expected = ListValue::new([42, 43].into_iter().map(|x| Some(x.into())).collect());
        let actual = array_distinct(array.as_scalar_ref());
        assert_eq!(actual, expected);
    }

    // More test cases are in e2e tests.
}
