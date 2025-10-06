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

use std::collections::HashSet;

use risingwave_common::array::{ListRef, ListValue};
use risingwave_expr::function;

/// Returns the intersection of two arrays, i.e., elements that appear in both arrays.
/// The result preserves the order of elements from the first array and removes duplicates.
///
/// ```sql
/// array_intersect (array1 anyarray, array2 anyarray) → array
/// ```
///
/// Examples:
///
/// ```slt
/// query T
/// select array_intersect(array[1,2,3], array[2,3,4]);
/// ----
/// {2,3}
///
/// query T
/// select array_intersect(array[1,2,2,3], array[2,3,4]);
/// ----
/// {2,3}
///
/// query T
/// select array_intersect(array[1,2,3], array[4,5,6]);
/// ----
/// {}
///
/// query T
/// select array_intersect(array[1,2,NULL], array[2,NULL,4]);
/// ----
/// {2,NULL}
///
/// query T
/// select array_intersect(null::int[], array[1,2,3]);
/// ----
/// NULL
///
/// query T
/// select array_intersect(array[1,2,3], null::int[]);
/// ----
/// NULL
/// ```

#[function("array_intersect(anyarray, anyarray) -> anyarray")]
fn array_intersect(left: ListRef<'_>, right: ListRef<'_>) -> ListValue {
    let right_set: HashSet<_> = right.iter().collect();
    let mut seen = HashSet::new();
    let mut result = Vec::new();

    for item in left.iter() {
        if right_set.contains(&item) && !seen.contains(&item) {
            result.push(item);
            seen.insert(item);
        }
    }

    ListValue::from_datum_iter(&left.elem_type(), result.into_iter())
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{ListValue, Scalar};

    use super::*;

    #[test]
    fn test_array_intersect_basic() {
        let left = ListValue::from_iter([1, 2, 3]);
        let right = ListValue::from_iter([2, 3, 4]);
        let expected = ListValue::from_iter([2, 3]);
        assert_eq!(
            array_intersect(left.as_scalar_ref(), right.as_scalar_ref()),
            expected
        );
    }

    #[test]
    fn test_array_intersect_with_duplicates() {
        let left = ListValue::from_iter([1, 2, 2, 3]);
        let right = ListValue::from_iter([2, 3, 4]);
        let expected = ListValue::from_iter([2, 3]);
        assert_eq!(
            array_intersect(left.as_scalar_ref(), right.as_scalar_ref()),
            expected
        );
    }

    #[test]
    fn test_array_intersect_with_nulls() {
        let left = ListValue::from_iter([Some(1), None, Some(2)]);
        let right = ListValue::from_iter([None, Some(2), Some(3)]);
        let expected = ListValue::from_iter([None, Some(2)]);
        assert_eq!(
            array_intersect(left.as_scalar_ref(), right.as_scalar_ref()),
            expected
        );
    }

    #[test]
    fn test_array_intersect_no_intersection() {
        let left = ListValue::from_iter([1, 2, 3]);
        let right = ListValue::from_iter([4, 5, 6]);
        let expected = ListValue::from_iter(std::iter::empty::<i32>());
        assert_eq!(
            array_intersect(left.as_scalar_ref(), right.as_scalar_ref()),
            expected
        );
    }
}
