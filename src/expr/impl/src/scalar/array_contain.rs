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

//! Range expression functions.

use std::collections::HashSet;

use risingwave_common::types::ListRef;
use risingwave_expr::function;

/// Returns whether left range contains right range.
///
/// Examples:
///
/// ```slt
/// query I
/// select array[1,2,3] @> array[2,3];
/// ----
/// t
///
/// query I
/// select array[1,2,3] @> array[3,4];
/// ----
/// f
///
/// query I
/// SELECT array[1,2,3] @> array[3,1];
/// ----
/// t
///
/// query I
/// SELECT array[1,2] @> array[1,1];
/// ----
/// t
///
/// query I
/// SELECT array[1,2,3] @> array[]::int[];
/// ----
/// t
///
/// query I
/// SELECT ARRAY[]::int[] @> ARRAY[]::int[];
/// ----
/// t
///
/// query I
/// select array[[[1,2],[3,4]],[[5,6],[7,8]]] @> array[2,3];
/// ----
/// t
///
/// query I
/// select array[1,2,3] @> null;
/// ----
/// NULL
///
/// query I
/// select null @> array[3,4];
/// ----
/// NULL
///
/// query I
/// select array[1,null,2] @> array[1,null,2];
/// ----
/// f
///
/// query I
/// select array[1,null,2] @> array[1,2];
/// ----
/// t
///
/// query I
/// SELECT array[1,NULL,2] @> array[NULL]::int[];
/// ----
/// f
///
/// query I
/// SELECT NULL::int[] @> ARRAY[1];
/// ----
/// NULL
/// ```
#[expect(
    clippy::mutable_key_type,
    reason = "array containment hashes immutable scalar refs for membership checks"
)]
fn array_contains_impl(left: ListRef<'_>, right: ListRef<'_>) -> bool {
    let flatten = left.flatten();
    let set: HashSet<_> = flatten.iter().collect();
    right
        .flatten()
        .iter()
        .all(|item| item.is_some_and(|v| set.contains(&Some(v))))
}

#[function("array_contains(anyarray, anyarray) -> boolean")]
fn array_contains(left: ListRef<'_>, right: ListRef<'_>) -> bool {
    array_contains_impl(left, right)
}

#[function("array_contained(anyarray, anyarray) -> boolean")]
fn array_contained(left: ListRef<'_>, right: ListRef<'_>) -> bool {
    array_contains_impl(right, left)
}

#[expect(
    clippy::mutable_key_type,
    reason = "array overlap hashes immutable scalar refs for membership checks"
)]
fn array_overlaps_impl(left: ListRef<'_>, right: ListRef<'_>) -> bool {
    let flatten = left.flatten();
    let set: HashSet<_> = flatten.iter().flatten().collect();
    right
        .flatten()
        .iter()
        .any(|item| item.is_some_and(|v| set.contains(&v)))
}

#[function("array_overlaps(anyarray, anyarray) -> boolean")]
fn array_overlaps(left: ListRef<'_>, right: ListRef<'_>) -> bool {
    array_overlaps_impl(left, right)
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{DataType, ListValue, Scalar, ScalarImpl};

    use super::*;

    #[test]
    fn test_contains() {
        assert!(array_contains_impl(
            ListValue::from_iter([2, 3]).as_scalar_ref(),
            ListValue::from_iter([2]).as_scalar_ref(),
        ));
        assert!(!array_contains_impl(
            ListValue::from_iter([2, 3]).as_scalar_ref(),
            ListValue::from_iter([5]).as_scalar_ref(),
        ));
    }

    #[test]
    fn test_overlaps() {
        assert!(array_overlaps_impl(
            ListValue::from_iter([2, 3]).as_scalar_ref(),
            ListValue::from_iter([3, 5]).as_scalar_ref(),
        ));
        assert!(!array_overlaps_impl(
            ListValue::from_iter([2, 3]).as_scalar_ref(),
            ListValue::from_iter([4, 5]).as_scalar_ref(),
        ));
        assert!(!array_overlaps_impl(
            ListValue::from_datum_iter(
                &DataType::Int32,
                [Some(ScalarImpl::Int32(1)), None::<ScalarImpl>],
            )
            .as_scalar_ref(),
            ListValue::from_datum_iter(
                &DataType::Int32,
                [None::<ScalarImpl>, Some(ScalarImpl::Int32(2))],
            )
            .as_scalar_ref(),
        ));
        assert!(!array_overlaps_impl(
            ListValue::from_datum_iter(&DataType::Int32, [None::<ScalarImpl>]).as_scalar_ref(),
            ListValue::from_datum_iter(&DataType::Int32, [None::<ScalarImpl>]).as_scalar_ref(),
        ));
    }
}
