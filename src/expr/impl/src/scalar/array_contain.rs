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
/// select array[1,2,3] @> NULL;
/// ----
/// NULL
///
/// query I
/// select NULL @> array[3,4];
/// ----
/// NULL
/// ```
#[function("array_contains(anyarray, anyarray) -> boolean")]
fn array_contains(left: ListRef<'_>, right: ListRef<'_>) -> bool {
    let set: HashSet<_> = left.iter().collect();
    right.iter().all(|item| set.contains(&item))
}

#[function("array_contained(anyarray, anyarray) -> boolean")]
fn array_contained(left: ListRef<'_>, right: ListRef<'_>) -> bool {
    array_contains(right, left)
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{ListValue, ScalarImpl};

    use super::*;

    #[test]
    fn test_contains() {
        assert!(array_contains(
            ListRef::ValueRef {
                val: &ListValue::new(vec![Some(ScalarImpl::Int32(2)), Some(ScalarImpl::Int32(3))]),
            },
            ListRef::ValueRef {
                val: &ListValue::new(vec![Some(ScalarImpl::Int32(2))]),
            }
        ));
        assert!(!array_contains(
            ListRef::ValueRef {
                val: &ListValue::new(vec![Some(ScalarImpl::Int32(2)), Some(ScalarImpl::Int32(3))]),
            },
            ListRef::ValueRef {
                val: &ListValue::new(vec![Some(ScalarImpl::Int32(5))]),
            }
        ));
    }
}
