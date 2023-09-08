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
use risingwave_common::types::{Datum, ScalarRef, ScalarRefImpl};
use risingwave_expr_macro::function;

use crate::expr::Context;

/// Concatenates the two arrays.
///
/// Examples:
///
/// ```slt
/// # concat
/// query T
/// select array_cat(array[66], array[123]);
/// ----
/// {66,123}
///
/// query T
/// select array_cat(array[66], null::int[]);
/// ----
/// {66}
///
/// query T
/// select array_cat(null::int[], array[123]);
/// ----
/// {123}
///
/// query T
/// select array_cat(null::int[], null::int[]);
/// ----
/// NULL
///
/// # append
/// query T
/// select array_cat(array[array[66]], array[233]);
/// ----
/// {{66},{233}}
///
/// query T
/// select array_cat(array[array[66]], null::int[]);
/// ----
/// {{66}}
///
/// # different from PG
/// query T
/// select array_cat(null::int[][], array[233]);
/// ----
/// {{233}}
///
/// query T
/// select array_cat(null::int[][], null::int[]);
/// ----
/// NULL
///
/// # prepend
/// query T
/// select array_cat(array[233], array[array[66]]);
/// ----
/// {{233},{66}}
///
/// query T
/// select array_cat(null::int[], array[array[66]]);
/// ----
/// {{66}}
///
/// # different from PG
/// query T
/// select array_cat(array[233], null::int[][]);
/// ----
/// {{233}}
///
/// query T
/// select array_cat(null::int[], null::int[][]);
/// ----
/// NULL
/// ```
#[function("array_cat(anyarray, anyarray) -> anyarray")]
fn array_cat(
    left: Option<ListRef<'_>>,
    right: Option<ListRef<'_>>,
    ctx: &Context,
) -> Option<ListValue> {
    let elems: Vec<Datum> = if ctx.arg_types[0] == ctx.arg_types[1] {
        // array || array
        let (Some(left), Some(right)) = (left, right) else {
            return left.or(right).map(|list| list.to_owned_scalar());
        };
        left.iter()
            .chain(right.iter())
            .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
            .collect()
    } else if ctx.arg_types[0].as_list() == &ctx.arg_types[1] {
        // array[] || array
        let Some(right) = right else {
            return left.map(|left| left.to_owned_scalar());
        };
        left.iter()
            .flat_map(|list| list.iter())
            .chain([Some(right.into())])
            .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
            .collect()
    } else if &ctx.arg_types[0] == ctx.arg_types[1].as_list() {
        // array || array[]
        let Some(left) = left else {
            return right.map(|right| right.to_owned_scalar());
        };
        std::iter::once(Some(left.into()))
            .chain(right.iter().flat_map(|list| list.iter()))
            .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
            .collect()
    } else {
        unreachable!()
    };
    Some(ListValue::new(elems))
}

/// Appends a value as the back element of an array.
/// The behavior is the same as PG.
///
/// Examples:
///
/// ```slt
/// query T
/// select array_append(array[66], 123);
/// ----
/// {66,123}
///
/// query T
/// select array_append(array[66], null::int);
/// ----
/// {66,NULL}
///
/// query T
/// select array_append(null::int[], 233);
/// ----
/// {233}
///
/// query T
/// select array_append(null::int[], null::int);
/// ----
/// {NULL}
/// ```
#[function("array_append(anyarray, any) -> anyarray")]
fn array_append<'a>(left: Option<ListRef<'_>>, right: Option<ScalarRefImpl<'a>>) -> ListValue {
    ListValue::new(
        left.iter()
            .flat_map(|list| list.iter())
            .chain(std::iter::once(right))
            .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
            .collect(),
    )
}

/// Prepends a value as the front element of an array.
/// The behavior is the same as PG.
///
/// Examples:
///
/// ```slt
/// query T
/// select array_prepend(123, array[66]);
/// ----
/// {123,66}
///
/// query T
/// select array_prepend(null::int, array[66]);
/// ----
/// {NULL,66}
///
/// query T
/// select array_prepend(233, null::int[]);
/// ----
/// {233}
///
/// query T
/// select array_prepend(null::int, null::int[]);
/// ----
/// {NULL}
/// ```
#[function("array_prepend(any, anyarray) -> anyarray")]
fn array_prepend<'a>(left: Option<ScalarRefImpl<'a>>, right: Option<ListRef<'_>>) -> ListValue {
    ListValue::new(
        std::iter::once(left)
            .chain(right.iter().flat_map(|list| list.iter()))
            .map(|x| x.map(ScalarRefImpl::into_scalar_impl))
            .collect(),
    )
}
