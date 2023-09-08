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

use auto_enums::auto_enum;
use risingwave_common::array::ListRef;
use risingwave_common::types::ScalarRefImpl;
use risingwave_expr_macro::function;

use super::*;

/// ```slt
/// query I
/// SELECT generate_subscripts(ARRAY['foo', 'bar', null], -1, false) AS s;
/// ----
///
/// query I
/// SELECT generate_subscripts(ARRAY['foo', 'bar', null], 0, false) AS s;
/// ----
///
/// query I
/// SELECT generate_subscripts(ARRAY['foo', 'bar', null], 1, true) AS s;
/// ----
/// 3
/// 2
/// 1
///
/// query I
/// SELECT generate_subscripts(ARRAY['foo', 'bar', null], 1, false) AS s;
/// ----
/// 1
/// 2
/// 3
///
/// query I
/// SELECT generate_subscripts(ARRAY[ARRAY['foo'], ARRAY['bar'], ARRAY[null]], 0, false) AS s;
/// ----
///
/// query I
/// SELECT generate_subscripts(ARRAY[ARRAY['foo'], ARRAY['bar'], ARRAY[null]], 1, false) AS s;
/// ----
/// 1
/// 2
/// 3
///
/// query I
/// SELECT generate_subscripts(ARRAY[ARRAY['foo'], ARRAY['bar'], ARRAY[null]], 2, false) AS s;
/// ----
/// 1
/// ```
#[function("generate_subscripts(anyarray, int32, boolean) -> setof int32")]
fn generate_subscripts_reverse(
    array: ListRef<'_>,
    dim: i32,
    reverse: bool,
) -> impl Iterator<Item = i32> {
    generate_subscripts_iterator(array, dim, reverse)
}

/// ```slt
/// query I
/// SELECT generate_subscripts(ARRAY['foo', 'bar', null], -1, false) AS s;
/// ----
///
/// query I
/// SELECT generate_subscripts(ARRAY['foo', 'bar', null], 0, false) AS s;
/// ----
///
/// query I
/// SELECT generate_subscripts(ARRAY['foo', 'bar', null], 1) AS s;
/// ----
/// 1
/// 2
/// 3
///
/// query I
/// SELECT generate_subscripts(ARRAY['foo', 'bar', null], 1) AS s;
/// ----
/// 1
/// 2
/// 3
///
/// query I
/// SELECT generate_subscripts(ARRAY[ARRAY['foo'], ARRAY['bar'], ARRAY[null]], 0) AS s;
/// ----
///
/// query I
/// SELECT generate_subscripts(ARRAY[ARRAY['foo'], ARRAY['bar'], ARRAY[null]], 1) AS s;
/// ----
/// 1
/// 2
/// 3
///
/// query I
/// SELECT generate_subscripts(ARRAY[ARRAY['foo'], ARRAY['bar'], ARRAY[null]], 2) AS s;
/// ----
/// 1
/// ```
#[function("generate_subscripts(anyarray, int32) -> setof int32")]
fn generate_subscripts(array: ListRef<'_>, dim: i32) -> impl Iterator<Item = i32> {
    generate_subscripts_iterator(array, dim, false)
}

#[auto_enum(Iterator)]
fn generate_subscripts_iterator(
    array: ListRef<'_>,
    dim: i32,
    reverse: bool,
) -> impl Iterator<Item = i32> {
    let (cur, end) = generate_subscripts_inner(array, dim);

    if reverse {
        return (cur..end).rev();
    } else {
        return cur..end;
    }
}

fn generate_subscripts_inner(array: ListRef<'_>, dim: i32) -> (i32, i32) {
    let nothing = (0, 0);
    match dim {
        ..=0 => nothing,
        1 => (1, array.len() as i32 + 1),
        // Although RW's array can be zig-zag, we just look at the first element.
        2.. => match array.elem_at(0) {
            Some(Some(ScalarRefImpl::List(list))) => generate_subscripts_inner(list, dim - 1),
            _ => nothing,
        },
    }
}
