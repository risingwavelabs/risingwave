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
#[function("generate_subscripts(list, int32, boolean) -> setof int32")]
fn generate_subscripts(
    array: ListRef<'_>,
    dim: i32,
    reverse: bool,
) -> Result<impl Iterator<Item = i32>> {
    let (mut cur, end) = generate_subscripts_inner(array, dim, reverse);

    // if we don't branch reverse inside, then we have to box it outside to have the same type
    // different closures different anonymous types, or range and reverse range.
    let mut next = move || {
        if reverse {
            if cur <= end {
                return None;
            }
        } else {
            if cur >= end {
                return None;
            }
        }
        let ret = cur;
        cur += if reverse { -1 } else { 1 };
        Some(ret)
    };
    Ok(std::iter::from_fn(move || next()))
}

fn generate_subscripts_inner(array: ListRef<'_>, dim: i32, reverse: bool) -> (i32, i32) {
    let nothing = (0, 0);
    match dim {
        ..=0 => nothing,
        1 => {
            if reverse {
                (array.len() as i32, 0)
            } else {
                (1, array.len() as i32 + 1)
            }
        }
        // Although RW's array can be zig-zag, we just look at the first element.
        2.. => match array.elem_at(0) {
            Some(datum_ref) => match datum_ref {
                Some(scalar_ref) => match scalar_ref {
                    ScalarRefImpl::List(list) => generate_subscripts_inner(list, dim - 1, reverse),
                    _ => nothing,
                },
                None => nothing,
            },
            None => nothing,
        },
    }
}
