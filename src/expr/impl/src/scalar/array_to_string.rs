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

#![allow(clippy::unit_arg)]

use risingwave_common::array::*;
use risingwave_common::types::ToText;
use risingwave_expr::expr::Context;
use risingwave_expr::function;

/// Converts each array element to its text representation, and concatenates those
/// separated by the delimiter string. If `null_string` is given and is not NULL,
/// then NULL array entries are represented by that string; otherwise, they are omitted.
///
/// ```sql
/// array_to_string ( array anyarray, delimiter text [, null_string text ] ) → text
/// ```
///
/// Examples:
///
/// ```slt
/// query T
/// select array_to_string(array[1, 2, 3, NULL, 5], ',')
/// ----
/// 1,2,3,5
///
/// query T
/// select array_to_string(array[1, 2, 3, NULL, 5], ',', '*')
/// ----
/// 1,2,3,*,5
///
/// query T
/// select array_to_string(array[null,'foo',null], ',', '*');
/// ----
/// *,foo,*
///
/// query T
/// select array_to_string(array['2023-02-20 17:35:25'::timestamp, null,'2023-02-19 13:01:30'::timestamp], ',', '*');
/// ----
/// 2023-02-20 17:35:25,*,2023-02-19 13:01:30
///
/// query T
/// with t as (
///   select array[1,null,2,3] as arr, ',' as d union all
///   select array[4,5,6,null,7] as arr, '|')
/// select array_to_string(arr, d) from t;
/// ----
/// 1,2,3
/// 4|5|6|7
///
/// # `array` or `delimiter` are required. Otherwise, returns null.
/// query T
/// select array_to_string(array[1,2], NULL);
/// ----
/// NULL
///
/// query error polymorphic type
/// select array_to_string(null, ',');
///
/// # multidimensional array
/// query T
/// select array_to_string(array[array['one', null], array['three', 'four']]::text[][], ',');
/// ----
/// one,three,four
///
/// query T
/// select array_to_string(array[array['one', null], array['three', 'four']]::text[][], ',', '*');
/// ----
/// one,*,three,four
/// ```
#[function("array_to_string(anyarray, varchar) -> varchar")]
fn array_to_string(
    array: ListRef<'_>,
    delimiter: &str,
    ctx: &Context,
    writer: &mut impl std::fmt::Write,
) {
    let element_data_type = ctx.arg_types[0].unnest_list();
    let mut first = true;
    for element in array.flatten().iter() {
        let Some(element) = element else { continue };
        if !first {
            write!(writer, "{}", delimiter).unwrap();
        } else {
            first = false;
        }
        element.write_with_type(element_data_type, writer).unwrap();
    }
}

#[function("array_to_string(anyarray, varchar, varchar) -> varchar")]
fn array_to_string_with_null(
    array: ListRef<'_>,
    delimiter: &str,
    null_string: &str,
    ctx: &Context,
    writer: &mut impl std::fmt::Write,
) {
    let element_data_type = ctx.arg_types[0].unnest_list();
    let mut first = true;
    for element in array.flatten().iter() {
        if !first {
            write!(writer, "{}", delimiter).unwrap();
        } else {
            first = false;
        }
        match element {
            Some(s) => s.write_with_type(element_data_type, writer).unwrap(),
            None => write!(writer, "{}", null_string).unwrap(),
        }
    }
}
