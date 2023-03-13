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

use std::fmt::Write;

use risingwave_common::array::*;
use risingwave_common::types::to_text::ToText;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr_macro::function;

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
/// ```

#[function("array_to_string(list, varchar) -> varchar")]
fn array_to_string(
    array: ListRef<'_>,
    element_data_type: &DataType,
    delimiter: &str,
    mut writer: &mut dyn Write,
) {
    let mut first = true;
    for element in array.values_ref().iter().flat_map(|f| f.iter()) {
        if !first {
            write!(writer, "{}", delimiter).unwrap();
        } else {
            first = false;
        }
        element
            .write_with_type(element_data_type, &mut writer)
            .unwrap();
    }
}

#[function("array_to_string(list, varchar, varchar) -> varchar")]
fn array_to_string_with_null(
    array: ListRef<'_>,
    element_data_type: &DataType,
    delimiter: &str,
    null_string: &str,
    mut writer: &mut dyn Write,
) {
    let mut first = true;
    for element in array.values_ref() {
        if !first {
            write!(writer, "{}", delimiter).unwrap();
        } else {
            first = false;
        }
        match element {
            Some(s) => s.write_with_type(element_data_type, &mut writer).unwrap(),
            None => write!(writer, "{}", null_string).unwrap(),
        }
    }
}
