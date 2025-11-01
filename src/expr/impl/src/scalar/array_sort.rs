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
use risingwave_common::util::sort_util::{OrderType, cmp_datum};
use risingwave_expr::function;

#[function("array_sort(anyarray) -> anyarray")]
pub fn array_sort(array: ListRef<'_>) -> ListValue {
    array_sort_with_desc_and_nulls(array, false, false)
}

#[function("array_sort(anyarray, boolean) -> anyarray")]
pub fn array_sort_with_desc(array: ListRef<'_>, descending: bool) -> ListValue {
    array_sort_with_desc_and_nulls(array, descending, false)
}

#[function("array_sort(anyarray, boolean, boolean) -> anyarray")]
pub fn array_sort_with_desc_and_nulls(
    array: ListRef<'_>,
    descending: bool,
    nulls_first: bool,
) -> ListValue {
    let order = OrderType::from_bools(Some(!descending), Some(nulls_first));

    ListValue::from_datum_iter(
        &array.elem_type(),
        array.iter().sorted_by(|a, b| cmp_datum(*a, *b, order)),
    )
}
