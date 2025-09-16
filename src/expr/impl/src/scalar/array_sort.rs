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
use risingwave_common::types::DefaultOrdered;
use risingwave_expr::expr::Context;
use risingwave_expr::function;

#[function("array_sort(anyarray) -> anyarray")]
pub fn array_sort(array: ListRef<'_>, ctx: &Context) -> ListValue {
    ListValue::from_datum_iter(
        ctx.arg_types[0].as_list_elem(),
        array.iter().map(DefaultOrdered).sorted().map(|v| v.0),
    )
}
