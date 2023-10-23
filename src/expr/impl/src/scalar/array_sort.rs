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

use risingwave_common::array::*;
use risingwave_common::types::{DatumRef, DefaultOrdered, ToOwnedDatum};
use risingwave_expr::function;

#[function("array_sort(anyarray) -> anyarray")]
pub fn array_sort(list: ListRef<'_>) -> ListValue {
    let mut v = list
        .iter()
        .map(DefaultOrdered)
        .collect::<Vec<DefaultOrdered<DatumRef<'_>>>>();
    v.sort();
    ListValue::new(v.into_iter().map(|x| x.0.to_owned_datum()).collect())
}
