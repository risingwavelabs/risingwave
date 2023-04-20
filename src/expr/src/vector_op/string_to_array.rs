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

use itertools::Itertools;
use risingwave_common::array::ListValue;
use risingwave_common::types::ScalarImpl;
use risingwave_expr_macro::function;

fn string_to_array_inner(s: Option<&str>, sep: Option<&str>) -> Vec<String> {
    s.map_or(vec![], |s| {
        sep.map_or(
            s.chars().map(|x| x.to_string()).collect_vec(),
            |sep| match sep.is_empty() {
                true => vec![s.to_string()],
                false => s
                    .split(sep)
                    .collect_vec()
                    .into_iter()
                    .map(|x| x.to_string())
                    .collect_vec(),
            },
        )
    })
}

// Use cases shown in `e2e_test/batch/functions/string_to_array.slt.part`
#[function("string_to_array(varchar, varchar) -> list")]
pub fn string_to_array2(s: Option<&str>, sep: Option<&str>) -> ListValue {
    ListValue::new(
        string_to_array_inner(s, sep)
            .into_iter()
            .map(|x| Some(ScalarImpl::Utf8(x.into())))
            .collect_vec(),
    )
}

#[function("string_to_array(varchar, varchar, varchar) -> list")]
pub fn string_to_array3(s: Option<&str>, sep: Option<&str>, null: Option<&str>) -> ListValue {
    null.map_or(string_to_array2(s, sep), |null| {
        ListValue::new(
            string_to_array_inner(s, sep)
                .into_iter()
                .map(|x| match x == null {
                    true => None,
                    _ => Some(ScalarImpl::Utf8(x.into())),
                })
                .collect_vec(),
        )
    })
}
