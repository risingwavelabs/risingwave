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
use risingwave_common::array::{ListValue, Utf8Array};
use risingwave_expr::function;

#[auto_enum(Iterator)]
fn string_to_array_inner<'a>(s: &'a str, sep: Option<&'a str>) -> impl Iterator<Item = &'a str> {
    match s.is_empty() {
        true => std::iter::empty(),
        #[nested]
        _ => match sep {
            Some(sep) if sep.is_empty() => std::iter::once(s),
            Some(sep) => s.split(sep),
            None => s.char_indices().map(move |(index, ch)| {
                let len = ch.len_utf8();
                &s[index..index + len]
            }),
        },
    }
}

// Use cases shown in `e2e_test/batch/functions/string_to_array.slt.part`
#[function("string_to_array(varchar, varchar) -> varchar[]")]
pub fn string_to_array2(s: Option<&str>, sep: Option<&str>) -> Option<ListValue> {
    Some(ListValue::new(
        string_to_array_inner(s?, sep).collect::<Utf8Array>().into(),
    ))
}

#[function("string_to_array(varchar, varchar, varchar) -> varchar[]")]
pub fn string_to_array3(
    s: Option<&str>,
    sep: Option<&str>,
    null: Option<&str>,
) -> Option<ListValue> {
    let Some(null) = null else {
        return string_to_array2(s, sep);
    };
    Some(ListValue::new(
        string_to_array_inner(s?, sep)
            .map(|x| if x == null { None } else { Some(x) })
            .collect::<Utf8Array>()
            .into(),
    ))
}
