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

enum StringIterator<'a> {
    Chars(std::str::Chars<'a>),
    Split(std::str::Split<'a, &'a str>),
    Once(std::iter::Once<String>),
}

impl<'a> Iterator for StringIterator<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            StringIterator::Chars(chars) => chars.next().map(|c| c.to_string()),
            StringIterator::Split(split) => split.next().map(|s| s.to_string()),
            StringIterator::Once(once) => once.next(),
        }
    }
}

fn string_to_array_inner<'a>(s: &'a str, sep: Option<&'a str>) -> StringIterator<'a> {
    if s.is_empty() {
        return StringIterator::Chars("".chars());
    }
    sep.map_or_else(
        || StringIterator::Chars(s.chars()),
        |sep| {
            if sep.is_empty() {
                StringIterator::Once(std::iter::once(s.to_string()))
            } else {
                StringIterator::Split(s.split(sep))
            }
        },
    )
}

// Use cases shown in `e2e_test/batch/functions/string_to_array.slt.part`
#[function("string_to_array(varchar, varchar) -> list")]
pub fn string_to_array2(s: Option<&str>, sep: Option<&str>) -> Option<ListValue> {
    s.map(|s| {
        ListValue::new(
            string_to_array_inner(s, sep)
                .map(|x| Some(ScalarImpl::Utf8(x.into())))
                .collect_vec(),
        )
    })
}

#[function("string_to_array(varchar, varchar, varchar) -> list")]
pub fn string_to_array3(
    s: Option<&str>,
    sep: Option<&str>,
    null: Option<&str>,
) -> Option<ListValue> {
    s.map(|s| {
        null.map_or_else(
            || string_to_array2(Some(s), sep).unwrap(),
            |null| {
                ListValue::new(
                    string_to_array_inner(s, sep)
                        .map(|x| {
                            if x == null {
                                None
                            } else {
                                Some(ScalarImpl::Utf8(x.into()))
                            }
                        })
                        .collect_vec(),
                )
            },
        )
    })
}
