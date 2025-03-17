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

use risingwave_common::array::{ListValue, Utf8Array};
use risingwave_expr::function;

use crate::scalar::regexp::RegexpContext;

#[function(
    "regexp_matches(varchar, varchar) -> setof varchar[]",
    prebuild = "RegexpContext::from_pattern($1)?"
)]
#[function(
    "regexp_matches(varchar, varchar, varchar) -> setof varchar[]",
    prebuild = "RegexpContext::from_pattern_flags($1, $2)?"
)]
fn regexp_matches<'a>(
    text: &'a str,
    regex: &'a RegexpContext,
) -> impl Iterator<Item = ListValue> + 'a {
    regex.regex.captures_iter(text).map(|capture| {
        // If there are multiple captures, then the first one is the whole match, and should be
        // ignored in PostgreSQL's behavior.
        let skip_flag = regex.regex.captures_len() > 1;
        let list = capture
            .unwrap()
            .iter()
            .skip(if skip_flag { 1 } else { 0 })
            .map(|mat| mat.map(|m| m.as_str()))
            .collect::<Utf8Array>();
        ListValue::new(list.into())
    })
}
