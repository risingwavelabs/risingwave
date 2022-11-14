// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use risingwave_common::array::{BytesGuard, BytesWriter};

use crate::Result;

#[inline(always)]
pub fn translate(
    s: &str,
    match_str: &str,
    replace_str: &str,
    writer: BytesWriter,
) -> Result<BytesGuard> {
    let mut char_map = HashMap::new();
    let mut match_chars = match_str.chars();
    let mut replace_chars = replace_str.chars();

    loop {
        let m = match_chars.next();
        let r = replace_chars.next();
        if let Some(match_c) = m {
            char_map.entry(match_c).or_insert(r);
        } else {
            break;
        }
    }

    let iter = s.chars().filter_map(|c| match char_map.get(&c) {
        Some(Some(m)) => Some(*m),
        Some(None) => None,
        None => Some(c),
    });

    writer.write_from_char_iter(iter).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, ArrayBuilder, Utf8ArrayBuilder};

    use super::*;

    #[test]
    fn test_translate() -> Result<()> {
        let cases = [
            ("hello world", "lo", "12", "he112 w2r1d"),
            (
                "人之初，性本善。性相近，习相远。",
                "人性。",
                "abcd",
                "a之初，b本善cb相近，习相远c",
            ),
            (
                "奇点无限 Singularity Data",
                "Data ",
                "1234",
                "奇点无限Singul2ri3y1232",
            ),
        ];

        for (s, match_str, replace_str, expected) in cases {
            let builder = Utf8ArrayBuilder::new(1);
            let writer = builder.writer();
            let guard = translate(s, match_str, replace_str, writer)?;
            let array = guard.into_inner().finish();
            let v = array.value_at(0).unwrap();
            assert_eq!(v, expected);
        }

        Ok(())
    }
}
