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

use risingwave_expr_macro::function;

#[function("replace(varchar, varchar, varchar) -> varchar")]
pub fn replace(s: &str, from_str: &str, to_str: &str, writer: &mut dyn Write) {
    if from_str.is_empty() {
        writer.write_str(s).unwrap();
        return;
    }
    let mut last = 0;
    while let Some(mut start) = s[last..].find(from_str) {
        start += last;
        writer.write_str(&s[last..start]).unwrap();
        writer.write_str(to_str).unwrap();
        last = start + from_str.len();
    }
    writer.write_str(&s[last..]).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replace() {
        let cases = vec![
            ("hello, word", "我的", "world", "hello, word"),
            ("hello, word", "", "world", "hello, word"),
            ("hello, word", "word", "world", "hello, world"),
            ("hello, world", "world", "", "hello, "),
            ("你是❤️，是暖，是希望", "是", "非", "你非❤️，非暖，非希望"),
            ("👴笑了", "👴", "爷爷", "爷爷笑了"),
        ];

        for (s, from_str, to_str, expected) in cases {
            let mut writer = String::new();
            replace(s, from_str, to_str, &mut writer);
            assert_eq!(writer, expected);
        }
    }
}
