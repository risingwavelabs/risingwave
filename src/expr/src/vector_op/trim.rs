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

#[function("trim(varchar) -> varchar")]
pub fn trim(s: &str, writer: &mut dyn Write) {
    writer.write_str(s.trim()).unwrap();
}

/// Note: the behavior of `ltrim` in `PostgreSQL` and `trim_start` (or `trim_left`) in Rust
/// are actually different when the string is in right-to-left languages like Arabic or Hebrew.
/// Since we would like to simplify the implementation, currently we omit this case.
#[function("ltrim(varchar) -> varchar")]
pub fn ltrim(s: &str, writer: &mut dyn Write) {
    writer.write_str(s.trim_start()).unwrap();
}

/// Note: the behavior of `rtrim` in `PostgreSQL` and `trim_end` (or `trim_right`) in Rust
/// are actually different when the string is in right-to-left languages like Arabic or Hebrew.
/// Since we would like to simplify the implementation, currently we omit this case.
#[function("rtrim(varchar) -> varchar")]
pub fn rtrim(s: &str, writer: &mut dyn Write) {
    writer.write_str(s.trim_end()).unwrap();
}

#[function("trim(varchar, varchar) -> varchar")]
pub fn trim_characters(s: &str, characters: &str, writer: &mut dyn Write) {
    let pattern = |c| characters.chars().any(|ch| ch == c);
    // We remark that feeding a &str and a slice of chars into trim_left/right_matches
    // means different, one is matching with the entire string and the other one is matching
    // with any char in the slice.
    writer.write_str(s.trim_matches(pattern)).unwrap();
}

#[function("ltrim(varchar, varchar) -> varchar")]
pub fn ltrim_characters(s: &str, characters: &str, writer: &mut dyn Write) {
    let pattern = |c| characters.chars().any(|ch| ch == c);
    writer.write_str(s.trim_start_matches(pattern)).unwrap();
}

#[function("rtrim(varchar, varchar) -> varchar")]
pub fn rtrim_characters(s: &str, characters: &str, writer: &mut dyn Write) {
    let pattern = |c| characters.chars().any(|ch| ch == c);
    writer.write_str(s.trim_end_matches(pattern)).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trim() {
        let cases = [
            (" Hello\tworld\t", "Hello\tworld"),
            (" 空I ❤️ databases空 ", "空I ❤️ databases空"),
        ];

        for (s, expected) in cases {
            let mut writer = String::new();
            trim(s, &mut writer);
            assert_eq!(writer, expected);
        }
    }

    #[test]
    fn test_ltrim() {
        let cases = [
            (" \tHello\tworld\t", "Hello\tworld\t"),
            (" \t空I ❤️ databases空 ", "空I ❤️ databases空 "),
        ];

        for (s, expected) in cases {
            let mut writer = String::new();
            ltrim(s, &mut writer);
            assert_eq!(writer, expected);
        }
    }

    #[test]
    fn test_rtrim() {
        let cases = [
            (" \tHello\tworld\t ", " \tHello\tworld"),
            (" \t空I ❤️ databases空\t ", " \t空I ❤️ databases空"),
        ];

        for (s, expected) in cases {
            let mut writer = String::new();
            rtrim(s, &mut writer);
            assert_eq!(writer, expected);
        }
    }

    #[test]
    fn test_trim_characters() {
        let cases = [
            ("Hello world", "Hdl", "ello wor"),
            ("abcde", "aae", "bcd"),
            ("zxy", "yxz", ""),
        ];

        for (s, characters, expected) in cases {
            let mut writer = String::new();
            trim_characters(s, characters, &mut writer);
            assert_eq!(writer, expected);
        }
    }

    #[test]
    fn test_ltrim_characters() {
        let cases = [
            ("Hello world", "Hdl", "ello world"),
            ("abcde", "aae", "bcde"),
            ("zxy", "yxz", ""),
        ];

        for (s, characters, expected) in cases {
            let mut writer = String::new();
            ltrim_characters(s, characters, &mut writer);
            assert_eq!(writer, expected);
        }
    }

    #[test]
    fn test_rtrim_characters() {
        let cases = [
            ("Hello world", "Hdl", "Hello wor"),
            ("abcde", "aae", "abcd"),
            ("zxy", "yxz", ""),
        ];

        for (s, characters, expected) in cases {
            let mut writer = String::new();
            rtrim_characters(s, characters, &mut writer);
            assert_eq!(writer, expected);
        }
    }
}
