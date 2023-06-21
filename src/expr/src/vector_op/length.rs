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

use risingwave_expr_macro::function;

#[function("length(varchar) -> int32")]
#[function("char_length(varchar) -> int32")]
pub fn length(s: &str) -> i32 {
    s.chars().count() as i32
}

#[function("octet_length(varchar) -> int32")]
pub fn octet_length_str(s: &str) -> i32 {
    s.as_bytes().len() as i32
}

#[function("length(bytea) -> int32")]
#[function("octet_length(bytea) -> int32")]
pub fn octet_length_bytea(s: &[u8]) -> i32 {
    s.len() as i32
}

#[function("bit_length(varchar) -> int32")]
pub fn bit_length_str(s: &str) -> i32 {
    octet_length_str(s) * 8
}

#[function("bit_length(bytea) -> int32")]
pub fn bit_length_bytea(s: &[u8]) -> i32 {
    octet_length_bytea(s) * 8
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_length() {
        let cases = [("hello world", 11), ("hello rust", 10)];

        for (s, expected) in cases {
            assert_eq!(length(s), expected);
        }
    }

    #[test]
    fn test_octet_length() {
        let cases = [("hello world", 11), ("你好", 6), ("😇哈哈hhh", 13)];

        for (s, expected) in cases {
            assert_eq!(octet_length_str(s), expected);
        }
    }

    #[test]
    fn test_bit_length() {
        let cases = [
            ("hello world", 11 * 8),
            ("你好", 6 * 8),
            ("😇哈哈hhh", 13 * 8),
        ];

        for (s, expected) in cases {
            assert_eq!(bit_length_str(s), expected);
        }
    }
}
