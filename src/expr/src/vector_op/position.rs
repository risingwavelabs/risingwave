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

/// Returns the index of the first occurrence of the specified substring in the input string,
/// or zero if the substring is not present.
///
/// # Example
///
/// ```slt
/// query I
/// select position('om' in 'Thomas');
/// ----
/// 3
///
/// query I
/// select strpos('hello, world', 'lo');
/// ----
/// 4
///
/// query I
/// select strpos('high', 'ig');
/// ----
/// 2
///
/// query I
/// select strpos('abc', 'def');
/// ----
/// 0
///
/// query I
/// select strpos('床前明月光', '月光');
/// ----
/// 4
/// ```
#[function("strpos(varchar, varchar) -> int32")] // backward compatibility with old proto
#[function("position(varchar, varchar) -> int32")]
pub fn position(str: &str, sub_str: &str) -> i32 {
    match str.find(sub_str) {
        Some(byte_idx) => (str[..byte_idx].chars().count() + 1) as i32,
        None => 0,
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_length() {
        let cases = [
            ("hello world", "world", 7),
            ("床前明月光", "月光", 4),
            ("床前明月光", "故乡", 0),
        ];

        for (str, sub_str, expected) in cases {
            assert_eq!(position(str, sub_str), expected)
        }
    }
}
