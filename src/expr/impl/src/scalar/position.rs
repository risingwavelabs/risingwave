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

use risingwave_expr::function;

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
#[function("strpos(varchar, varchar) -> int4", deprecated)]
#[function("position(varchar, varchar) -> int4")]
pub fn position(str: &str, sub_str: &str) -> i32 {
    match str.find(sub_str) {
        Some(byte_idx) => (str[..byte_idx].chars().count() + 1) as i32,
        None => 0,
    }
}

/// Returns the index of the first occurrence of the specified bytea substring in the input bytea,
/// or zero if the substring is not present.
///
/// # Example
///
/// ```slt
/// query I
/// select position('\x6c6f'::bytea in '\x68656c6c6f2c20776f726c64'::bytea);
/// ----
/// 4
///
/// query I
/// select position('\x6967'::bytea in '\x68696768'::bytea);
/// ----
/// 2
///
/// query I
/// select position('\x64'::bytea in '\x616263'::bytea);
/// ----
/// 0
///
/// query I
/// select position(''::bytea in '\x616263'::bytea);
/// ----
/// 1
///
/// query I
/// select position('\x616263'::bytea in ''::bytea);
/// ----
/// 0
/// ```
#[function("position(bytea, bytea) -> int4")]
pub fn bytea_position(bytea: &[u8], sub_bytea: &[u8]) -> i32 {
    if sub_bytea.is_empty() {
        return 1;
    }
    if sub_bytea.len() > bytea.len() {
        return 0;
    }
    let mut i = 0;
    while i <= bytea.len().saturating_sub(sub_bytea.len()) {
        if &bytea[i..i + sub_bytea.len()] == sub_bytea {
            return (i + 1) as i32;
        }
        i += 1;
    }
    0
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

    #[test]
    fn test_bytea_position() {
        let cases: [(&[u8], &[u8], i32); 3] = [
            (b"\x01\x02\x03", b"\x03", 3),
            (b"\x01\x02\x03", b"\x04", 0),
            (b"\x01\x02\x03", b"", 1),
        ];
        for (bytea, sub_bytea, expected) in cases {
            assert_eq!(bytea_position(bytea, sub_bytea), expected)
        }
    }
}
