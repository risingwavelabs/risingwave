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

#[function("md5(varchar) -> varchar")]
pub fn md5(s: &str, writer: &mut dyn Write) {
    write!(writer, "{:x}", ::md5::compute(s)).unwrap();
}

#[function("md5(bytea) -> varchar")]
pub fn md5_from_bytea(s: &[u8], writer: &mut dyn Write) {
    writer
        .write_str(&::hex::encode(::md5::compute(s).0))
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_md5() {
        let cases = [
            ("hello world", "5eb63bbbe01eeed093cb22bb8f5acdc3"),
            ("hello RUST", "917b821a0a5f23ab0cfdb36056d2eb9d"),
            (
                "abcdefghijklmnopqrstuvwxyz",
                "c3fcd3d76192e4007dfb496cca67e13b",
            ),
        ];

        for (s, expected) in cases {
            let mut writer = String::new();
            md5(s, &mut writer);
            assert_eq!(writer, expected);
        }
    }

    #[test]
    fn test_md5_bytea() {
        let cases = [
            ("hello world".as_bytes(), "5eb63bbbe01eeed093cb22bb8f5acdc3"),
            ("hello RUST".as_bytes(), "917b821a0a5f23ab0cfdb36056d2eb9d"),
            (
                "abcdefghijklmnopqrstuvwxyz".as_bytes(),
                "c3fcd3d76192e4007dfb496cca67e13b",
            ),
        ];

        for (s, expected) in cases {
            let mut writer = String::new();
            md5_from_bytea(s, &mut writer);
            assert_eq!(writer, expected);
        }
    }
}
