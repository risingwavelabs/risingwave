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

use std::fmt::Write;

use risingwave_expr::function;

#[function("concat_op(varchar, varchar) -> varchar")]
pub fn concat_op(left: &str, right: &str, writer: &mut impl Write) {
    writer.write_str(left).unwrap();
    writer.write_str(right).unwrap();
}

/// Concatenates the two binary strings.
///
/// # Example
///
/// ```slt
/// query I
/// select '\x123456'::bytea || '\x789a00bcde'::bytea;
/// ----
/// \x123456789a00bcde
///
/// query I
/// select '\x123456'::bytea || '\x789a00bcde';
/// ----
/// \x123456789a00bcde
///
/// query I
/// select '\x123456'::bytea || ''::bytea;
/// ----
/// \x123456
/// ```
#[function("bytea_concat_op(bytea, bytea) -> bytea")]
pub fn bytea_concat_op(left: &[u8], right: &[u8]) -> Box<[u8]> {
    let mut result = Vec::with_capacity(left.len() + right.len());
    result.extend_from_slice(left);
    result.extend_from_slice(right);
    result.into_boxed_slice()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concat_op() {
        let mut s = String::new();
        concat_op("114", "514", &mut s);
        assert_eq!(s, "114514")
    }

    #[test]
    fn test_bytea_concat_op() {
        let left = b"\x01\x02\x03";
        let right = b"\x04\x05";
        let result = bytea_concat_op(left, right);
        assert_eq!(&*result, b"\x01\x02\x03\x04\x05");
    }
}
