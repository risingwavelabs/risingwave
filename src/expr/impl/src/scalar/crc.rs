// Copyright 2026 RisingWave Labs
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

/// Computes the CRC-32 value of the given bytes.
///
/// ```slt
/// query I
/// select crc32('\x00'::bytea);
/// ----
/// 3523407757
///
/// query I
/// select crc32(''::bytea);
/// ----
/// 0
///
/// query I
/// select crc32('The quick brown fox jumps over the lazy dog'::bytea);
/// ----
/// 1095738169
/// ```
#[function("crc32(bytea) -> int8")]
pub fn crc32(data: &[u8]) -> i64 {
    crc32fast::hash(data) as i64
}

/// Computes the CRC-32C value of the given bytes.
///
/// ```slt
/// query I
/// select crc32c(''::bytea);
/// ----
/// 0
///
/// query I
/// select crc32c('\x00'::bytea);
/// ----
/// 1383945041
/// ```
#[function("crc32c(bytea) -> int8")]
pub fn crc32c(data: &[u8]) -> i64 {
    crc32c::crc32c(data) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32() {
        assert_eq!(crc32(b""), 0);
        assert_eq!(crc32(b"\x00"), 3523407757);
        assert_eq!(crc32(b"123456789"), 3421780262); // standard CRC-32 check value
        assert_eq!(
            crc32(b"The quick brown fox jumps over the lazy dog"),
            1095738169
        );
    }

    #[test]
    fn test_crc32c() {
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(b"\x00"), 1383945041);
        assert_eq!(crc32c(b"123456789"), 3808858755); // standard CRC-32C check value
    }
}
