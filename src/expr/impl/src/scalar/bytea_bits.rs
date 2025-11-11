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

use risingwave_expr::{ExprError, Result, function};

/// Extracts n'th bit from binary string.
///
/// # Example
///
/// ```slt
/// query T
/// SELECT get_bit('\x1234567890'::bytea, 30);
/// ----
/// 1
/// ```
#[function("get_bit(bytea, int8) -> int4")]
pub fn get_bit(bytes: &[u8], n: i64) -> Result<i32> {
    let max_sz = (bytes.len() * 8) as i64;
    if n < 0 || n >= max_sz {
        return Err(ExprError::InvalidParam {
            name: "get_bit",
            reason: format!("index {} out of valid range, 0..{}", n, max_sz - 1).into(),
        });
    }
    let index = n / 8;
    let byte = bytes[index as usize];
    Ok(((byte >> (n % 8)) & 1) as i32)
}

/// Sets n'th bit in binary string to newvalue.
///
/// # Example
///
/// ```slt
/// query T
/// SELECT set_bit('\x1234567890'::bytea, 30, 0);
/// ----
/// \x1234563890
/// ```
#[function("set_bit(bytea, int8, int4) -> bytea")]
pub fn set_bit(bytes: &[u8], n: i64, value: i32, writer: &mut impl std::io::Write) -> Result<()> {
    let max_sz = (bytes.len() * 8) as i64;
    if n < 0 || n >= max_sz {
        return Err(ExprError::InvalidParam {
            name: "set_bit",
            reason: format!("index {} out of valid range, 0..{}", n, max_sz - 1).into(),
        });
    }

    if value != 0 && value != 1 {
        return Err(ExprError::InvalidParam {
            name: "set_bit",
            reason: format!("value {} is invalid, new bit must be 0 or 1", value).into(),
        });
    }

    let index = (n / 8) as usize;
    let bit_pos = (n % 8) as u32;
    let orig = bytes[index];
    let mask = 1u8 << bit_pos;
    let new_byte = if value != 0 {
        orig | mask
    } else {
        orig & !mask
    };

    if index > 0 {
        writer.write_all(&bytes[..index]).unwrap();
    }
    writer.write_all(&[new_byte]).unwrap();
    if index + 1 < bytes.len() {
        writer.write_all(&bytes[index + 1..]).unwrap();
    }
    Ok(())
}

/// Extracts n'th byte from binary string.
///
/// # Example
///
/// ```slt
/// query T
/// SELECT get_byte('\x1234567890'::bytea, 4);
/// ----
/// 144
/// ```
#[function("get_byte(bytea, int4) -> int4")]
pub fn get_byte(bytes: &[u8], n: i32) -> Result<i32> {
    let max_sz = bytes.len() as i32;
    if n < 0 || n >= max_sz {
        return Err(ExprError::InvalidParam {
            name: "get_byte",
            reason: format!("index {} out of valid range, 0..{}", n, max_sz - 1).into(),
        });
    }
    Ok(bytes[n as usize].into())
}

/// Sets n'th byte in binary string to newvalue.
///
/// # Example
///
/// ```slt
/// query T
/// SELECT set_byte('\x1234567890'::bytea, 4, 64);
/// ----
/// \x1234567840
/// ```
#[function("set_byte(bytea, int4, int4) -> bytea")]
pub fn set_byte(bytes: &[u8], n: i32, value: i32, writer: &mut impl std::io::Write) -> Result<()> {
    let max_sz = bytes.len() as i32;
    if n < 0 || n >= max_sz {
        return Err(ExprError::InvalidParam {
            name: "set_byte",
            reason: format!("index {} out of valid range, 0..{}", n, max_sz - 1).into(),
        });
    }

    let index = n as usize;
    if index > 0 {
        writer.write_all(&bytes[..index]).unwrap();
    }
    writer.write_all(&[value as u8]).unwrap();
    if index + 1 < bytes.len() {
        writer.write_all(&bytes[index + 1..]).unwrap();
    }
    Ok(())
}

/// Returns the number of bits set in the binary string
///
/// # Example
///
/// ```slt
/// query T
/// SELECT bit_count('\x1234567890'::bytea);
/// ----
/// 15
/// ```
#[function("bit_count(bytea) -> int8")]
pub fn bit_count(bytes: &[u8]) -> i64 {
    let mut ans = 0;
    for byte in bytes {
        ans += byte.count_ones();
    }
    ans.into()
}

/// Reverses the bytes in the binary string.
///
/// # Example
///
/// ```slt
/// query T
/// SELECT reverse('\x1234567890'::bytea);
/// ----
/// \x9078563412
/// ```
#[function("reverse(bytea) -> bytea")]
pub fn reverse_bytea(bytes: &[u8], writer: &mut impl std::io::Write) {
    for byte in bytes.iter().rev() {
        writer.write_all(&[*byte]).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_bytes() -> Vec<u8> {
        vec![0x12, 0x34, 0x56, 0x78, 0x90]
    }

    #[test]
    fn test_get_bit_basic_and_bounds() {
        let b = sample_bytes();
        // example: bit 30 is 1
        assert_eq!(get_bit(&b, 30).unwrap(), 1);
        // negative index -> error
        assert!(get_bit(&b, -1i64).is_err());
        // out of range -> error
        let max_bits = (b.len() * 8) as i64;
        assert!(get_bit(&b, max_bits).is_err());
    }

    #[test]
    fn test_set_bit_basic() {
        let b = sample_bytes();
        let mut out = Vec::new();
        // set bit 30 to 0, expected change at byte index 3 (0x78 -> 0x38)
        set_bit(&b, 30, 0, &mut out).unwrap();
        assert_eq!(&out, &[0x12, 0x34, 0x56, 0x38, 0x90]);
    }

    #[test]
    fn test_get_byte_and_bounds() {
        let b = sample_bytes();
        assert_eq!(get_byte(&b, 4).unwrap(), 144); // 0x90 == 144
        assert!(get_byte(&b, -1i32).is_err());
        assert!(get_byte(&b, b.len() as i32).is_err());
    }

    #[test]
    fn test_set_byte_basic_and_invalid_value() {
        let b = sample_bytes();
        let mut out = Vec::new();
        // set last byte to 64 (0x40)
        set_byte(&b, 4, 64, &mut out).unwrap();
        assert_eq!(&out, &[0x12, 0x34, 0x56, 0x78, 0x40]);

        // invalid byte value -> error
        assert!(set_byte(&b, 4, 256, &mut Vec::new()).is_err());
    }

    #[test]
    fn test_bit_count() {
        let b = sample_bytes();
        // expected 15 as in example
        assert_eq!(bit_count(&b), 15);
    }

    #[test]
    fn test_reverse_bytea() {
        let b = sample_bytes();
        let mut out = Vec::new();
        reverse_bytea(&b, &mut out);
        assert_eq!(&out, &[0x90, 0x78, 0x56, 0x34, 0x12]);
    }
}
