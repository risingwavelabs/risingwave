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

use num_bigint::{BigInt, Sign};

use super::{AccessResult, bail_uncategorized};

pub fn scaled_bigint_to_rust_decimal(
    value: BigInt,
    scale: usize,
) -> AccessResult<rust_decimal::Decimal> {
    let (sign, bytes) = value.to_bytes_be();
    let negative = sign == Sign::Minus;
    let (lo, mid, hi) = extract_decimal(bytes)?;

    Ok(rust_decimal::Decimal::from_parts(
        lo,
        mid,
        hi,
        negative,
        scale as u32,
    ))
}

/// Converts a Rust Decimal back to a `BigInt` with scale for Avro encoding
pub fn rust_decimal_to_scaled_bigint(decimal: rust_decimal::Decimal) -> (BigInt, usize) {
    let scale = decimal.scale() as usize;
    let negative = decimal.is_sign_negative();

    // Extract the parts
    // let (lo, mid, hi, _, _) = decimal.into_parts();
    let mut decimal_bytes = decimal.serialize();
    // The serialize method returns the bytes in little-endian order so need to reverse it to fit the big-endian order
    decimal_bytes.reverse();

    // Concatenate bytes from hi, mid, and lo in big-endian order
    let mut bytes = Vec::with_capacity(12);
    bytes.extend_from_slice(&decimal_bytes[0..4]); // hi
    bytes.extend_from_slice(&decimal_bytes[4..8]); // mid
    bytes.extend_from_slice(&decimal_bytes[8..12]); // lo

    // Trim leading zeros
    let first_non_zero = bytes.iter().position(|&x| x != 0).unwrap_or(bytes.len());
    bytes = bytes[first_non_zero..].to_vec();

    // Create BigInt with correct sign
    let bigint = if bytes.is_empty() {
        BigInt::from(0)
    } else {
        BigInt::from_bytes_be(if negative { Sign::Minus } else { Sign::Plus }, &bytes)
    };

    (bigint, scale)
}

fn extract_decimal(bytes: Vec<u8>) -> AccessResult<(u32, u32, u32)> {
    match bytes.len() {
        len @ 0..=4 => {
            let mut pad = vec![0; 4 - len];
            pad.extend_from_slice(&bytes);
            let lo = u32::from_be_bytes(pad.try_into().unwrap());
            Ok((lo, 0, 0))
        }
        len @ 5..=8 => {
            let zero_len = 8 - len;
            let mid_end = 4 - zero_len;

            let mut pad = vec![0; zero_len];
            pad.extend_from_slice(&bytes[..mid_end]);
            let mid = u32::from_be_bytes(pad.try_into().unwrap());

            let lo = u32::from_be_bytes(bytes[mid_end..].to_owned().try_into().unwrap());
            Ok((lo, mid, 0))
        }
        len @ 9..=12 => {
            let zero_len = 12 - len;
            let hi_end = 4 - zero_len;
            let mid_end = hi_end + 4;

            let mut pad = vec![0; zero_len];
            pad.extend_from_slice(&bytes[..hi_end]);
            let hi = u32::from_be_bytes(pad.try_into().unwrap());

            let mid = u32::from_be_bytes(bytes[hi_end..mid_end].to_owned().try_into().unwrap());

            let lo = u32::from_be_bytes(bytes[mid_end..].to_owned().try_into().unwrap());
            Ok((lo, mid, hi))
        }
        _ => bail_uncategorized!("invalid decimal bytes length {}", bytes.len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal_conversion() {
        // Test case 1: Large positive number with scale
        let bigint = BigInt::from(123456789012345678_i64);
        let scale = 10;
        test_conversion(bigint, scale);

        // Test case 2: Negative number
        let bigint = BigInt::from(-987654321987654321_i64);
        let scale = 5;
        test_conversion(bigint, scale);

        // Test case 3: Zero with scale
        let bigint = BigInt::from(0);
        let scale = 3;
        test_conversion(bigint, scale);

        // Test case 4: Maximum u32 boundaries
        let bigint = BigInt::from(u32::MAX) + BigInt::from(1);
        let scale = 0;
        test_conversion(bigint, scale);

        // Test case 5: Small number with large scale
        let bigint = BigInt::from(123);
        let scale = 28;
        test_conversion(bigint, scale);

        // Test case 6: Number with leading zeros in binary representation
        let bigint = BigInt::parse_bytes(b"100000000", 10).unwrap();
        let scale = 4;
        test_conversion(bigint, scale);

        // Test case 7: Maximum supported decimal
        let bigint = BigInt::parse_bytes(b"79228162514264337593543950335", 10).unwrap();
        let scale = 28;
        test_conversion(bigint, scale);

        // Test case 8: Minimum supported decimal
        let bigint = BigInt::parse_bytes(b"-79228162514264337593543950335", 10).unwrap();
        let scale = 28;
        test_conversion(bigint, scale);

        // Test case 9: Numbers near u32 boundaries
        let values = [
            BigInt::from(u32::MAX) - BigInt::from(1),
            BigInt::from(u32::MAX),
            BigInt::from(u32::MAX) + BigInt::from(1),
            BigInt::from(u32::MAX) + BigInt::from(u32::MAX),
        ];
        for value in values {
            test_conversion(value, 5);
        }
    }

    fn test_conversion(bigint: BigInt, scale: usize) {
        let decimal = scaled_bigint_to_rust_decimal(bigint.clone(), scale).unwrap();
        let (bigint_back, scale_back) = rust_decimal_to_scaled_bigint(decimal);
        assert_eq!(
            bigint, bigint_back,
            "BigInt conversion failed for value: {}",
            bigint
        );
        assert_eq!(
            scale, scale_back,
            "Scale conversion failed for scale: {}",
            scale
        );
    }
}
