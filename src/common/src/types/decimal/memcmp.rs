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

//! The memcomparable encoding of decimals.
//!
//! The format is the one of `memcomparable::Serializer::serialize_decimal`, which follows the key
//! encoding of `SQLite4` (<https://sqlite.org/src4/doc/trunk/www/key_encoding.wiki>) except that
//! `NaN` sorts above `Infinity`: a byte with the sign and the base-100 exponent, then base-100
//! digits. The crate only handles `rust_decimal` values, so this module implements the same
//! format for all decimals, with the same bytes for the values the crate can encode.

use bytes::{Buf, BufMut};
use ethnum::U256;
use rust_decimal::Decimal as RustDecimal;
use serde::{Deserialize, Serialize};

use super::wide::{self, Finite};
use super::{Decimal, KIND_NAN, KIND_POSITIVE_INF, LegacyDecimal};

const NEGATIVE_INF: u8 = 0x07;
const ZERO: u8 = 0x15;
const POSITIVE_INF: u8 = 0x23;
const NAN: u8 = 0x24;

/// A finite value has at most 39 decimal digits, or 20 base-100 digits, including padding.
const MAX_CENTIMAL_DIGITS: usize = 20;

impl Decimal {
    pub fn memcmp_serialize(
        &self,
        serializer: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        let mut put = |byte: u8| byte.serialize(&mut *serializer);
        let Some(value) = self.finite() else {
            return put(match self.kind() {
                KIND_NAN => NAN,
                KIND_POSITIVE_INF => POSITIVE_INF,
                _ => NEGATIVE_INF,
            });
        };
        if value.is_zero() {
            return put(ZERO);
        }
        let (exponent, digits) = exponent_and_digits(value.coefficient, value.scale);
        if !value.negative {
            match exponent {
                11.. => {
                    put(0x22)?;
                    put(exponent as u8)?;
                }
                0..=10 => put(0x17 + exponent as u8)?,
                _ => {
                    put(0x16)?;
                    put(!(-exponent) as u8)?;
                }
            }
            for digit in digits {
                put(digit)?;
            }
        } else {
            match exponent {
                11.. => {
                    put(0x08)?;
                    put(!exponent as u8)?;
                }
                0..=10 => put(0x13 - exponent as u8)?,
                _ => {
                    put(0x14)?;
                    put(-exponent as u8)?;
                }
            }
            for digit in digits {
                put(!digit)?;
            }
        }
        Ok(())
    }

    pub fn memcmp_deserialize(
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        let mut get = || u8::deserialize(&mut *deserializer);
        let flag = get()?;
        let exponent = match flag {
            NEGATIVE_INF => return Ok(Self::NEGATIVE_INF),
            0x08 => !get()? as i8,
            0x09..=0x13 => (0x13 - flag) as i8,
            0x14 => -(get()? as i8),
            ZERO => {
                return Ok(Self::from_legacy(LegacyDecimal::Normalized(
                    RustDecimal::ZERO,
                )));
            }
            0x16 => -!(get()? as i8),
            0x17..=0x21 => (flag - 0x17) as i8,
            0x22 => get()? as i8,
            POSITIVE_INF => return Ok(Self::POSITIVE_INF),
            NAN => return Ok(Self::NAN),
            b => return Err(memcomparable::Error::InvalidDecimalEncoding(b)),
        };
        let negative = (NEGATIVE_INF..ZERO).contains(&flag);
        let mut mantissa = U256::ZERO;
        let mut len = 0i32;
        loop {
            let mut byte = get()?;
            if negative {
                byte = !byte;
            }
            mantissa = mantissa * 100 + (byte / 2) as u128;
            len += 1;
            if byte & 1 == 0 {
                break;
            }
            if len as usize >= MAX_CENTIMAL_DIGITS {
                return Err(memcomparable::Error::InvalidDecimalEncoding(byte));
            }
        }

        let mut scale = (len - exponent as i32) * 2;
        if scale < -(wide::MAX_DIGITS as i32) {
            return Err(memcomparable::Error::InvalidDecimalEncoding(flag));
        }
        if scale <= 0 {
            // For example, 1 with the exponent 2 is 100.
            for _ in 0..-scale {
                mantissa *= 10;
            }
            scale = 0;
        } else if mantissa % 10 == 0 {
            // Remove the padding, for example in `0.01_11_10`.
            mantissa /= 10;
            scale -= 1;
        }
        if mantissa >= U256::new(10u128.pow(wide::MAX_DIGITS)) || scale > wide::MAX_SCALE as i32 {
            return Err(memcomparable::Error::InvalidDecimalEncoding(flag));
        }
        let value = Finite {
            negative,
            coefficient: mantissa.as_u128(),
            scale: scale as u32,
        };
        if Self::fits_legacy(&value) {
            // Exactly what the `memcomparable` crate decodes.
            let mantissa = value.coefficient as i128;
            let mantissa = if negative { -mantissa } else { mantissa };
            return Ok(Self::from_legacy(LegacyDecimal::Normalized(
                RustDecimal::from_i128_with_scale(mantissa, value.scale),
            )));
        }
        Ok(Self::from_finite(value))
    }
}

/// The base-100 exponent and the base-100 digits of a non-zero value, each digit `d` encoded as
/// `2d + 1`, except for `2d` on the last one.
fn exponent_and_digits(coefficient: u128, scale: u32) -> (i8, Vec<u8>) {
    let precision = coefficient.ilog10() as i32 + 1;
    let e10 = precision - scale as i32;
    let e100 = if e10 >= 0 { (e10 + 1) / 2 } else { e10 / 2 };
    // An odd exponent needs a leading zero digit, for example `111.11` is `0.011111 * 100^2`.
    let mut digit_count = if e10 == 2 * e100 {
        precision
    } else {
        precision + 1
    };
    let mut mantissa = U256::new(coefficient);
    while mantissa % 10 == 0 {
        mantissa /= 10;
        digit_count -= 1;
    }
    // Pad to whole base-100 digits, for example `0.12345` but not `0.01111`.
    if digit_count % 2 == 1 {
        mantissa *= 10;
    }
    let mut digits = Vec::with_capacity(MAX_CENTIMAL_DIGITS);
    while mantissa != U256::ZERO {
        digits.push((mantissa % 100).as_u8() * 2 + 1);
        mantissa /= 100;
    }
    digits[0] -= 1;
    digits.reverse();
    (e100 as i8, digits)
}

#[cfg(test)]
mod tests {
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::util::iter_util::ZipEqFast;

    fn encode(decimal: Decimal, reverse: bool) -> Vec<u8> {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        serializer.set_reverse(reverse);
        decimal.memcmp_serialize(&mut serializer).unwrap();
        serializer.into_inner()
    }

    fn decode(bytes: &[u8], reverse: bool) -> Decimal {
        let mut deserializer = memcomparable::Deserializer::new(bytes);
        deserializer.set_reverse(reverse);
        let decimal = Decimal::memcmp_deserialize(&mut deserializer).unwrap();
        assert!(!deserializer.has_remaining());
        decimal
    }

    /// Values the `memcomparable` crate can encode keep exactly its bytes and decoded values.
    #[test]
    fn test_same_as_memcomparable_crate() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(28);
        let mut values = vec![
            Decimal::NAN,
            Decimal::POSITIVE_INF,
            Decimal::NEGATIVE_INF,
            "-0.000".parse().unwrap(),
        ];
        for _ in 0..50000 {
            let coefficient = match rng.random_range(0..3) {
                0 => rng.random_range(0..10000u128),
                1 => 10u128.pow(rng.random_range(0..29)) * rng.random_range(1..8u128),
                _ => rng.random_range(0..1u128 << 96),
            };
            let value = Finite {
                negative: rng.random(),
                coefficient: coefficient.min((1 << 96) - 1),
                scale: rng.random_range(0..=28),
            };
            values.push(Decimal::from_finite(value));
        }
        for decimal in values {
            for reverse in [false, true] {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                serializer.set_reverse(reverse);
                serializer
                    .serialize_decimal(match decimal.to_legacy() {
                        LegacyDecimal::Normalized(d) => memcomparable::Decimal::Normalized(d),
                        LegacyDecimal::NaN => memcomparable::Decimal::NaN,
                        LegacyDecimal::PositiveInf => memcomparable::Decimal::Inf,
                        LegacyDecimal::NegativeInf => memcomparable::Decimal::NegInf,
                    })
                    .unwrap();
                let expected = serializer.into_inner();
                assert_eq!(encode(decimal, reverse), expected, "{decimal}");

                let mut deserializer = memcomparable::Deserializer::new(&expected[..]);
                deserializer.set_reverse(reverse);
                let expected = match deserializer.deserialize_decimal().unwrap() {
                    memcomparable::Decimal::Normalized(d) => LegacyDecimal::Normalized(d),
                    memcomparable::Decimal::NaN => LegacyDecimal::NaN,
                    memcomparable::Decimal::Inf => LegacyDecimal::PositiveInf,
                    memcomparable::Decimal::NegInf => LegacyDecimal::NegativeInf,
                };
                let decoded = decode(&encode(decimal, reverse), reverse);
                assert_eq!(
                    decoded.to_fixed_bytes(),
                    Decimal::from_legacy(expected).to_fixed_bytes(),
                    "{decimal}"
                );
            }
        }
    }

    /// Wide values round-trip by value and sort like the values.
    #[test]
    fn test_wide_order() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(38);
        let mut values: Vec<Decimal> = (0..20000)
            .map(|_| {
                let digits = rng.random_range(1..=wide::MAX_DIGITS);
                Decimal::from_finite(Finite {
                    negative: rng.random(),
                    coefficient: rng.random_range(0..10u128.pow(digits)),
                    scale: rng.random_range(0..=wide::MAX_SCALE),
                })
            })
            .collect();
        values.sort();
        for reverse in [false, true] {
            let encoded: Vec<_> = values.iter().map(|&d| encode(d, reverse)).collect();
            for (decimal, bytes) in values.iter().zip_eq_fast(&encoded) {
                let decoded = decode(bytes, reverse);
                assert_eq!(decoded, *decimal);
                assert_eq!(
                    decoded.to_fixed_bytes(),
                    decimal.normalize().to_fixed_bytes()
                );
            }
            for (pair, decimals) in encoded.windows(2).zip_eq_fast(values.windows(2)) {
                let expected = decimals[0].cmp(&decimals[1]);
                let actual = if reverse {
                    pair[1].cmp(&pair[0])
                } else {
                    pair[0].cmp(&pair[1])
                };
                assert_eq!(actual, expected, "{} vs {}", decimals[0], decimals[1]);
            }
        }
    }
}
