// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::types::{Decimal, OrderedF64};

use crate::Result;

#[inline(always)]
pub fn round_digits<D: Into<i32>>(input: Decimal, digits: D) -> Result<Decimal> {
    let digits = digits.into();
    if digits < 0 {
        Ok(Decimal::zero())
    } else {
        Ok(input.round_dp(digits as u32))
    }
}

#[inline(always)]
pub fn ceil_f64(input: OrderedF64) -> Result<OrderedF64> {
    Ok(f64::ceil(input.0).into())
}

#[inline(always)]
pub fn ceil_decimal(input: Decimal) -> Result<Decimal> {
    Ok(input.ceil())
}

#[inline(always)]
pub fn floor_f64(input: OrderedF64) -> Result<OrderedF64> {
    Ok(f64::floor(input.0).into())
}

#[inline(always)]
pub fn floor_decimal(input: Decimal) -> Result<Decimal> {
    Ok(input.floor())
}

// Ties are broken by rounding away from zero
#[inline(always)]
pub fn round_f64(input: OrderedF64) -> Result<OrderedF64> {
    Ok(f64::round(input.0).into())
}

// Ties are broken by rounding away from zero
#[inline(always)]
pub fn round_decimal(input: Decimal) -> Result<Decimal> {
    Ok(input.round_dp(0))
}
#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use num_traits::FromPrimitive;
    use risingwave_common::types::{Decimal, OrderedF64};

    use super::ceil_f64;
    use crate::vector_op::round::*;

    fn do_test(input: &str, digits: i32, expected_output: &str) {
        let v = Decimal::from_str(input).unwrap();
        let rounded_value = round_digits(v, digits).unwrap();
        assert_eq!(expected_output, rounded_value.to_string().as_str());
    }

    #[test]
    fn test_round_digits() {
        do_test("21.666666666666666666666666667", 4, "21.6667");
        do_test("84818.33333333333333333333333", 4, "84818.3333");
        do_test("84818.15", 1, "84818.2");
        do_test("21.372736", -1, "0");
    }

    #[test]
    fn test_round_f64() {
        assert_eq!(
            ceil_f64(OrderedF64::from(42.2)).unwrap(),
            OrderedF64::from(43.0)
        );
        assert_eq!(
            ceil_f64(OrderedF64::from(-42.8)).unwrap(),
            OrderedF64::from(-42.0)
        );

        assert_eq!(
            floor_f64(OrderedF64::from(42.8)).unwrap(),
            OrderedF64::from(42.0)
        );
        assert_eq!(
            floor_f64(OrderedF64::from(-42.8)).unwrap(),
            OrderedF64::from(-43.0)
        );

        assert_eq!(
            round_f64(OrderedF64::from(42.4)).unwrap(),
            OrderedF64::from(42.0)
        );
        assert_eq!(
            round_f64(OrderedF64::from(42.5)).unwrap(),
            OrderedF64::from(43.0)
        );
        assert_eq!(
            round_f64(OrderedF64::from(-6.5)).unwrap(),
            OrderedF64::from(-7.0)
        );
    }

    #[test]
    fn test_round_decimal() {
        assert_eq!(
            ceil_decimal(Decimal::from_f64(42.2).unwrap()).unwrap(),
            Decimal::from_f64(43.0).unwrap()
        );
        assert_eq!(
            ceil_decimal(Decimal::from_f64(-42.8).unwrap()).unwrap(),
            Decimal::from_f64(-42.0).unwrap()
        );

        assert_eq!(
            floor_decimal(Decimal::from_f64(42.2).unwrap()).unwrap(),
            Decimal::from_f64(42.0).unwrap()
        );
        assert_eq!(
            floor_decimal(Decimal::from_f64(-42.8).unwrap()).unwrap(),
            Decimal::from_f64(-43.0).unwrap()
        );

        assert_eq!(
            round_decimal(Decimal::from_f64(42.4).unwrap()).unwrap(),
            Decimal::from_f64(42.0).unwrap()
        );
        assert_eq!(
            round_decimal(Decimal::from_f64(42.5).unwrap()).unwrap(),
            Decimal::from_f64(43.0).unwrap()
        );
        assert_eq!(
            round_decimal(Decimal::from_f64(-6.5).unwrap()).unwrap(),
            Decimal::from_f64(-7.0).unwrap()
        );
    }
}
