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

use num_traits::{Float, Zero};
use risingwave_common::types::F64;
use risingwave_expr_macro::function;

use crate::{ExprError, Result};

#[function("exp(float64) -> float64")]
pub fn exp_f64(input: F64) -> Result<F64> {
    // The cases where the exponent value is Inf or NaN can be handled explicitly and without
    // evaluating the `exp` operation.
    if input.is_nan() {
        Ok(input)
    } else if input.is_infinite() {
        if input.is_sign_negative() {
            Ok(0.into())
        } else {
            Ok(input)
        }
    } else {
        let res = input.exp();

        // If the argument passed to `exp` is not `inf` or `-inf` then a result that is `inf` or `0`
        // means that the operation had an overflow or an underflow, and the appropriate
        // error should be returned.
        if res.is_infinite() {
            Err(ExprError::NumericOverflow)
        } else if res.is_zero() {
            Err(ExprError::NumericUnderflow)
        } else {
            Ok(res)
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::F64;

    use super::exp_f64;
    use crate::ExprError;

    #[test]
    fn legal_input() {
        let res = exp_f64(0.0.into()).unwrap();
        assert_eq!(res, F64::from(1.0));
    }

    #[test]
    fn underflow() {
        let res = exp_f64((-1000.0).into()).unwrap_err();
        match res {
            ExprError::NumericUnderflow => (),
            _ => panic!("Expected ExprError::FloatUnderflow"),
        }
    }

    #[test]
    fn overflow() {
        let res = exp_f64(1000.0.into()).unwrap_err();
        match res {
            ExprError::NumericOverflow => (),
            _ => panic!("Expected ExprError::FloatUnderflow"),
        }
    }

    #[test]
    fn nan() {
        let res = exp_f64(f64::NAN.into()).unwrap();
        assert_eq!(res, F64::from(f64::NAN));

        let res = exp_f64((-f64::NAN).into()).unwrap();
        assert_eq!(res, F64::from(-f64::NAN));
    }

    #[test]
    fn infinity() {
        let res = exp_f64(f64::INFINITY.into()).unwrap();
        assert_eq!(res, F64::from(f64::INFINITY));

        let res = exp_f64(f64::NEG_INFINITY.into()).unwrap();
        assert_eq!(res, F64::from(0.0));
    }
}
