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

use risingwave_common::types::F64;
use risingwave_expr_macro::function;

#[function("sin(float64) -> float64")]
pub fn sin_f64(input: F64) -> F64 {
    f64::sin(input.0).into()
}

#[function("cos(float64) -> float64")]
pub fn cos_f64(input: F64) -> F64 {
    f64::cos(input.0).into()
}

#[function("tan(float64) -> float64")]
pub fn tan_f64(input: F64) -> F64 {
    f64::tan(input.0).into()
}

#[function("cot(float64) -> float64")]
pub fn cot_f64(input: F64) -> F64 {
    let res = 1.0 / f64::tan(input.0);
    res.into()
}

#[function("asin(float64) -> float64")]
pub fn asin_f64(input: F64) -> F64 {
    f64::asin(input.0).into()
}

#[function("acos(float64) -> float64")]
pub fn acos_f64(input: F64) -> F64 {
    f64::acos(input.0).into()
}

#[function("atan(float64) -> float64")]
pub fn atan_f64(input: F64) -> F64 {
    f64::atan(input.0).into()
}

#[function("atan2(float64, float64) -> float64")]
pub fn atan2_f64(input_x: F64, input_y: F64) -> F64 {
    input_x.0.atan2(input_y.0).into()
}

#[function("sinh(float64) -> float64")]
pub fn sinh_f64(input: F64) -> F64 {
    f64::sinh(input.0).into()
}

#[function("cosh(float64) -> float64")]
pub fn cosh_f64(input: F64) -> F64 {
    f64::cosh(input.0).into()
}

#[function("tanh(float64) -> float64")]
pub fn tanh_f64(input: F64) -> F64 {
    f64::tanh(input.0).into()
}

#[function("coth(float64) -> float64")]
pub fn coth_f64(input: F64) -> F64 {
    if input.0 == 0.0 {
        return f64::NAN.into();
    }
    // https://en.wikipedia.org/wiki/Hyperbolic_functions#Exponential_definitions
    (f64::cosh(input.0) / f64::sinh(input.0)).into()
}

#[function("asinh(float64) -> float64")]
pub fn asinh_f64(input: F64) -> F64 {
    f64::asinh(input.0).into()
}

#[function("acosh(float64) -> float64")]
pub fn acosh_f64(input: F64) -> F64 {
    f64::acosh(input.0).into()
}

#[function("atanh(float64) -> float64")]
pub fn atanh_f64(input: F64) -> F64 {
    f64::atanh(input.0).into()
}

#[cfg(test)]
mod tests {

    use num_traits::ToPrimitive;
    use risingwave_common::types::F64;

    use crate::vector_op::trigonometric::*;

    /// numbers are equal within a rounding error
    fn assert_similar(lhs: F64, rhs: F64) {
        let x = F64::from(lhs.abs() - rhs.abs()).abs();
        assert!(x <= 0.000000000000001);
    }

    #[test]
    fn test_trigonometric_funcs() {
        // from https://en.wikipedia.org/wiki/Trigonometric_functions#Sum_and_difference_formulas
        let x = F64::from(1);
        let y = F64::from(3);
        let one = F64::from(1);
        assert_similar(
            sin_f64(x + y),
            sin_f64(x) * cos_f64(y) + cos_f64(x) * sin_f64(y),
        );
        assert_similar(
            cos_f64(x + y),
            cos_f64(x) * cos_f64(y) - sin_f64(x) * sin_f64(y),
        );
        assert_similar(
            tan_f64(x + y),
            (tan_f64(x) + tan_f64(y)) / (one - tan_f64(x) * tan_f64(y)),
        );
    }

    #[test]
    fn test_inverse_trigonometric_funcs() {
        let x = F64::from(1);
        let y = F64::from(3);
        let two = F64::from(2);
        // https://en.wikipedia.org/wiki/Inverse_trigonometric_functions#Relationships_between_trigonometric_functions_and_inverse_trigonometric_functions
        assert_similar(x, sin_f64(asin_f64(x)));
        assert_similar(x, cos_f64(acos_f64(x)));
        assert_similar(x, tan_f64(atan_f64(x)));
        // https://en.wikipedia.org/wiki/Inverse_trigonometric_functions#Two-argument_variant_of_arctangent
        assert_similar(
            atan2_f64(y, x),
            two * atan_f64(y / (F64::from(F64::from(x.powi(2) + y.powi(2)).sqrt()) + x)),
        )
    }

    #[test]
    fn test_hyperbolic_trigonometric_funcs() {
        let two = F64::from(2);
        let one = F64::from(1);
        let x = F64::from(5);
        let y = F64::from(3);
        // https://en.wikipedia.org/wiki/Hyperbolic_functions#Sums_of_arguments
        assert_similar(
            sinh_f64(x + y),
            sinh_f64(x) * cosh_f64(y) + cosh_f64(x) * sinh_f64(y),
        );
        assert_similar(
            cosh_f64(x + y),
            cosh_f64(x) * cosh_f64(y) + sinh_f64(x) * sinh_f64(y),
        );
        assert_similar(
            tanh_f64(x + y),
            (tanh_f64(x) + tanh_f64(y)) / (one + tanh_f64(x) * tanh_f64(y)),
        );
        // https://en.wikipedia.org/wiki/Hyperbolic_functions#Useful_relations
        assert_similar(coth_f64(-x), -coth_f64(x));
        assert_similar(tanh_f64(-x), -tanh_f64(x));
        // https://en.wikipedia.org/wiki/Inverse_hyperbolic_functions#Other_identities
        assert_similar(two * acosh_f64(x), acosh_f64(two * x.powi(2) - one)); // for x >= 1
        assert_similar(two * asinh_f64(x), acosh_f64(two * x.powi(2) + one)); // for x >= 0
        assert_similar(
            asinh_f64(F64::from(x.powi(2).to_f64().unwrap() - 1.to_f64().unwrap()) / (two * x)),
            atanh_f64(
                F64::from(x.powi(2).to_f64().unwrap() - 1.to_f64().unwrap())
                    / F64::from(x.powi(2).to_f64().unwrap() + 1.to_f64().unwrap()),
            ),
        );
    }

    // TODO: add tests here
}
