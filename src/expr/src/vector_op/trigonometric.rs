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

// Radians per degree, a.k.a. PI / 180
static RADIANS_PER_DEGREE: f64 = 0.0174532925199432957692;

// returns the cosine of an angle that lies between 0 and 60 degrees.  This will return exactly 1
// when xis 0, and exactly 0.5 when x is 60 degrees.
fn cosd_0_to_60(x: f64) -> f64 {
    // https://github.com/postgres/postgres/blob/REL_15_2/src/backend/utils/adt/float.c
    let one_minus_cos_x: f64 = 1.0 - f64::cos(x * RADIANS_PER_DEGREE);
    // TODO: one_minus_cos_60 as a constant?
    let one_minus_cos_60 = 1.0 - f64::cos(60.0 * RADIANS_PER_DEGREE);
    return 1.0 - (one_minus_cos_x / one_minus_cos_60) / 2.0;
}

// returns the sine of an angle that lies between 0 and 30 degrees.  This will return exactly 0 when
// x is 0, and exactly 0.5 when x is 30 degrees.
fn sind_0_to_30(x: f64) -> f64 {
    // https://github.com/postgres/postgres/blob/REL_15_2/src/backend/utils/adt/float.c

    let sin_x = f64::sin(x * RADIANS_PER_DEGREE);
    // TODO: sin_30 as a constant?
    let sin_30 = f64::sin(30.0 * RADIANS_PER_DEGREE);

    return (sin_x / sin_30) / 2.0;
}

// Returns the sine of an angle in the first quadrant (0 to 90 degrees).
fn sind_q1(input: f64) -> f64 {
    // https://github.com/postgres/postgres/blob/REL_15_2/src/backend/utils/adt/float.c

    //  Stitch together the sine and cosine functions for the ranges [0, 30]
    //  and (30, 90].  These guarantee to return exact answers at their
    //  endpoints, so the overall result is a continuous monotonic function
    //  that gives exact results when x = 0, 30 and 90 degrees.

    if input <= 30.0 {
        sind_0_to_30(input)
    } else {
        cosd_0_to_60(90.0 - input)
    }
}

#[function("sind(float64) -> float64")]
pub fn sind_f64(input: F64) -> F64 {
    // PSQL implementation: https://github.com/postgres/postgres/blob/REL_15_2/src/backend/utils/adt/float.c#L2444

    // Returns NaN if input is NaN or infinite. Different from PSQL implementation.
    if input.0.is_nan() || input.0.is_infinite() {
        return f64::NAN.into();
    }

    let arg1 = input.0 % 360.0;
    let sign = 1.0;

    let (arg1, sign) = if arg1 < 0.0 {
        // sind(-x) = -sind(x)
        (-arg1, -sign)
    } else if arg1 > 180.0 {
        //  sind(360-x) = -sind(x)
        (360.0 - arg1, -sign)
    } else if arg1 > 90.0 {
        //  sind(180-x) = sind(x)
        (180.0 - arg1, sign)
    } else {
        (arg1, sign)
    };

    let result = sign * sind_q1(arg1);

    if result.is_infinite() {
        // Different from PSQL implementation.
        f64::NAN.into()
    } else {
        result.into()
    }
}

#[cfg(test)]
mod tests {

    use std::f64::consts::PI;

    use risingwave_common::types::F64;

    use crate::vector_op::trigonometric::*;

    /// numbers are equal within a rounding error
    fn assert_similar(lhs: F64, rhs: F64) {
        let x = F64::from(lhs.abs() - rhs.abs()).abs() <= 0.000000000000001;
        assert!(x);
    }

    #[test]
    fn test_degrees() {
        let d = F64::from(180);
        let pi = F64::from(PI);
        assert_similar(
            sin_f64(F64::from(50).to_radians().into()),
            sind_f64(F64::from(50)),
        );
        assert_similar(
            sin_f64(F64::from(100).to_radians().into()),
            sind_f64(F64::from(100)),
        );
        assert_similar(
            sin_f64(F64::from(250).to_radians().into()),
            sind_f64(F64::from(250)),
        );
        assert_similar(sin_f64(pi), sind_f64(d));
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
}
