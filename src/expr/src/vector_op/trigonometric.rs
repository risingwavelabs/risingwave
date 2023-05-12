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

// Radians per degree, a.k.a. PI / 180
static RADIANS_PER_DEGREE: f64 = 0.017_453_292_519_943_295;
// Constants we use to get more accurate results.
// See PSQL: https://github.com/postgres/postgres/blob/78ec02d612a9b69039ec2610740f738968fe144d/src/backend/utils/adt/float.c#L2024
static SIND_30: f64 = 0.499_999_999_999_999_94;
static ONE_MINUS_COSD_60: f64 = 0.499_999_999_999_999_9;
static TAND_45: f64 = 1.0;
static COTD_45: f64 = 1.0;
static ASIN_0_5: f64 = 0.523_598_775_598_298_8;

// returns the cosine of an angle that lies between 0 and 60 degrees. This will return exactly 1
// when xi s 0, and exactly 0.5 when x is 60 degrees.
fn cosd_0_to_60(x: f64) -> f64 {
    // https://github.com/postgres/postgres/blob/REL_15_2/src/backend/utils/adt/float.c
    let one_minus_cos_x: f64 = 1.0 - f64::cos(x * RADIANS_PER_DEGREE);
    1.0 - (one_minus_cos_x / ONE_MINUS_COSD_60) / 2.0
}

// returns the sine of an angle that lies between 0 and 30 degrees. This will return exactly 0 when
// x is 0, and exactly 0.5 when x is 30 degrees.
fn sind_0_to_30(x: f64) -> f64 {
    // https://github.com/postgres/postgres/blob/REL_15_2/src/backend/utils/adt/float.c
    let sin_x = f64::sin(x * RADIANS_PER_DEGREE);
    (sin_x / SIND_30) / 2.0
}

// returns the cosine of an angle in the first quadrant (0 to 90 degrees).
fn cosd_q1(x: f64) -> f64 {
    // https://github.com/postgres/postgres/blob/REL_15_2/src/backend/utils/adt/float.c
    // Stitch together the sine and cosine functions for the ranges [0, 60]
    // and (60, 90].  These guarantee to return exact answers at their
    // endpoints, so the overall result is a continuous monotonic function
    // that gives exact results when x = 0, 60 and 90 degrees.
    if x <= 60.0 {
        cosd_0_to_60(x)
    } else {
        sind_0_to_30(90.0 - x)
    }
}

#[function("cosd(float64) -> float64")]
pub fn cosd_f64(input: F64) -> F64 {
    // See PSQL implementation: https://github.com/postgres/postgres/blob/78ec02d612a9b69039ec2610740f738968fe144d/src/backend/utils/adt/float.c
    let arg1 = input.0;

    // Return NaN if input is NaN or Infinite. Slightly different from PSQL implementation
    if input.0.is_nan() || input.0.is_infinite() {
        return F64::from(f64::NAN);
    }

    // Reduce the range of the input to [0,90] degrees
    let mut sign = 1.0;
    let mut arg1 = arg1 % 360.0;

    if arg1 < 0.0 {
        // cosd(-x) = cosd(x)
        arg1 = -arg1;
    }
    if arg1 > 180.0 {
        // cosd(360-x) = cosd(x)
        arg1 = 360.0 - arg1;
    }
    if arg1 > 90.0 {
        // cosd(180-x) = -cosd(x)
        arg1 = 180.0 - arg1;
        sign = -sign;
    }

    let result: f64 = sign * cosd_q1(arg1);

    if result.is_infinite() {
        return F64::from(f64::NAN);
    }

    result.into()
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

    let mut arg1 = input.0 % 360.0;
    let mut sign = 1.0;

    if arg1 < 0.0 {
        // sind(-x) = -sind(x)
        arg1 = -arg1;
        sign = -sign;
    }
    if arg1 > 180.0 {
        //  sind(360-x) = -sind(x)
        arg1 = 360.0 - arg1;
        sign = -sign;
    }
    if arg1 > 90.0 {
        //  sind(180-x) = sind(x)
        arg1 = 180.0 - arg1;
    }

    let result = sign * sind_q1(arg1);

    if result.is_infinite() {
        // Different from PSQL implementation.
        f64::NAN.into()
    } else {
        result.into()
    }
}

#[function("cotd(float64) -> float64")]
pub fn cotd_f64(input: F64) -> F64 {
    // PSQL implementation: https://github.com/postgres/postgres/blob/78ec02d612a9b69039ec2610740f738968fe144d/src/backend/utils/adt/float.c#L2378

    // Returns NaN if input is NaN or infinite. Different from PSQL implementation.
    if input.0.is_nan() || input.0.is_infinite() {
        return f64::NAN.into();
    }

    let mut arg1 = input.0 % 360.0;
    let mut sign = 1.0;

    if arg1 < 0.0 {
        // cotd(-x) = -cotd(x)
        arg1 = -arg1;
        sign = -sign;
    }

    if arg1 > 180.0 {
        // cotd(360-x) = -cotd(x)
        arg1 = 360.0 - arg1;
        sign = -sign;
    }

    if arg1 > 90.0 {
        // cotd(180-x) = -cotd(x)
        arg1 = 180.0 - arg1;
        sign = -sign;
    }

    let cot_arg1 = cosd_q1(arg1) / sind_q1(arg1);
    let result = sign * (cot_arg1 / COTD_45);

    // On some machines we get cotd(270) = minus zero, but this isn't always
    // true. For portability, and because the user constituency for this
    // function probably doesn't want minus zero, force it to plain zero.
    let result = if result == 0.0 { 0.0 } else { result };
    // Not checking for overflow because cotd(0) == Inf
    result.into()
}

#[function("tand(float64) -> float64")]
pub fn tand_f64(input: F64) -> F64 {
    // PSQL implementation: https://github.com/postgres/postgres/blob/REL_15_2/src/backend/utils/adt/float.c

    // Returns NaN if input is NaN or infinite. Different from PSQL implementation.
    if input.0.is_nan() || input.0.is_infinite() {
        return f64::NAN.into();
    }

    let mut arg1 = input.0 % 360.0;
    let mut sign = 1.0;

    if arg1 < 0.0 {
        // tand(-x) = -tand(x)
        arg1 = -arg1;
        sign = -sign;
    }

    if arg1 % 180.0 == 90.0 {
        return F64::from(f64::INFINITY);
    }

    if arg1 > 180.0 {
        // tand(360-x) = -tand(x)
        arg1 = 360.0 - arg1;
        sign = -sign;
    }

    if arg1 > 90.0 {
        // tand(180-x) = -tand(x)
        arg1 = 180.0 - arg1;
        sign = -sign;
    }

    let tan_arg1 = sind_q1(arg1) / cosd_q1(arg1);
    let result = sign * (tan_arg1 / TAND_45);

    // On some machines we get tand(180) = minus zero, but this isn't always
    // true. For portability, and because the user constituency for this
    // function probably doesn't want minus zero, force it to plain zero.
    let result = if result == 0.0 { 0.0 } else { result };
    result.into()
}

// returns the inverse sine of x in degrees, for x in the range [0,
// 1].  The result is an angle in the first quadrant --- [0, 90] degrees.
// For the 3 special case inputs (0, 0.5 and 1), this function will return exact values (0, 30 and
// 90 degrees respectively).
pub fn asind_q1(x: f64) -> f64 {
    // Stitch together inverse sine and cosine functions for the ranges [0,0.5] and (0.5, 1]. Each
    // expression below is guaranteed to returnexactly 30 for x=0.5, so the result is a continuous
    // monotonic functionover the full range.
    if x <= 0.5 {
        let asin_x = f64::asin(x);
        return (asin_x / ASIN_0_5) * 30.0;
    }

    let acos_x = f64::acos(x);
    90.0 - (acos_x / ASIN_0_5) * 60.0
}

#[function("asind(float64) -> float64")]
pub fn asind_f64(input: F64) -> F64 {
    let arg1 = input.0;

    // Return NaN if input is NaN or Infinite. Slightly different from PSQL implementation
    if input.0.is_nan() || input.0.is_infinite() {
        return F64::from(f64::NAN);
    }

    // Return NaN if input is out of range. Slightly different from PSQL implementation
    if input.0 < -1.0 || input.0 > 1.0 {
        return F64::from(f64::NAN);
    }

    let result = if arg1 >= 0.0 {
        asind_q1(arg1)
    } else {
        -asind_q1(-arg1)
    };

    if result.is_infinite() {
        return F64::from(f64::NAN);
    }
    result.into()
}

#[function("degrees(float64) -> float64")]
pub fn degrees_f64(input: F64) -> F64 {
    input.0.to_degrees().into()
}

#[function("radians(float64) -> float64")]
pub fn radians_f64(input: F64) -> F64 {
    input.0.to_radians().into()
}

#[cfg(test)]
mod tests {
    use std::f64::consts::PI;

    use risingwave_common::types::{FloatExt, F64};

    use crate::vector_op::trigonometric::*;

    fn precision() -> f64 {
        1e-13
    }

    /// numbers are equal within a rounding error
    fn assert_similar(lhs: F64, rhs: F64) {
        let x = (lhs.0 - rhs.0).abs() <= precision();
        assert!(
            x,
            "{:?} != {:?}. Required precision is {:?}",
            lhs.0,
            rhs.0,
            precision()
        );
    }

    #[test]
    fn test_degrees() {
        let d = F64::from(180);
        let pi = F64::from(PI);

        // sind
        assert_similar(sin_f64(50_f64.to_radians().into()), sind_f64(F64::from(50)));
        assert_similar(
            sin_f64(100_f64.to_radians().into()),
            sind_f64(F64::from(100)),
        );
        assert_similar(
            sin_f64(250_f64.to_radians().into()),
            sind_f64(F64::from(250)),
        );
        assert_similar(sin_f64(pi), sind_f64(d));

        // exact matches
        assert_eq!(sind_f64(F64::from(30)).0, 0.5);
        assert_eq!(sind_f64(F64::from(90)).0, 1.0);
        assert_eq!(sind_f64(F64::from(180)).0, 0.0);
        assert_eq!(sind_f64(F64::from(270)).0, -1.0);

        // cosd
        assert_eq!(cos_f64(pi), cosd_f64(d));
        assert_similar(
            cos_f64((-180_f64).to_radians().into()),
            cosd_f64(F64::from(-180)),
        );
        assert_similar(
            cos_f64((-190_f64).to_radians().into()),
            cosd_f64(F64::from(-190)),
        );
        assert_similar(cos_f64(50_f64.to_radians().into()), cosd_f64(F64::from(50)));
        assert_similar(
            cos_f64(100_f64.to_radians().into()),
            cosd_f64(F64::from(100)),
        );
        assert_similar(
            cos_f64(250_f64.to_radians().into()),
            cosd_f64(F64::from(250)),
        );

        // exact matches
        assert_eq!(cosd_f64(F64::from(0)).0, 1.0);
        assert_eq!(cosd_f64(F64::from(90)).0, 0.0);

        // cotd
        assert_eq!(F64::from(-f64::INFINITY), cotd_f64(d));
        assert!(cotd_f64(F64::from(-180)).is_infinite());
        assert!(
            (cotd_f64(F64::from(-190)) + F64::from(5.671281819617705))
                .abs()
                .0
                <= precision(),
        );
        assert_similar(cot_f64(50_f64.to_radians().into()), cotd_f64(F64::from(50)));
        assert_similar(
            cot_f64(100_f64.to_radians().into()),
            cotd_f64(F64::from(100)),
        );
        assert_similar(
            cot_f64(250_f64.to_radians().into()),
            cotd_f64(F64::from(250)),
        );

        // tand
        assert_similar(
            tan_f64((-10_f64).to_radians().into()),
            tand_f64(F64::from(-10)),
        );
        assert_similar(tan_f64(50_f64.to_radians().into()), tand_f64(F64::from(50)));
        // we get slightly different result here, which is why I reduce the required accuracy
        assert!(
            (tan_f64(250_f64.to_radians().into()) - tand_f64(F64::from(250)))
                .0
                .abs()
                < precision()
        );
        assert_similar(
            tan_f64(360_f64.to_radians().into()),
            tand_f64(F64::from(360)),
        );

        // asind
        assert_similar(asind_f64(F64::from(-1)), F64::from(-90));
        assert_similar(asind_f64(F64::from(-0.5)), F64::from(-30));
        assert_similar(asind_f64(F64::from(0)), F64::from(0));
        assert_similar(asind_f64(F64::from(0.5)), F64::from(30));
        assert_similar(asind_f64(F64::from(1)), F64::from(90));

        // exact matches
        assert!(tand_f64(F64::from(-270)).0.is_infinite());
        assert_eq!(tand_f64(F64::from(-180)), 0.0);
        assert_eq!(tand_f64(F64::from(180)), 0.0);
        assert!(tand_f64(F64::from(-90)).0.is_infinite());
        assert!(tand_f64(F64::from(90)).0.is_infinite());
        assert!(tand_f64(F64::from(270)).0.is_infinite());
        assert!(tand_f64(F64::from(450)).0.is_infinite());
        assert!(tand_f64(F64::from(90)).0.is_infinite());
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
            two * atan_f64(y / (F64::from((x.0.powi(2) + y.0.powi(2)).sqrt()) + x)),
        )
    }

    #[test]
    fn test_degrees_and_radians() {
        let full_angle = F64::from(360);
        let tau = F64::from(std::f64::consts::TAU);
        assert_similar(degrees_f64(tau), full_angle);
        assert_similar(radians_f64(full_angle), tau);

        let straight_angle = F64::from(180);
        let pi = F64::from(std::f64::consts::PI);
        assert_similar(degrees_f64(pi), straight_angle);
        assert_similar(radians_f64(straight_angle), pi);

        let right_angle = F64::from(90);
        let half_pi = F64::from(std::f64::consts::PI / 2.);
        assert_similar(degrees_f64(half_pi), right_angle);
        assert_similar(radians_f64(right_angle), half_pi);

        let zero = F64::from(0);
        assert_similar(degrees_f64(zero), zero);
        assert_similar(radians_f64(zero), zero);
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

        let x = x.powi(2).0;

        assert_similar(
            asinh_f64(F64::from(x.powi(2) - 1.0) / (two * x)),
            atanh_f64(F64::from(x.powi(2) - 1.0) / F64::from(x.powi(2) + 1.0)),
        );
    }
}
