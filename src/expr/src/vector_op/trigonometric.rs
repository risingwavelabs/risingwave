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
static RADIANS_PER_DEGREE: f64 = 0.017_453_292_519_943_295;
// Constants we use to get more accurate results.
// See PSQL: https://github.com/postgres/postgres/blob/78ec02d612a9b69039ec2610740f738968fe144d/src/backend/utils/adt/float.c#L2024
static SIN_30: f64 = 0.499_999_999_999_999_94;
static ONE_MINUS_COS_60: f64 = 0.499_999_999_999_999_9;
static TAN_45: f64 = 1.0;

// returns the cosine of an angle that lies between 0 and 60 degrees. This will return exactly 1
// when xi s 0, and exactly 0.5 when x is 60 degrees.
fn cosd_0_to_60(x: f64) -> f64 {
    // https://github.com/postgres/postgres/blob/REL_15_2/src/backend/utils/adt/float.c
    let one_minus_cos_x: f64 = 1.0 - f64::cos(x * RADIANS_PER_DEGREE);
    1.0 - (one_minus_cos_x / ONE_MINUS_COS_60) / 2.0
}

// returns the sine of an angle that lies between 0 and 30 degrees. This will return exactly 0 when
// x is 0, and exactly 0.5 when x is 30 degrees.
fn sind_0_to_30(x: f64) -> f64 {
    // https://github.com/postgres/postgres/blob/REL_15_2/src/backend/utils/adt/float.c
    let sin_x = f64::sin(x * RADIANS_PER_DEGREE);
    (sin_x / SIN_30) / 2.0
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
    let result = sign * (tan_arg1 / TAN_45);

    // On some machines we get tand(180) = minus zero, but this isn't always
    // true. For portability, and because the user constituency for this
    // function probably doesn't want minus zero, force it to plain zero.
    let result = if result == 0.0 { 0.0 } else { result };
    result.into()
}

#[cfg(test)]
mod tests {

    use std::f64::consts::PI;

    use risingwave_common::types::F64;

    use crate::vector_op::trigonometric::*;

    /// numbers are equal within a rounding error
    fn assert_similar(lhs: F64, rhs: F64) {
        let x = F64::from(lhs.abs() - rhs.abs()).abs() <= 0.000000000000001;
        assert!(x, "{:?} != {:?}", lhs.0, rhs.0);
    }

    #[test]
    fn test_degrees() {
        let d = F64::from(180);
        let pi = F64::from(PI);

        // sind
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

        // exact matches
        assert_eq!(sind_f64(F64::from(30)).0, 0.5);
        assert_eq!(sind_f64(F64::from(90)).0, 1.0);
        assert_eq!(sind_f64(F64::from(180)).0, 0.0);
        assert_eq!(sind_f64(F64::from(270)).0, -1.0);

        // cosd
        assert_eq!(cos_f64(pi), cosd_f64(d));
        assert_similar(
            cos_f64(F64::from(-180).to_radians().into()),
            cosd_f64(F64::from(-180)),
        );
        assert_similar(
            cos_f64(F64::from(-190).to_radians().into()),
            cosd_f64(F64::from(-190)),
        );
        assert_similar(
            cos_f64(F64::from(50).to_radians().into()),
            cosd_f64(F64::from(50)),
        );
        assert_similar(
            cos_f64(F64::from(100).to_radians().into()),
            cosd_f64(F64::from(100)),
        );
        assert_similar(
            cos_f64(F64::from(250).to_radians().into()),
            cosd_f64(F64::from(250)),
        );

        // exact matches
        assert_eq!(cosd_f64(F64::from(0)).0, 1.0);
        assert_eq!(cosd_f64(F64::from(90)).0, 0.0);

        // tand
        assert_similar(
            tan_f64(F64::from(-10).to_radians().into()),
            tand_f64(F64::from(-10)),
        );
        assert_similar(
            tan_f64(F64::from(50).to_radians().into()),
            tand_f64(F64::from(50)),
        );
        // we get slightly different result here, which is why I reduce the required accuracy
        assert!(
            (tan_f64(F64::from(250).to_radians().into()) - tand_f64(F64::from(250))).abs()
                < 0.00000000000001
        );
        assert_similar(
            tan_f64(F64::from(360).to_radians().into()),
            tand_f64(F64::from(360)),
        );

        // exact matches
        assert!(tand_f64(F64::from(-270)).is_infinite());
        assert_eq!(tand_f64(F64::from(-180)), 0.0);
        assert_eq!(tand_f64(F64::from(180)), 0.0);
        assert!(tand_f64(F64::from(-90)).is_infinite());
        assert!(tand_f64(F64::from(90)).is_infinite());
        assert!(tand_f64(F64::from(270)).is_infinite());
        assert!(tand_f64(F64::from(450)).is_infinite());
        assert!(tand_f64(F64::from(90)).is_infinite());
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
