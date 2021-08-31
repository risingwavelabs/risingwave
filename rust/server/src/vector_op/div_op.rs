use crate::error::ErrorCode::NumericValueOutOfRange;
use crate::error::{Result, RwError};

macro_rules! div_integer_types {
    ($fn_name: ident, $left_type: ty, $right_type: ty, $max_type: ty) => {
        pub(crate) fn $fn_name(
            v1: Option<$left_type>,
            v2: Option<$right_type>,
        ) -> Result<Option<$max_type>> {
            match (v1, v2) {
                (Some(l1), Some(l2)) => match (l1 as $max_type).checked_div(l2 as $max_type) {
                    Some(v) => Ok(Some(v)),
                    None => Err(RwError::from(NumericValueOutOfRange)),
                },
                _ => Ok(None),
            }
        }
    };
}

div_integer_types!(div_i16_i16, i16, i16, i16);
div_integer_types!(div_i32_i16, i32, i16, i32);
div_integer_types!(div_i64_i16, i64, i16, i64);
div_integer_types!(div_i16_i32, i16, i32, i32);
div_integer_types!(div_i32_i32, i32, i32, i32);
div_integer_types!(div_i64_i32, i64, i32, i64);
div_integer_types!(div_i16_i64, i16, i64, i64);
div_integer_types!(div_i32_i64, i32, i64, i64);
div_integer_types!(div_i64_i64, i64, i64, i64);

macro_rules! div_float_types {
    ($fn_name: ident, $left_type: ty, $right_type: ty, $max_type: ty) => {
        pub(crate) fn $fn_name(
            v1: Option<$left_type>,
            v2: Option<$right_type>,
        ) -> Result<Option<$max_type>> {
            match (v1, v2) {
                (Some(l1), Some(l2)) => {
                    let v = (l1 as $max_type) / (l2 as $max_type);
                    let check = (v.is_finite() && !v.is_nan());
                    match check {
                        true => Ok(Some(v)),
                        false => Err(RwError::from(NumericValueOutOfRange)),
                    }
                }
                _ => Ok(None),
            }
        }
    };
}

div_float_types!(div_i16_f32, i16, f32, f32);
div_float_types!(div_i32_f32, i32, f32, f32);
div_float_types!(div_i64_f32, i64, f32, f32);
div_float_types!(div_f32_f32, f32, f32, f32);
div_float_types!(div_f32_i16, f32, i16, f32);
div_float_types!(div_f32_i32, f32, i32, f32);
div_float_types!(div_f32_i64, f32, i64, f32);

div_float_types!(div_i16_f64, i16, f64, f64);
div_float_types!(div_i32_f64, i32, f64, f64);
div_float_types!(div_i64_f64, i64, f64, f64);
div_float_types!(div_f32_f64, f32, f64, f64);
div_float_types!(div_f64_f64, f64, f64, f64);
div_float_types!(div_f64_i16, f64, i16, f64);
div_float_types!(div_f64_i32, f64, i32, f64);
div_float_types!(div_f64_i64, f64, i64, f64);
div_float_types!(div_f64_f32, f64, f32, f64);
