use crate::error::ErrorCode::NumericValueOutOfRange;
use crate::error::{Result, RwError};

// Add is a symmetric operation, so we assume the left type is always "larger" than the right type.
macro_rules! add_integer_types {
    ($fn_name: ident, $left_type: ty, $right_type: ty) => {
        pub fn $fn_name(
            v1: Option<$left_type>,
            v2: Option<$right_type>,
        ) -> Result<Option<$left_type>> {
            match (v1, v2) {
                (Some(l1), Some(l2)) => match l1.checked_add(l2 as $left_type) {
                    Some(v) => Ok(Some(v)),
                    None => Err(RwError::from(NumericValueOutOfRange)),
                },
                _ => Ok(None),
            }
        }
    };
}

add_integer_types!(add_i16_i16, i16, i16);
add_integer_types!(add_i32_i16, i32, i16);
add_integer_types!(add_i64_i16, i64, i16);
add_integer_types!(add_i32_i32, i32, i32);
add_integer_types!(add_i64_i32, i64, i32);
add_integer_types!(add_i64_i64, i64, i64);

macro_rules! add_float_types {
    ($fn_name: ident, $left_type: ty, $right_type: ty) => {
        pub fn $fn_name(
            v1: Option<$left_type>,
            v2: Option<$right_type>,
        ) -> Result<Option<$left_type>> {
            match (v1, v2) {
                (Some(l1), Some(l2)) => {
                    let v = l1 + (l2 as $left_type);
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

add_float_types!(add_f32_i16, f32, i16);
add_float_types!(add_f32_i32, f32, i32);
add_float_types!(add_f32_i64, f32, i64);
add_float_types!(add_f32_f32, f32, f32);
add_float_types!(add_f64_i16, f64, i16);
add_float_types!(add_f64_i32, f64, i32);
add_float_types!(add_f64_i64, f64, i64);
add_float_types!(add_f64_f32, f64, f32);
add_float_types!(add_f64_f64, f64, f64);
