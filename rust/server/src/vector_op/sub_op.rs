use crate::error::ErrorCode::NumericValueOutOfRange;
use crate::error::{Result, RwError};

// The subtraction is an asymmetric operation, i.e. A-B != B-A.
// The larger type among LHS and RHS has to be manually specified.
macro_rules! sub_integer_types {
    ($fn_name: ident, $left_type: ty, $right_type: ty, $max_type: ty) => {
        pub fn $fn_name(
            v1: Option<$left_type>,
            v2: Option<$right_type>,
        ) -> Result<Option<$max_type>> {
            match (v1, v2) {
                (Some(l1), Some(l2)) => match (l1 as $max_type).checked_sub(l2 as $max_type) {
                    Some(v) => Ok(Some(v)),
                    None => Err(RwError::from(NumericValueOutOfRange)),
                },
                _ => Ok(None),
            }
        }
    };
}

sub_integer_types!(sub_i16_i16, i16, i16, i16);
sub_integer_types!(sub_i32_i16, i32, i16, i32);
sub_integer_types!(sub_i64_i16, i64, i16, i64);
sub_integer_types!(sub_i16_i32, i16, i32, i32);
sub_integer_types!(sub_i32_i32, i32, i32, i32);
sub_integer_types!(sub_i64_i32, i64, i32, i64);
sub_integer_types!(sub_i16_i64, i16, i64, i16);
sub_integer_types!(sub_i32_i64, i32, i64, i32);
sub_integer_types!(sub_i64_i64, i64, i64, i64);

macro_rules! sub_float_types {
    ($fn_name: ident, $left_type: ty, $right_type: ty, $max_type: ty) => {
        pub fn $fn_name(
            v1: Option<$left_type>,
            v2: Option<$right_type>,
        ) -> Result<Option<$max_type>> {
            match (v1, v2) {
                (Some(l1), Some(l2)) => {
                    let v = (l1 as $max_type) - (l2 as $max_type);
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

sub_float_types!(sub_i16_f32, i16, f32, f32);
sub_float_types!(sub_i32_f32, i32, f32, f32);
sub_float_types!(sub_i64_f32, i64, f32, f32);
sub_float_types!(sub_f32_f32, f32, f32, f32);
sub_float_types!(sub_f32_i16, f32, i16, f32);
sub_float_types!(sub_f32_i32, f32, i32, f32);
sub_float_types!(sub_f32_i64, f32, i64, f32);

sub_float_types!(sub_i16_f64, i16, f64, f64);
sub_float_types!(sub_i32_f64, i32, f64, f64);
sub_float_types!(sub_i64_f64, i64, f64, f64);
sub_float_types!(sub_f32_f64, f32, f64, f64);
sub_float_types!(sub_f64_f64, f64, f64, f64);
sub_float_types!(sub_f64_i16, f64, i16, f64);
sub_float_types!(sub_f64_i32, f64, i32, f64);
sub_float_types!(sub_f64_i64, f64, i64, f64);
sub_float_types!(sub_f64_f32, f64, f32, f64);
