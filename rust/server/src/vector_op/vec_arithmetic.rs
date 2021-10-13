use crate::array::ArrayImpl;
use crate::array::ArrayImpl::{Float32, Float64, Int16, Int32, Int64};
use crate::array::ArrayRef;
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::types::ArithmeticOperatorKind;
use crate::vector_op::add_op::{vector_add_primitive_float, vector_add_primitive_integer};
use crate::vector_op::div_op::{vector_div_primitive_float, vector_div_primitive_integer};
use crate::vector_op::mod_op::vector_mod_primitive;
use crate::vector_op::mul_op::{vector_mul_primitive_float, vector_mul_primitive_integer};
use crate::vector_op::sub_op::{vector_sub_primitive_float, vector_sub_primitive_integer};

macro_rules! vec_arithmetic {
    ($f_name:ident,$f_integer:ident, $f_float:ident) => {
        fn $f_name(left_array: ArrayRef, right_array: ArrayRef) -> Result<ArrayRef> {
            let left = left_array.as_ref();
            let right = right_array.as_ref();

            let result: ArrayImpl = match (left, right) {
                // Int16, Int32, Int64
                (Int16(l), Int16(r)) => $f_integer::<_, _, i16>(l, r)?.into(),
                (Int16(l), Int32(r)) => $f_integer::<_, _, i32>(l, r)?.into(),
                (Int16(l), Int64(r)) => $f_integer::<_, _, i64>(l, r)?.into(),
                (Int32(l), Int16(r)) => $f_integer::<_, _, i32>(l, r)?.into(),
                (Int32(l), Int32(r)) => $f_integer::<_, _, i32>(l, r)?.into(),
                (Int32(l), Int64(r)) => $f_integer::<_, _, i64>(l, r)?.into(),
                (Int64(l), Int16(r)) => $f_integer::<_, _, i64>(l, r)?.into(),
                (Int64(l), Int32(r)) => $f_integer::<_, _, i64>(l, r)?.into(),
                (Int64(l), Int64(r)) => $f_integer::<_, _, i64>(l, r)?.into(),
                // Float32, Float64
                // Float can op with integer.
                (Float32(l), Int16(r)) => $f_float::<_, _, f32>(l, r)?.into(),
                (Float32(l), Int32(r)) => $f_float::<_, _, f32>(l, r)?.into(),
                (Float32(l), Int64(r)) => $f_float::<_, _, f32>(l, r)?.into(),
                (Int16(r), Float32(l)) => $f_float::<_, _, f32>(l, r)?.into(),
                (Int32(r), Float32(l)) => $f_float::<_, _, f32>(l, r)?.into(),
                (Int64(r), Float32(l)) => $f_float::<_, _, f32>(l, r)?.into(),
                (Float32(l), Float32(r)) => $f_float::<_, _, f32>(l, r)?.into(),
                (Float32(l), Float64(r)) => $f_float::<_, _, f64>(l, r)?.into(),
                (Float64(l), Int16(r)) => $f_float::<_, _, f64>(l, r)?.into(),
                (Float64(l), Int32(r)) => $f_float::<_, _, f64>(l, r)?.into(),
                (Float64(l), Int64(r)) => $f_float::<_, _, f64>(l, r)?.into(),
                (Int16(r), Float64(l)) => $f_float::<_, _, f64>(l, r)?.into(),
                (Int32(r), Float64(l)) => $f_float::<_, _, f64>(l, r)?.into(),
                (Int64(r), Float64(l)) => $f_float::<_, _, f64>(l, r)?.into(),
                (Float64(l), Float32(r)) => $f_float::<_, _, f64>(l, r)?.into(),
                (Float64(l), Float64(r)) => $f_float::<_, _, f64>(l, r)?.into(),
                _ => {
                    return Err(InternalError(
                        "Unsupported type for arithmetic operation.".to_string(),
                    )
                    .into())
                }
            };
            Ok(result.into())
        }
    };
}

vec_arithmetic!(
    vector_add,
    vector_add_primitive_integer,
    vector_add_primitive_float
);

vec_arithmetic!(
    vector_subtract,
    vector_sub_primitive_integer,
    vector_sub_primitive_float
);

vec_arithmetic!(
    vector_multiply,
    vector_mul_primitive_integer,
    vector_mul_primitive_float
);

vec_arithmetic!(
    vector_divide,
    vector_div_primitive_integer,
    vector_div_primitive_float
);

vec_arithmetic!(vector_mod, vector_mod_primitive, vector_mod_primitive);

pub fn vector_arithmetic_impl(
    operator_type: ArithmeticOperatorKind,
    left_array: ArrayRef,
    right_array: ArrayRef,
) -> Result<ArrayRef> {
    let left_len = left_array.len();
    let right_len = right_array.len();
    ensure!(left_len == right_len);
    match operator_type {
        ArithmeticOperatorKind::Plus => vector_add(left_array, right_array),
        ArithmeticOperatorKind::Subtract => vector_subtract(left_array, right_array),
        ArithmeticOperatorKind::Multiply => vector_multiply(left_array, right_array),
        ArithmeticOperatorKind::Divide => vector_divide(left_array, right_array),
        ArithmeticOperatorKind::Mod => vector_mod(left_array, right_array),
    }
}
