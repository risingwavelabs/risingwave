use crate::array::ArrayRef;
use crate::array2::ArrayImpl;
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::types::ArithmeticOperatorKind;
use crate::vector_op::add_op::{vector_add_primitive_float, vector_add_primitive_integer};

pub fn vector_arithmetic_impl(
    operator_type: ArithmeticOperatorKind,
    left_array: ArrayRef,
    right_array: ArrayRef,
) -> Result<ArrayRef> {
    let left_len = left_array.len();
    let right_len = right_array.len();
    ensure!(left_len == right_len);
    let left_type = left_array.data_type();
    let right_type = right_array.data_type();
    ensure!(left_type.data_type_kind() == right_type.data_type_kind());
    match operator_type {
        ArithmeticOperatorKind::Plus => vector_add(left_array, right_array),
        ArithmeticOperatorKind::Subtract => unimplemented!(),
        ArithmeticOperatorKind::Multiply => unimplemented!(),
        ArithmeticOperatorKind::Divide => unimplemented!(),
        _ => unimplemented!(),
    }
}

fn vector_add(left_array: ArrayRef, right_array: ArrayRef) -> Result<ArrayRef> {
    // TODO(chi): remove compact_v1 convert
    let left = Into::<ArrayImpl>::into(left_array) as ArrayImpl;
    let right = Into::<ArrayImpl>::into(right_array) as ArrayImpl;

    use ArrayImpl::*;

    let result: ArrayImpl = match (left, right) {
        // Int16, Int32, Int64
        (Int16(l), Int16(r)) => vector_add_primitive_integer::<_, _, i16>(l, r)?.into(),
        (Int16(l), Int32(r)) => vector_add_primitive_integer::<_, _, i32>(l, r)?.into(),
        (Int16(l), Int64(r)) => vector_add_primitive_integer::<_, _, i64>(l, r)?.into(),
        (Int32(l), Int16(r)) => vector_add_primitive_integer::<_, _, i32>(l, r)?.into(),
        (Int32(l), Int32(r)) => vector_add_primitive_integer::<_, _, i32>(l, r)?.into(),
        (Int32(l), Int64(r)) => vector_add_primitive_integer::<_, _, i64>(l, r)?.into(),
        (Int64(l), Int16(r)) => vector_add_primitive_integer::<_, _, i64>(l, r)?.into(),
        (Int64(l), Int32(r)) => vector_add_primitive_integer::<_, _, i64>(l, r)?.into(),
        (Int64(l), Int64(r)) => vector_add_primitive_integer::<_, _, i64>(l, r)?.into(),
        // Float32, Float64
        (Float32(l), Float32(r)) => vector_add_primitive_float::<_, _, f32>(l, r)?.into(),
        (Float32(l), Float64(r)) => vector_add_primitive_float::<_, _, f64>(l, r)?.into(),
        (Float64(l), Float32(r)) => vector_add_primitive_float::<_, _, f64>(l, r)?.into(),
        (Float64(l), Float64(r)) => vector_add_primitive_float::<_, _, f64>(l, r)?.into(),
        _ => {
            return Err(
                InternalError("Unsupported type for arithmetic operation.".to_string()).into(),
            )
        }
    };

    Ok(result.into())
}
