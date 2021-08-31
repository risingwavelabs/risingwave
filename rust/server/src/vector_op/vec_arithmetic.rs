use crate::array::ArrayRef;
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::types::{ArithmeticOperatorKind, DataTypeKind};
use crate::types::{Float32Type, Float64Type, Int16Type, Int32Type, Int64Type};
use crate::vector_op::add_op::*;
use crate::vector_op::binary_op::vec_binary_op_primitive_array;
use std::ops::Deref;

pub(crate) fn vector_arithmetic_impl(
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
    let left_type = left_array.data_type().data_type_kind();
    let right_type = right_array.data_type().data_type_kind();
    match (left_type, right_type) {
        (DataTypeKind::Int16, DataTypeKind::Int16) => {
            vec_binary_op_primitive_array::<Int16Type, Int16Type, Int16Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_i16_i16,
            )
        }
        (DataTypeKind::Int16, DataTypeKind::Int32) => {
            // Send the right array on the LHS of add_i32_i16 operation.
            vec_binary_op_primitive_array::<Int32Type, Int16Type, Int32Type, _>(
                right_array.deref(),
                left_array.deref(),
                add_i32_i16,
            )
        }
        (DataTypeKind::Int16, DataTypeKind::Int64) => {
            vec_binary_op_primitive_array::<Int64Type, Int16Type, Int64Type, _>(
                right_array.deref(),
                left_array.deref(),
                add_i64_i16,
            )
        }
        (DataTypeKind::Int16, DataTypeKind::Float32) => {
            vec_binary_op_primitive_array::<Float32Type, Int16Type, Float32Type, _>(
                right_array.deref(),
                left_array.deref(),
                add_f32_i16,
            )
        }
        (DataTypeKind::Int16, DataTypeKind::Float64) => {
            vec_binary_op_primitive_array::<Float64Type, Int16Type, Float64Type, _>(
                right_array.deref(),
                left_array.deref(),
                add_f64_i16,
            )
        }
        (DataTypeKind::Int32, DataTypeKind::Int16) => {
            vec_binary_op_primitive_array::<Int32Type, Int16Type, Int32Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_i32_i16,
            )
        }
        (DataTypeKind::Int32, DataTypeKind::Int32) => {
            vec_binary_op_primitive_array::<Int32Type, Int32Type, Int32Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_i32_i32,
            )
        }
        (DataTypeKind::Int32, DataTypeKind::Int64) => {
            vec_binary_op_primitive_array::<Int64Type, Int32Type, Int64Type, _>(
                right_array.deref(),
                left_array.deref(),
                add_i64_i32,
            )
        }
        (DataTypeKind::Int32, DataTypeKind::Float32) => {
            vec_binary_op_primitive_array::<Float32Type, Int32Type, Float32Type, _>(
                right_array.deref(),
                left_array.deref(),
                add_f32_i32,
            )
        }
        (DataTypeKind::Int32, DataTypeKind::Float64) => {
            vec_binary_op_primitive_array::<Float64Type, Int32Type, Float64Type, _>(
                right_array.deref(),
                left_array.deref(),
                add_f64_i32,
            )
        }
        (DataTypeKind::Int64, DataTypeKind::Int16) => {
            vec_binary_op_primitive_array::<Int64Type, Int16Type, Int64Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_i64_i16,
            )
        }
        (DataTypeKind::Int64, DataTypeKind::Int32) => {
            vec_binary_op_primitive_array::<Int64Type, Int32Type, Int64Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_i64_i32,
            )
        }
        (DataTypeKind::Int64, DataTypeKind::Int64) => {
            vec_binary_op_primitive_array::<Int64Type, Int64Type, Int64Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_i64_i64,
            )
        }
        (DataTypeKind::Int64, DataTypeKind::Float32) => {
            vec_binary_op_primitive_array::<Float32Type, Int64Type, Float32Type, _>(
                right_array.deref(),
                left_array.deref(),
                add_f32_i64,
            )
        }
        (DataTypeKind::Int64, DataTypeKind::Float64) => {
            vec_binary_op_primitive_array::<Float64Type, Int64Type, Float64Type, _>(
                right_array.deref(),
                left_array.deref(),
                add_f64_i64,
            )
        }
        (DataTypeKind::Float32, DataTypeKind::Int16) => {
            vec_binary_op_primitive_array::<Float32Type, Int16Type, Float32Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_f32_i16,
            )
        }
        (DataTypeKind::Float32, DataTypeKind::Int32) => {
            vec_binary_op_primitive_array::<Float32Type, Int32Type, Float32Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_f32_i32,
            )
        }
        (DataTypeKind::Float32, DataTypeKind::Int64) => {
            vec_binary_op_primitive_array::<Float32Type, Int64Type, Float32Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_f32_i64,
            )
        }
        (DataTypeKind::Float32, DataTypeKind::Float32) => {
            vec_binary_op_primitive_array::<Float32Type, Float32Type, Float32Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_f32_f32,
            )
        }
        (DataTypeKind::Float32, DataTypeKind::Float64) => {
            vec_binary_op_primitive_array::<Float64Type, Float32Type, Float64Type, _>(
                right_array.deref(),
                left_array.deref(),
                add_f64_f32,
            )
        }
        (DataTypeKind::Float64, DataTypeKind::Int16) => {
            vec_binary_op_primitive_array::<Float64Type, Int16Type, Float64Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_f64_i16,
            )
        }
        (DataTypeKind::Float64, DataTypeKind::Int32) => {
            vec_binary_op_primitive_array::<Float64Type, Int32Type, Float64Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_f64_i32,
            )
        }
        (DataTypeKind::Float64, DataTypeKind::Int64) => {
            vec_binary_op_primitive_array::<Float64Type, Int64Type, Float64Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_f64_i64,
            )
        }
        (DataTypeKind::Float64, DataTypeKind::Float32) => {
            vec_binary_op_primitive_array::<Float64Type, Float32Type, Float64Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_f64_f32,
            )
        }
        (DataTypeKind::Float64, DataTypeKind::Float64) => {
            vec_binary_op_primitive_array::<Float64Type, Float64Type, Float64Type, _>(
                left_array.deref(),
                right_array.deref(),
                add_f64_f64,
            )
        }
        (DataTypeKind::Decimal, DataTypeKind::Decimal) => unimplemented!(),
        _ => Err(InternalError("Unsupported type for arithmetic operation.".to_string()).into()),
    }
}
