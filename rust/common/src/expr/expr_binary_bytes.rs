/// For expression that only accept two arguments + 1 bytes writer as input.
use crate::array::{I32Array, Utf8Array};
use crate::expr::template::BinaryBytesExpression;
use crate::expr::BoxedExpression;
use crate::types::DataTypeRef;
use crate::vector_op::substr::*;
use std::marker::PhantomData;

pub fn new_substr_start(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(BinaryBytesExpression::<Utf8Array, I32Array, _> {
        expr_ia1,
        expr_ia2,
        return_type,
        func: substr_start,
        _phantom: PhantomData,
    })
}

pub fn new_substr_for(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(BinaryBytesExpression::<Utf8Array, I32Array, _> {
        expr_ia1,
        expr_ia2,
        return_type,
        func: substr_for,
        _phantom: PhantomData,
    })
}
