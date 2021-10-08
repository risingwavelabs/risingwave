use crate::array2::{I32Array, UTF8Array};
use crate::expr::expr_tmpl::TenaryBytesExpression;
use crate::expr::BoxedExpression;
use crate::types::DataTypeRef;
use crate::vector_op::substr::substr_start_for;
use std::marker::PhantomData;

pub fn new_substr_start_end(
    items: BoxedExpression,
    off: BoxedExpression,
    len: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(TenaryBytesExpression::<UTF8Array, I32Array, I32Array, _> {
        expr_ia1: items,
        expr_ia2: off,
        expr_ia3: len,
        return_type,
        func: substr_start_for,
        data1: PhantomData,
    })
}
