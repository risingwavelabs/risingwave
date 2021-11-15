/// For expression that accept 3 arguments + 1 bytes writer as input.
use crate::array::{I32Array, UTF8Array};
use crate::expr::template::TernaryBytesExpression;
use crate::expr::BoxedExpression;
use crate::types::DataTypeRef;
use crate::vector_op::replace::replace;
use crate::vector_op::substr::substr_start_for;
use std::marker::PhantomData;

pub fn new_substr_start_end(
    items: BoxedExpression,
    off: BoxedExpression,
    len: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(TernaryBytesExpression::<UTF8Array, I32Array, I32Array, _> {
        expr_ia1: items,
        expr_ia2: off,
        expr_ia3: len,
        return_type,
        func: substr_start_for,
        _phantom: PhantomData,
    })
}

pub fn new_replace_expr(
    s: BoxedExpression,
    from_str: BoxedExpression,
    to_str: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(
        TernaryBytesExpression::<UTF8Array, UTF8Array, UTF8Array, _> {
            expr_ia1: s,
            expr_ia2: from_str,
            expr_ia3: to_str,
            return_type,
            func: replace,
            _phantom: PhantomData,
        },
    )
}
