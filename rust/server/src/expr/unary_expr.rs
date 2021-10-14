use super::expr_tmpl::{UnaryBytesExpression, UnaryExpression};
use crate::array::{I32Array, I64Array, UTF8Array};
use crate::expr::BoxedExpression;
use crate::types::{DataTypeKind, DataTypeRef};
use crate::vector_op::cast;
use crate::vector_op::length::length_default;
use crate::vector_op::upper::upper;
use risingwave_proto::expr::ExprNode_Type;
use std::marker::PhantomData;

pub fn new_unary_expr(
    expr_type: ExprNode_Type,
    return_type: DataTypeRef,
    child_expr: BoxedExpression,
) -> BoxedExpression {
    match (
        expr_type,
        return_type.data_type_kind(),
        child_expr.return_type().data_type_kind(),
    ) {
        (ExprNode_Type::CAST, DataTypeKind::Date, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<UTF8Array, I32Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::str_to_date,
                _phantom: PhantomData,
            })
        }
        (ExprNode_Type::CAST, DataTypeKind::Time, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<UTF8Array, I64Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::str_to_time,
                _phantom: PhantomData,
            })
        }
        (ExprNode_Type::CAST, DataTypeKind::Timestamp, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<UTF8Array, I64Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::str_to_timestamp,
                _phantom: PhantomData,
            })
        }
        (ExprNode_Type::CAST, DataTypeKind::Timestampz, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<UTF8Array, I64Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::str_to_timestampz,
                _phantom: PhantomData,
            })
        }
        (ExprNode_Type::UPPER, _, _) => Box::new(UnaryBytesExpression::<UTF8Array, _> {
            expr_ia1: child_expr,
            return_type,
            func: upper,
            _phantom: PhantomData,
        }),
        (_, _, _) => {
            unimplemented!(
                "The expression using vectorized expression framework is not supported yet!"
            )
        }
    }
}

pub fn new_length_default(expr_ia1: BoxedExpression, return_type: DataTypeRef) -> BoxedExpression {
    Box::new(UnaryExpression::<UTF8Array, I64Array, _> {
        expr_ia1,
        return_type,
        func: length_default,
        _phantom: PhantomData,
    })
}
