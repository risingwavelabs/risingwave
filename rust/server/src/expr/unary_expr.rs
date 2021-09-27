use super::expr_tmpl::UnaryExpression;
use crate::array2::{I32Array, I64Array, UTF8Array};
use crate::expr::BoxedExpression;
use crate::types::{DataTypeKind, DataTypeRef};
use crate::vector_op::cast;
use risingwave_proto::expr::ExprNode_ExprNodeType;
use std::marker::PhantomData;

pub fn new_unary_expr(
    expr_type: ExprNode_ExprNodeType,
    return_type: DataTypeRef,
    child_expr: BoxedExpression,
) -> BoxedExpression {
    match (
        expr_type,
        return_type.data_type_kind(),
        child_expr.return_type().data_type_kind(),
    ) {
        (ExprNode_ExprNodeType::CAST, DataTypeKind::Date, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<UTF8Array, I32Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::str_to_date,
                data1: PhantomData,
            })
        }
        (ExprNode_ExprNodeType::CAST, DataTypeKind::Time, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<UTF8Array, I64Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::str_to_time,
                data1: PhantomData,
            })
        }
        (ExprNode_ExprNodeType::CAST, DataTypeKind::Timestamp, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<UTF8Array, I64Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::str_to_timestamp,
                data1: PhantomData,
            })
        }
        (ExprNode_ExprNodeType::CAST, DataTypeKind::Timestampz, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<UTF8Array, I64Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: cast::str_to_timestampz,
                data1: PhantomData,
            })
        }
        (_, _, _) => {
            unimplemented!(
                "The expression using vectorized expression framework is not supported yet!"
            )
        }
    }
}
