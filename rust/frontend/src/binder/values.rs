use risingwave_common::array::RwError;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_pb::expr::expr_node::Type;
use risingwave_sqlparser::ast::Values;

use crate::binder::Binder;
use crate::expr::{Expr as _, ExprImpl, FunctionCall};

#[derive(Debug, PartialEq)]
pub struct BoundValues {
    pub rows: Vec<Vec<ExprImpl>>,
    pub schema: Schema,
}

impl Binder {
    pub(super) fn bind_values(&mut self, values: Values) -> Result<BoundValues> {
        let vec2d = values.0;
        let bound = vec2d
            .into_iter()
            .map(|vec| vec.into_iter().map(|expr| self.bind_expr(expr)).collect())
            .collect::<Result<Vec<Vec<_>>>>()?;
        // calc column type and insert casts here
        let mut types = bound[0]
            .iter()
            .map(|expr| expr.return_type())
            .collect::<Vec<DataType>>();
        for vec in &bound {
            for (i, expr) in vec.iter().enumerate() {
                if let Some(tmp) = Self::check_compat(types[i], expr.return_type()) {
                    types[i] = tmp;
                } else {
                    return Err(RwError::from(ErrorCode::InternalError(format!(
                        "Can not find a compatible type for {:?} and {:?}",
                        types[i],
                        expr.return_type()
                    ))));
                }
            }
        }
        let rows = bound
            .into_iter()
            .map(|vec| {
                vec.into_iter()
                    .enumerate()
                    .map(|(i, expr)| {
                        if types[i] != expr.return_type() {
                            ExprImpl::FunctionCall(Box::new(FunctionCall::new_with_return_type(
                                Type::Cast,
                                vec![expr],
                                types[i],
                            )))
                        } else {
                            expr
                        }
                    })
                    .collect::<Vec<ExprImpl>>()
            })
            .collect::<Vec<Vec<ExprImpl>>>();
        let schema = Schema::new(types.into_iter().map(Field::unnamed).collect());
        Ok(BoundValues { rows, schema })
    }

    fn check_compat(left: DataType, right: DataType) -> Option<DataType> {
        if (left == right || left.is_numeric() && right.is_numeric())
            || (left.is_string() && right.is_string()
                || (left.is_date_or_timestamp() && right.is_date_or_timestamp()))
        {
            if left as i32 > right as i32 {
                Some(left)
            } else {
                Some(right)
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_sqlparser::ast::{Expr, Value};

    use super::*;

    #[test]
    fn test_bind_values() {
        let mut binder = Binder {};

        // Test i32 -> decimal.
        let expr1 = Expr::Value(Value::Number("1".to_string(), false));
        let expr2 = Expr::Value(Value::Number("1.1".to_string(), false));
        let values = Values(vec![vec![expr1.clone()], vec![expr2.clone()]]);
        let res = binder.bind_values(values).unwrap();

        let row1 = ExprImpl::FunctionCall(Box::new(FunctionCall::new_with_return_type(
            Type::Cast,
            vec![binder.bind_expr(expr1).unwrap()],
            DataType::Decimal,
        )));
        let row2 = binder.bind_expr(expr2).unwrap();
        let rows = vec![vec![row1], vec![row2]];
        let types = vec![DataType::Decimal];
        let schema = Schema::new(types.into_iter().map(Field::unnamed).collect());
        let expected_res = BoundValues { rows, schema };

        assert_eq!(res, expected_res);

        // Test incompatible.
        let expr1 = Expr::Value(Value::SingleQuotedString("1".to_string()));
        let expr2 = Expr::Value(Value::Number("1".to_string(), false));
        let values = Values(vec![vec![expr1.clone()], vec![expr2.clone()]]);
        let res = binder.bind_values(values);
        assert_eq!(
            res,
            Err(RwError::from(ErrorCode::InternalError(format!(
                "Can not find a compatible type for {:?} and {:?}",
                DataType::Varchar,
                DataType::Int32
            ))))
        );
    }
}
