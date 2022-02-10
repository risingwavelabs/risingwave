use risingwave_common::array::RwError;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::Values;

use crate::binder::Binder;
use crate::expr::{Expr as _, ExprImpl, ExprType, FunctionCall};

#[derive(Debug)]
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
                types[i] = Self::find_compat(types[i], expr.return_type())?
            }
        }
        let rows = bound
            .into_iter()
            .map(|vec| {
                vec.into_iter()
                    .enumerate()
                    .map(|(i, expr)| Self::ensure_type(expr, types[i]))
                    .collect::<Vec<ExprImpl>>()
            })
            .collect::<Vec<Vec<ExprImpl>>>();
        let schema = Schema::new(types.into_iter().map(Field::unnamed).collect());
        Ok(BoundValues { rows, schema })
    }

    /// Find compatible type for `left` and `right`.
    fn find_compat(left: DataType, right: DataType) -> Result<DataType> {
        if (left == right || left.is_numeric() && right.is_numeric())
            || (left.is_string() && right.is_string()
                || (left.is_date_or_timestamp() && right.is_date_or_timestamp()))
        {
            if left as i32 > right as i32 {
                Ok(left)
            } else {
                Ok(right)
            }
        } else {
            Err(RwError::from(ErrorCode::InternalError(format!(
                "Can not find compatible type for {:?} and {:?}",
                left, right
            ))))
        }
    }

    /// Check if cast needs to be inserted.
    fn ensure_type(expr: ExprImpl, ty: DataType) -> ExprImpl {
        if ty != expr.return_type() {
            ExprImpl::FunctionCall(Box::new(FunctionCall::new_with_return_type(
                ExprType::Cast,
                vec![expr],
                ty,
            )))
        } else {
            expr
        }
    }
}

#[cfg(test)]
mod tests {
    use std::iter::zip;

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
            ExprType::Cast,
            vec![binder.bind_expr(expr1).unwrap()],
            DataType::Decimal,
        )));
        let row2 = binder.bind_expr(expr2).unwrap();
        let rows = vec![vec![row1], vec![row2]];
        let types = vec![DataType::Decimal];
        let schema = Schema::new(types.into_iter().map(Field::unnamed).collect());

        assert_eq!(res.schema, schema);
        for (vec, expected_vec) in zip(res.rows, rows) {
            for (expr, expected_expr) in zip(vec, expected_vec) {
                assert_eq!(expr.return_type(), expected_expr.return_type());
            }
        }

        // Test incompatible.
        let expr1 = Expr::Value(Value::SingleQuotedString("1".to_string()));
        let expr2 = Expr::Value(Value::Number("1".to_string(), false));
        let values = Values(vec![vec![expr1], vec![expr2]]);
        let res = binder.bind_values(values);
        assert!(res.is_err());
        if let Err(res_err) = res {
            assert_eq!(
                res_err,
                RwError::from(ErrorCode::InternalError(format!(
                    "Can not find compatible type for {:?} and {:?}",
                    DataType::Varchar,
                    DataType::Int32
                )))
            );
        }
    }
}
