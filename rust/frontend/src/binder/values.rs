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
        self.context.in_values_clause = true;
        let vec2d = values.0;
        let bound = vec2d
            .into_iter()
            .map(|vec| vec.into_iter().map(|expr| self.bind_expr(expr)).collect())
            .collect::<Result<Vec<Vec<_>>>>()?;
        self.context.in_values_clause = false;

        // calc column type and insert casts here
        let mut types = bound[0]
            .iter()
            .map(|expr| expr.return_type())
            .collect::<Vec<DataType>>();
        for vec in &bound {
            for (i, expr) in vec.iter().enumerate() {
                types[i] = Self::find_compat(types[i].clone(), expr.return_type())?
            }
        }
        let rows = bound
            .into_iter()
            .map(|vec| {
                vec.into_iter()
                    .enumerate()
                    .map(|(i, expr)| Self::ensure_type(expr, types[i].clone()))
                    .collect::<Vec<ExprImpl>>()
            })
            .collect::<Vec<Vec<ExprImpl>>>();
        let schema = Schema::new(types.into_iter().map(Field::unnamed).collect());
        Ok(BoundValues { rows, schema })
    }

    /// Find compatible type for `left` and `right`.
    pub fn find_compat(left: DataType, right: DataType) -> Result<DataType> {
        if (left == right || left.is_numeric() && right.is_numeric())
            || (left.is_string() && right.is_string()
                || (left.is_date_or_timestamp() && right.is_date_or_timestamp()))
        {
            if left.type_index() > right.type_index() {
                Ok(left)
            } else {
                Ok(right)
            }
        } else {
            Err(ErrorCode::InternalError(format!(
                "Can not find compatible type for {:?} and {:?}",
                left, right
            ))
            .into())
        }
    }

    /// Find proper type over a collection of exprs
    pub fn find_proper_type(exprs: &[ExprImpl]) -> Result<DataType> {
        let mut return_type = exprs.get(0).unwrap().return_type();
        for i in 1..exprs.len() {
            return_type = Self::find_compat(return_type, exprs.get(i).unwrap().return_type())?;
        }
        Ok(return_type)
    }

    /// Check if cast needs to be inserted.
    pub fn ensure_type(expr: ExprImpl, ty: DataType) -> ExprImpl {
        if ty == expr.return_type() {
            expr
        } else {
            ExprImpl::FunctionCall(Box::new(FunctionCall::new_with_return_type(
                ExprType::Cast,
                vec![expr],
                ty,
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::zip_eq;
    use risingwave_sqlparser::ast::{Expr, Value};

    use super::*;
    use crate::catalog::database_catalog::DatabaseCatalog;

    #[test]
    fn test_bind_values() {
        let catalog = DatabaseCatalog::new(0);
        let mut binder = Binder::new(Arc::new(catalog));

        // Test i32 -> decimal.
        let expr1 = Expr::Value(Value::Number("1".to_string(), false));
        let expr2 = Expr::Value(Value::Number("1.1".to_string(), false));
        let values = Values(vec![vec![expr1], vec![expr2]]);
        let res = binder.bind_values(values).unwrap();

        let types = vec![DataType::Decimal];
        let schema = Schema::new(types.into_iter().map(Field::unnamed).collect());

        assert_eq!(res.schema, schema);
        for vec in res.rows {
            for (expr, ty) in zip_eq(vec, schema.data_types()) {
                assert_eq!(expr.return_type(), ty);
            }
        }
    }
}
