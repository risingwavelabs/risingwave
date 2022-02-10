use risingwave_common::array::RwError;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataTypeKind;
use risingwave_pb::expr::expr_node::Type;
use risingwave_sqlparser::ast::Values;

use crate::binder::Binder;
use crate::expr::{Expr as _, ExprImpl, FunctionCall};

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
        // calc row type and insert casts here
        let mut types = bound[0]
            .iter()
            .map(|expr| expr.return_type())
            .collect::<Vec<DataTypeKind>>();
        for vec in &bound {
            for (i, expr) in vec.iter().enumerate() {
                if let Some(tmp) = Self::check_compat(types[i], expr.return_type()) {
                    types[i] = tmp;
                } else {
                    return Err(RwError::from(ErrorCode::NotImplementedError(
                        "bind_values".to_string(),
                    )));
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

    fn check_compat(left: DataTypeKind, right: DataTypeKind) -> Option<DataTypeKind> {
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
