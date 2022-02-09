use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::Values;

use crate::binder::Binder;
use crate::expr::{Expr as _, ExprImpl};

#[derive(Debug)]
pub struct BoundValues {
    pub rows: Vec<Vec<ExprImpl>>,
    pub schema: Schema,
}

impl Binder<'_> {
    pub(super) fn bind_values(&mut self, values: Values) -> Result<BoundValues> {
        let vec2d = values.0;
        let bound = vec2d
            .into_iter()
            .map(|vec| vec.into_iter().map(|expr| self.bind_expr(expr)).collect())
            .collect::<Result<Vec<Vec<_>>>>()?;
        // calc row type and insert casts here
        let types = bound[0].iter().map(|expr| expr.return_type());
        let schema = Schema::new(types.map(Field::unnamed).collect());
        Ok(BoundValues {
            rows: bound,
            schema,
        })
    }
}
