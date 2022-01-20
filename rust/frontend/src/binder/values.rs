use risingwave_common::error::Result;
use risingwave_sqlparser::ast::Values;

use crate::binder::Binder;
use crate::expr::BoundExprImpl;

#[derive(Debug)]
pub struct BoundValues(pub Vec<Vec<BoundExprImpl>>);

impl Binder {
    pub(super) fn bind_values(&mut self, values: Values) -> Result<BoundValues> {
        let vec2d = values.0;
        let bound = vec2d
            .into_iter()
            .map(|vec| vec.into_iter().map(|expr| self.bind_expr(expr)).collect())
            .collect::<Result<_>>()?;
        // calc row type and insert casts here
        Ok(BoundValues(bound))
    }
}
