use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::SetExpr;

use crate::binder::{Binder, BoundSelect, BoundValues};

/// Part of a validated query, without order or limit clause. It may be composed of smaller
/// BoundSetExprs via set operators (e.g. union).
#[derive(Debug, Clone)]
pub enum BoundSetExpr {
    Select(Box<BoundSelect>),
    Values(Box<BoundValues>),
}

impl Binder {
    pub(super) fn bind_set_expr(&mut self, set_expr: SetExpr) -> Result<BoundSetExpr> {
        match set_expr {
            SetExpr::Select(s) => Ok(BoundSetExpr::Select(Box::new(self.bind_select(*s)?))),
            SetExpr::Values(v) => Ok(BoundSetExpr::Values(Box::new(self.bind_values(v)?))),
            _ => Err(ErrorCode::NotImplementedError(format!("{:?}", set_expr)).into()),
        }
    }
}
