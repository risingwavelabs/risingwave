use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::SetExpr;

use crate::binder::{Binder, BoundSelect, BoundValues};

/// Part of a validated query, without order or limit clause. It may be composed of smaller
/// BoundSetExprs via set operators (e.g. union).
#[derive(Debug)]
pub enum BoundSetExpr {
    Select(Box<BoundSelect>),
    Values(Box<BoundValues>),
}

impl BoundSetExpr {
    pub fn names(&self) -> Vec<String> {
        match self {
            BoundSetExpr::Select(s) => s.names(),
            BoundSetExpr::Values(v) => v.schema.fields().iter().map(|f| f.name.clone()).collect(),
        }
    }

    pub fn data_types(&self) -> Vec<DataType> {
        match self {
            BoundSetExpr::Select(s) => s.data_types(),
            BoundSetExpr::Values(v) => v
                .schema
                .fields()
                .iter()
                .map(|f| f.data_type.clone())
                .collect(),
        }
    }
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
