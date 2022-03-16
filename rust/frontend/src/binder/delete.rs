use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{Expr, ObjectName};

use super::{BaseTableRef, Binder};
use crate::expr::ExprImpl;

#[derive(Debug, Clone)]
pub struct BoundDelete {
    pub table: BaseTableRef,
    pub selection: Option<ExprImpl>,
}

impl Binder {
    pub(super) fn bind_delete(
        &mut self,
        table_name: ObjectName,
        selection: Option<Expr>,
    ) -> Result<BoundDelete> {
        let delete = BoundDelete {
            table: self.bind_table(table_name)?,
            selection: selection.map(|expr| self.bind_expr(expr)).transpose()?,
        };

        Ok(delete)
    }
}
