use std::sync::Arc;

use risingwave_common::error::Result;
use risingwave_sqlparser::ast::Statement;

mod expr;
mod query;
mod select;
mod set_expr;
mod statement;
mod table_ref;
mod values;

pub use query::BoundQuery;
pub use select::BoundSelect;
pub use set_expr::BoundSetExpr;
pub use statement::BoundStatement;
pub use table_ref::{BaseTableRef, TableRef};
pub use values::BoundValues;

use crate::catalog::database_catalog::DatabaseCatalog;

pub struct Binder {
    #[allow(dead_code)]
    catalog: Arc<DatabaseCatalog>,
}

impl Binder {
    pub fn new(catalog: Arc<DatabaseCatalog>) -> Binder {
        Binder { catalog }
    }
    pub fn bind(&mut self, stmt: Statement) -> Result<BoundStatement> {
        self.bind_statement(stmt)
    }
}
