use std::sync::Arc;

use risingwave_common::error::Result;
use risingwave_sqlparser::ast::Statement;

mod bind_context;
mod delete;
pub(crate) mod expr;
mod insert;
mod query;
mod select;
mod set_expr;
mod statement;
mod table_ref;
mod values;

pub use bind_context::BindContext;
pub use delete::BoundDelete;
pub use insert::BoundInsert;
pub use query::BoundQuery;
pub use select::BoundSelect;
pub use set_expr::BoundSetExpr;
pub use statement::BoundStatement;
pub use table_ref::{BaseTableRef, BoundJoin, TableRef};
pub use values::BoundValues;

use crate::catalog::database_catalog::DatabaseCatalog;

/// `Binder` binds the identifiers in AST to columns in relations
pub struct Binder {
    #[allow(dead_code)]
    catalog: Arc<DatabaseCatalog>,
    context: BindContext,
    /// A stack holding contexts of outer queries when binding a subquery.
    ///
    /// See [`Binder::bind_subquery`] for details.
    upper_contexts: Vec<BindContext>,
}

impl Binder {
    pub fn new(catalog: Arc<DatabaseCatalog>) -> Binder {
        Binder {
            catalog,
            context: BindContext::new(),
            upper_contexts: vec![],
        }
    }

    /// Bind a [`Statement`].
    pub fn bind(&mut self, stmt: Statement) -> Result<BoundStatement> {
        self.bind_statement(stmt)
    }

    fn push_context(&mut self) {
        let new_context = std::mem::take(&mut self.context);
        self.upper_contexts.push(new_context);
    }

    fn pop_context(&mut self) {
        let old_context = self.upper_contexts.pop();
        self.context = old_context.unwrap();
    }
}

/// The column name stored in [`BindContext`] for a column without an alias.
const UNNAMED_COLUMN: &str = "?column?";
/// The table name stored in [`BindContext`] for a subquery without an alias.
const UNNAMED_SUBQUERY: &str = "?subquery?";
