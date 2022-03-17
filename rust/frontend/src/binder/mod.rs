use risingwave_common::error::Result;
use risingwave_sqlparser::ast::Statement;

use crate::catalog::schema_catalog::SchemaCatalog;

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

use crate::catalog::catalog_service::CatalogReadGuard;

/// `Binder` binds the identifiers in AST to columns in relations
pub struct Binder {
    // TODO: maybe we can only lock the database, but not the whole catalog.
    catalog: CatalogReadGuard,
    db_name: String,
    // TODO: support subquery.
    context: BindContext,
}

impl Binder {
    pub fn new(catalog: CatalogReadGuard, db_name: String) -> Binder {
        Binder {
            catalog,
            db_name,
            context: BindContext::new(),
        }
    }

    /// Bind a [`Statement`].
    pub fn bind(&mut self, stmt: Statement) -> Result<BoundStatement> {
        self.bind_statement(stmt)
    }

    fn get_schema_by_name(&self, schema_name: &String) -> Option<&SchemaCatalog> {
        self.catalog.get_schema_by_name(&self.db_name, schema_name)
    }
}

#[cfg(test)]
pub mod test_utils {
    use std::sync::Arc;

    use parking_lot::RwLock;

    use super::Binder;
    use crate::catalog::catalog::Catalog;
    use crate::catalog::catalog_service::CatalogReader;

    #[cfg(test)]
    pub fn mock_binder_with_catalog(catalog: Catalog, db_name: String) -> Binder {
        let catalog = Arc::new(RwLock::new(catalog));
        let catalog_reader = CatalogReader(catalog);
        Binder::new(catalog_reader.read_guard(), db_name)
    }
    #[cfg(test)]
    pub fn mock_binder() -> Binder {
        mock_binder_with_catalog(Catalog::default(), "".to_string())
    }
}
