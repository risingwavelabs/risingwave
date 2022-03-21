// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
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
mod relation;
mod values;

pub use bind_context::BindContext;
pub use delete::BoundDelete;
pub use insert::BoundInsert;
pub use query::BoundQuery;
pub use select::BoundSelect;
pub use set_expr::BoundSetExpr;
pub use statement::BoundStatement;
pub use relation::{BoundBaseTable, BoundJoin, Relation};
pub use values::BoundValues;

use crate::catalog::catalog_service::CatalogReadGuard;

/// `Binder` binds the identifiers in AST to columns in relations
pub struct Binder {
    // TODO: maybe we can only lock the database, but not the whole catalog.
    catalog: CatalogReadGuard,
    db_name: String,
    context: BindContext,
    /// A stack holding contexts of outer queries when binding a subquery.
    ///
    /// See [`Binder::bind_subquery`] for details.
    upper_contexts: Vec<BindContext>,
}

impl Binder {
    pub fn new(catalog: CatalogReadGuard, db_name: String) -> Binder {
        Binder {
            catalog,
            db_name,
            context: BindContext::new(),
            upper_contexts: vec![],
        }
    }

    /// Bind a [`Statement`].
    pub fn bind(&mut self, stmt: Statement) -> Result<BoundStatement> {
        self.bind_statement(stmt)
    }

    fn get_schema_by_name(&self, schema_name: &str) -> Option<&SchemaCatalog> {
        self.catalog.get_schema_by_name(&self.db_name, schema_name)
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

#[cfg(test)]
pub mod test_utils {
    use std::sync::Arc;

    use parking_lot::RwLock;

    use super::Binder;
    use crate::catalog::catalog_service::CatalogReader;
    use crate::catalog::root_catalog::Catalog;

    #[cfg(test)]
    pub fn mock_binder_with_catalog(catalog: Catalog, db_name: String) -> Binder {
        let catalog = Arc::new(RwLock::new(catalog));
        let catalog_reader = CatalogReader::new(catalog);
        Binder::new(catalog_reader.read_guard(), db_name)
    }
    #[cfg(test)]
    pub fn mock_binder() -> Binder {
        mock_binder_with_catalog(Catalog::default(), "".to_string())
    }
}

/// The column name stored in [`BindContext`] for a column without an alias.
const UNNAMED_COLUMN: &str = "?column?";
/// The table name stored in [`BindContext`] for a subquery without an alias.
const UNNAMED_SUBQUERY: &str = "?subquery?";
