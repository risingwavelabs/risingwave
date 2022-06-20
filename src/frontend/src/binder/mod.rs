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

use std::collections::HashMap;

use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{Statement, TableAlias};

pub mod bind_context;
mod delete;
pub(crate) mod expr;
mod insert;
mod query;
mod relation;
mod select;
mod set_expr;
mod statement;
mod struct_field;
mod update;
mod values;

pub use bind_context::BindContext;
pub use delete::BoundDelete;
pub use expr::bind_data_type;
pub use insert::BoundInsert;
pub use query::BoundQuery;
pub use relation::{
    BoundBaseTable, BoundJoin, BoundSource, BoundTableFunction, BoundTableSource,
    BoundWindowTableFunction, FunctionType, Relation, WindowTableFunctionKind,
};
pub use select::BoundSelect;
pub use set_expr::BoundSetExpr;
pub use statement::BoundStatement;
pub use update::BoundUpdate;
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
    /// See [`Binder::bind_subquery_expr`] for details.
    upper_contexts: Vec<BindContext>,

    next_subquery_id: usize,
    /// Map the cte's name to its Relation::Subquery.
    cte_to_relation: HashMap<String, (BoundQuery, TableAlias)>,
}

impl Binder {
    pub fn new(catalog: CatalogReadGuard, db_name: String) -> Binder {
        Binder {
            catalog,
            db_name,
            context: BindContext::new(),
            upper_contexts: vec![],
            next_subquery_id: 0,
            cte_to_relation: HashMap::new(),
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

    fn next_subquery_id(&mut self) -> usize {
        let id = self.next_subquery_id;
        self.next_subquery_id += 1;
        id
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
pub const UNNAMED_COLUMN: &str = "?column?";
/// The table name stored in [`BindContext`] for a subquery without an alias.
const UNNAMED_SUBQUERY: &str = "?subquery?";
