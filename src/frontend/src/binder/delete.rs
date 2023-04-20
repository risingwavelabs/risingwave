// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::catalog::{Schema, TableVersionId};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_sqlparser::ast::{Expr, ObjectName, SelectItem};

use super::statement::RewriteExprsRecursive;
use super::{Binder, BoundBaseTable};
use crate::catalog::TableId;
use crate::expr::ExprImpl;
use crate::user::UserId;

#[derive(Debug, Clone)]
pub struct BoundDelete {
    /// Id of the table to perform deleting.
    pub table_id: TableId,

    /// Version id of the table.
    pub table_version_id: TableVersionId,

    /// Name of the table to perform deleting.
    pub table_name: String,

    /// Owner of the table to perform deleting.
    pub owner: UserId,

    /// Used for scanning the records to delete with the `selection`.
    pub table: BoundBaseTable,

    pub selection: Option<ExprImpl>,

    /// used for the 'RETURNING" keyword to indicate the returning items and schema
    /// if the list is empty and the schema is None, the output schema will be a INT64 as the
    /// affected row cnt
    pub returning_list: Vec<ExprImpl>,

    pub returning_schema: Option<Schema>,
}

impl RewriteExprsRecursive for BoundDelete {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        self.selection =
            std::mem::take(&mut self.selection).map(|expr| rewriter.rewrite_expr(expr));

        let new_returning_list = std::mem::take(&mut self.returning_list)
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect::<Vec<_>>();
        self.returning_list = new_returning_list;
    }
}

impl Binder {
    pub(super) fn bind_delete(
        &mut self,
        name: ObjectName,
        selection: Option<Expr>,
        returning_items: Vec<SelectItem>,
    ) -> Result<BoundDelete> {
        let (schema_name, table_name) = Self::resolve_schema_qualified_name(&self.db_name, name)?;
        let schema_name = schema_name.as_deref();

        let table_catalog = self.resolve_dml_table(schema_name, &table_name, false)?;
        let table_id = table_catalog.id;
        let owner = table_catalog.owner;
        let table_version_id = table_catalog.version_id().expect("table must be versioned");

        // TODO(yuhao): delete from table with generated columns
        if table_catalog.has_generated_column() {
            return Err(RwError::from(ErrorCode::BindError(
                "Delete from a table with generated column has not been implemented.".to_string(),
            )));
        }

        let table = self.bind_table(schema_name, &table_name, None)?;
        let (returning_list, fields) = self.bind_returning_list(returning_items)?;
        let returning = !returning_list.is_empty();
        let delete = BoundDelete {
            table_id,
            table_version_id,
            table_name,
            owner,
            table,
            selection: selection.map(|expr| self.bind_expr(expr)).transpose()?,
            returning_list,
            returning_schema: if returning {
                Some(Schema { fields })
            } else {
                None
            },
        };
        Ok(delete)
    }
}
