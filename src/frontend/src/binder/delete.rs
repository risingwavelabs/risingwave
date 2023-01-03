// Copyright 2023 Singularity Data
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

use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{Expr, ObjectName};

use super::{Binder, BoundBaseTable};
use crate::catalog::TableId;
use crate::expr::ExprImpl;
use crate::user::UserId;

#[derive(Debug)]
pub struct BoundDelete {
    /// Id of the table to perform deleting.
    pub table_id: TableId,

    /// Name of the table to perform deleting.
    pub table_name: String,

    /// Owner of the table to perform deleting.
    pub owner: UserId,

    /// Used for scanning the records to delete with the `selection`.
    pub table: BoundBaseTable,

    pub selection: Option<ExprImpl>,
}

impl Binder {
    pub(super) fn bind_delete(
        &mut self,
        name: ObjectName,
        selection: Option<Expr>,
    ) -> Result<BoundDelete> {
        let (schema_name, table_name) = Self::resolve_schema_qualified_name(&self.db_name, name)?;
        let schema_name = schema_name.as_deref();

        let table_catalog = self.resolve_dml_table(schema_name, &table_name, false)?;
        let table_id = table_catalog.id;
        let owner = table_catalog.owner;

        let table = self.bind_table(schema_name, &table_name, None)?;

        Ok(BoundDelete {
            table_id,
            table_name,
            owner,
            table,
            selection: selection.map(|expr| self.bind_expr(expr)).transpose()?,
        })
    }
}
