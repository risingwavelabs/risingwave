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

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_sqlparser::ast::{Expr, ObjectName, SelectItem};

use super::{Binder, BoundBaseTable, BoundTableSource, UNNAMED_COLUMN};
use crate::expr::{Expr as _, ExprImpl};

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

    pub returning_list: Vec<ExprImpl>,

    pub schema: Schema,
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

        let table = self.bind_table(schema_name, &table_name, None)?;
        let (returning_list, aliases) = self.bind_select_list(returning_items)?;
        if returning_list
            .iter()
            .any(|expr| expr.has_agg_call() || expr.has_window_function())
        {
            return Err(RwError::from(ErrorCode::BindError(
                "DELETE RETURNING should not have agg/window".to_string(),
            )));
        }

        let fields = returning_list
            .iter()
            .zip_eq(aliases.iter())
            .map(|(s, a)| {
                let name = a.clone().unwrap_or_else(|| UNNAMED_COLUMN.to_string());
                Ok(Field::with_name(s.return_type(), name))
            })
            .collect::<Result<Vec<Field>>>()?;
        let delete = BoundDelete {
            table_id,
            table_name,
            owner,
            table,
            selection: selection.map(|expr| self.bind_expr(expr)).transpose()?,
            returning_list,
            schema: Schema { fields },
        };
        Ok(delete)
    }
}
