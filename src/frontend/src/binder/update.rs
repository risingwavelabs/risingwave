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

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use itertools::Itertools;
use risingwave_common::catalog::{Schema, TableVersionId};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_sqlparser::ast::{Assignment, AssignmentValue, Expr, ObjectName, SelectItem};

use super::statement::RewriteExprsRecursive;
use super::{Binder, Relation};
use crate::catalog::TableId;
use crate::expr::{Expr as _, ExprImpl};
use crate::user::UserId;

#[derive(Debug, Clone)]
pub struct BoundUpdate {
    /// Id of the table to perform updating.
    pub table_id: TableId,

    /// Version id of the table.
    pub table_version_id: TableVersionId,

    /// Name of the table to perform updating.
    pub table_name: String,

    /// Owner of the table to perform updating.
    pub owner: UserId,

    /// Used for scanning the records to update with the `selection`.
    pub table: Relation,

    pub selection: Option<ExprImpl>,

    /// Expression used to project to the updated row. The assigned columns will use the new
    /// expression, and the other columns will be simply `InputRef`.
    pub exprs: Vec<ExprImpl>,

    // used for the 'RETURNING" keyword to indicate the returning items and schema
    // if the list is empty and the schema is None, the output schema will be a INT64 as the
    // affected row cnt
    pub returning_list: Vec<ExprImpl>,

    pub returning_schema: Option<Schema>,
}

impl RewriteExprsRecursive for BoundUpdate {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        self.table.rewrite_exprs_recursive(rewriter);

        self.selection =
            std::mem::take(&mut self.selection).map(|expr| rewriter.rewrite_expr(expr));

        let new_exprs = std::mem::take(&mut self.exprs)
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect::<Vec<_>>();
        self.exprs = new_exprs;

        let new_returning_list = std::mem::take(&mut self.returning_list)
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect::<Vec<_>>();
        self.returning_list = new_returning_list;
    }
}

impl Binder {
    pub(super) fn bind_update(
        &mut self,
        name: ObjectName,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
        returning_items: Vec<SelectItem>,
    ) -> Result<BoundUpdate> {
        let (schema_name, table_name) =
            Self::resolve_schema_qualified_name(&self.db_name, name.clone())?;

        let table_catalog = self.resolve_dml_table(schema_name.as_deref(), &table_name, false)?;

        // TODO(yuhao): update a table with generated columns
        if table_catalog.has_generated_column() {
            return Err(ErrorCode::BindError(
                "Update a table with generated columns is not supported.".to_string(),
            )
            .into());
        }

        let pk_indices = table_catalog
            .pk()
            .iter()
            .map(|column_order| column_order.column_index)
            .collect_vec();
        let table_id = table_catalog.id;
        let owner = table_catalog.owner;
        let table_version_id = table_catalog.version_id().expect("table must be versioned");

        let table = self.bind_relation_by_name(name, None, false)?;

        let selection = selection.map(|expr| self.bind_expr(expr)).transpose()?;

        let mut assignment_exprs = HashMap::new();
        for Assignment { id, value } in assignments {
            // FIXME: Parsing of `id` is not strict. It will even treat `a.b` as `(a, b)`.
            let assignments = match (id.as_slice(), value) {
                // col = expr
                ([id], value) => {
                    vec![(id.clone(), value)]
                }

                // (col1, col2) = (subquery)
                (_ids, AssignmentValue::Expr(Expr::Subquery(_))) => {
                    return Err(ErrorCode::NotImplemented(
                        "subquery on the right side of multi-assignment".to_owned(),
                        None.into(),
                    )
                    .into())
                }
                // (col1, col2) = (expr1, expr2)
                // TODO: support `DEFAULT` in multiple assignments
                (ids, AssignmentValue::Expr(Expr::Row(values))) if ids.len() == values.len() => id
                    .into_iter()
                    .zip_eq_fast(values.into_iter().map(AssignmentValue::Expr))
                    .collect(),
                // (col1, col2) = <other expr>
                _ => {
                    return Err(ErrorCode::BindError(
                        "number of columns does not match number of values".to_owned(),
                    )
                    .into())
                }
            };

            for (id, value) in assignments {
                let id_expr = self.bind_expr(Expr::Identifier(id.clone()))?;
                if let Some(id_input_ref) = id_expr.clone().as_input_ref() {
                    let id_index = id_input_ref.index;
                    for &pk in &pk_indices {
                        if id_index == pk {
                            return Err(ErrorCode::BindError(
                                "update modifying the PK column is banned".to_owned(),
                            )
                            .into());
                        }
                    }
                }

                let value_expr = match value {
                    AssignmentValue::Expr(expr) => {
                        self.bind_expr(expr)?.cast_assign(id_expr.return_type())?
                    }
                    // TODO: specify default expression after we support non-`NULL` default values.
                    AssignmentValue::Default => ExprImpl::literal_null(id_expr.return_type()),
                };

                match assignment_exprs.entry(id_expr) {
                    Entry::Occupied(_) => {
                        return Err(ErrorCode::BindError(
                            "multiple assignments to same column".to_owned(),
                        )
                        .into())
                    }
                    Entry::Vacant(v) => {
                        v.insert(value_expr);
                    }
                }
            }
        }

        let all_columns = Self::iter_bound_columns(self.context.columns.iter()).0;
        let exprs = all_columns
            .into_iter()
            .map(|c| assignment_exprs.remove(&c).unwrap_or(c))
            .collect_vec();

        let (returning_list, fields) = self.bind_returning_list(returning_items)?;
        let returning = !returning_list.is_empty();

        Ok(BoundUpdate {
            table_id,
            table_version_id,
            table_name,
            owner,
            table,
            selection,
            exprs,
            returning_list,
            returning_schema: if returning {
                Some(Schema { fields })
            } else {
                None
            },
        })
    }
}
