// Copyright 2025 RisingWave Labs
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

use std::collections::{BTreeMap, HashMap};

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::acl::AclMode;
use risingwave_common::catalog::{Schema, TableVersionId};
use risingwave_common::types::StructType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::user::grant_privilege::PbObject;
use risingwave_sqlparser::ast::{Assignment, AssignmentValue, Expr, ObjectName, SelectItem};

use super::statement::RewriteExprsRecursive;
use super::{Binder, BoundBaseTable};
use crate::TableCatalog;
use crate::catalog::TableId;
use crate::error::{ErrorCode, Result, RwError, bail_bind_error, bind_error};
use crate::expr::{Expr as _, ExprImpl, SubqueryKind};
use crate::handler::privilege::ObjectCheckItem;
use crate::user::UserId;

/// Project into `exprs` in `BoundUpdate` to get the new values for updating.
#[derive(Debug, Clone, Copy)]
pub enum UpdateProject {
    /// Use the expression at the given index in `exprs`.
    Simple(usize),
    /// Use the `i`-th field of the expression (returning a struct) at the given index in `exprs`.
    Composite(usize, usize),
}

impl UpdateProject {
    /// Offset the index by `i`.
    pub fn offset(self, i: usize) -> Self {
        match self {
            UpdateProject::Simple(index) => UpdateProject::Simple(index + i),
            UpdateProject::Composite(index, j) => UpdateProject::Composite(index + i, j),
        }
    }
}

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
    pub table: BoundBaseTable,

    pub selection: Option<ExprImpl>,

    /// Expression used to evaluate the new values for the columns.
    pub exprs: Vec<ExprImpl>,

    /// Mapping from the index of the column to be updated, to the index of the expression in `exprs`.
    ///
    /// By constructing two `Project` nodes with `exprs` and `projects`, we can get the new values.
    pub projects: HashMap<usize, UpdateProject>,

    // used for the 'RETURNING" keyword to indicate the returning items and schema
    // if the list is empty and the schema is None, the output schema will be a INT64 as the
    // affected row cnt
    pub returning_list: Vec<ExprImpl>,

    pub returning_schema: Option<Schema>,
}

impl RewriteExprsRecursive for BoundUpdate {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
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

fn get_col_referenced_by_generated_pk(table_catalog: &TableCatalog) -> Result<FixedBitSet> {
    let column_num = table_catalog.columns().len();
    let pk_col_id = table_catalog.pk_column_ids();
    let mut bitset = FixedBitSet::with_capacity(column_num);

    let generated_pk_col_exprs = table_catalog
        .columns
        .iter()
        .filter(|c| pk_col_id.contains(&c.column_id()))
        .flat_map(|c| c.generated_expr());
    for expr_node in generated_pk_col_exprs {
        let expr = ExprImpl::from_expr_proto(expr_node)?;
        bitset.union_with(&expr.collect_input_refs(column_num));
    }
    Ok(bitset)
}

impl Binder {
    pub(super) fn bind_update(
        &mut self,
        name: ObjectName,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
        returning_items: Vec<SelectItem>,
    ) -> Result<BoundUpdate> {
        let (schema_name, table_name) = Self::resolve_schema_qualified_name(&self.db_name, name)?;
        let table = self.bind_table(schema_name.as_deref(), &table_name)?;

        let table_catalog = &table.table_catalog;
        Self::check_for_dml(table_catalog, false)?;
        self.check_privilege(
            ObjectCheckItem::new(
                table_catalog.owner,
                AclMode::Update,
                table_name.clone(),
                PbObject::TableId(table_catalog.id.table_id),
            ),
            table_catalog.database_id,
        )?;

        let default_columns_from_catalog =
            table_catalog.default_columns().collect::<BTreeMap<_, _>>();
        if !returning_items.is_empty() && table_catalog.has_generated_column() {
            return Err(RwError::from(ErrorCode::BindError(
                "`RETURNING` clause is not supported for tables with generated columns".to_owned(),
            )));
        }

        let table_id = table_catalog.id;
        let owner = table_catalog.owner;
        let table_version_id = table_catalog.version_id().expect("table must be versioned");
        let cols_refed_by_generated_pk = get_col_referenced_by_generated_pk(table_catalog)?;

        let selection = selection.map(|expr| self.bind_expr(expr)).transpose()?;

        let mut exprs = Vec::new();
        let mut projects = HashMap::new();

        macro_rules! record {
            ($id:expr, $project:expr) => {
                let id_index = $id.as_input_ref().unwrap().index;
                projects
                    .try_insert(id_index, $project)
                    .map_err(|_e| bind_error!("multiple assignments to the same column"))?;
            };
        }

        for Assignment { id, value } in assignments {
            let ids: Vec<_> = id
                .iter()
                .map(|id| self.bind_expr(Expr::Identifier(id.clone())))
                .try_collect()?;

            match (ids.as_slice(), value) {
                // `SET col1 = DEFAULT`, `SET (col1, col2, ...) = DEFAULT`
                (ids, AssignmentValue::Default) => {
                    for id in ids {
                        let id_index = id.as_input_ref().unwrap().index;
                        let expr = default_columns_from_catalog
                            .get(&id_index)
                            .cloned()
                            .unwrap_or_else(|| ExprImpl::literal_null(id.return_type()));

                        exprs.push(expr);
                        record!(id, UpdateProject::Simple(exprs.len() - 1));
                    }
                }

                // `SET col1 = expr`
                ([id], AssignmentValue::Expr(expr)) => {
                    let expr = self.bind_expr(expr)?.cast_assign(id.return_type())?;
                    exprs.push(expr);
                    record!(id, UpdateProject::Simple(exprs.len() - 1));
                }
                // `SET (col1, col2, ...) = (val1, val2, ...)`
                (ids, AssignmentValue::Expr(Expr::Row(values))) => {
                    if ids.len() != values.len() {
                        bail_bind_error!("number of columns does not match number of values");
                    }

                    for (id, value) in ids.iter().zip_eq_fast(values) {
                        let expr = self.bind_expr(value)?.cast_assign(id.return_type())?;
                        exprs.push(expr);
                        record!(id, UpdateProject::Simple(exprs.len() - 1));
                    }
                }
                // `SET (col1, col2, ...) = (SELECT ...)`
                (ids, AssignmentValue::Expr(Expr::Subquery(subquery))) => {
                    let expr = self.bind_subquery_expr(*subquery, SubqueryKind::UpdateSet)?;

                    if expr.return_type().as_struct().len() != ids.len() {
                        bail_bind_error!("number of columns does not match number of values");
                    }

                    let target_type = StructType::new(
                        id.iter()
                            .zip_eq_fast(ids)
                            .map(|(id, expr)| (id.real_value(), expr.return_type())),
                    )
                    .into();
                    let expr = expr.cast_assign(target_type)?;

                    exprs.push(expr);

                    for (i, id) in ids.iter().enumerate() {
                        record!(id, UpdateProject::Composite(exprs.len() - 1, i));
                    }
                }

                (_ids, AssignmentValue::Expr(_expr)) => {
                    bail_bind_error!(
                        "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression"
                    );
                }
            }
        }

        // Check whether updating these columns is allowed.
        for &id_index in projects.keys() {
            if (table.table_catalog.pk())
                .iter()
                .any(|k| k.column_index == id_index)
            {
                return Err(ErrorCode::BindError(
                    "update modifying the PK column is unsupported".to_owned(),
                )
                .into());
            }
            if (table.table_catalog.generated_col_idxes()).contains(&id_index) {
                return Err(ErrorCode::BindError(
                    "update modifying the generated column is unsupported".to_owned(),
                )
                .into());
            }
            if cols_refed_by_generated_pk.contains(id_index) {
                return Err(ErrorCode::BindError(
                    "update modifying the column referenced by generated columns that are part of the primary key is not allowed".to_owned(),
                )
                .into());
            }

            let col = &table.table_catalog.columns()[id_index];
            if !col.can_dml() {
                bail_bind_error!("update modifying column `{}` is unsupported", col.name());
            }
        }

        let (returning_list, fields) = self.bind_returning_list(returning_items)?;
        let returning = !returning_list.is_empty();

        Ok(BoundUpdate {
            table_id,
            table_version_id,
            table_name,
            owner,
            table,
            selection,
            projects,
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
