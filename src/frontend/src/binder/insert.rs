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

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::{Schema, TableVersionId};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_sqlparser::ast::{Ident, ObjectName, Query, SelectItem, SetExpr};

use super::statement::RewriteExprsRecursive;
use super::{BoundQuery, BoundSetExpr};
use crate::binder::Binder;
use crate::catalog::TableId;
use crate::expr::{ExprImpl, InputRef};
use crate::user::UserId;

#[derive(Debug)]
pub struct BoundInsert {
    /// Id of the table to perform inserting.
    pub table_id: TableId,

    /// Version id of the table.
    pub table_version_id: TableVersionId,

    /// Name of the table to perform inserting.
    pub table_name: String,

    /// Owner of the table to perform inserting.
    pub owner: UserId,

    // An optional column index of row ID. If the primary key is specified by the user,
    // this will be `None`.
    pub row_id_index: Option<usize>,

    /// User defined columns in which to insert
    /// Is equal to [0, 2, 1] for insert statement
    /// create table t1 (v1 int, v2 int, v3 int); insert into t1 (v1, v3, v2) values (5, 6, 7);
    /// Empty if user does not define insert columns
    pub column_indices: Vec<usize>,

    pub source: BoundQuery,

    /// Used as part of an extra `Project` when the column types of the query does not match
    /// those of the table. This does not include a simple `VALUE`. See comments in code for
    /// details.
    pub cast_exprs: Vec<ExprImpl>,

    // used for the 'RETURNING" keyword to indicate the returning items and schema
    // if the list is empty and the schema is None, the output schema will be a INT64 as the
    // affected row cnt
    pub returning_list: Vec<ExprImpl>,

    pub returning_schema: Option<Schema>,
}

impl RewriteExprsRecursive for BoundInsert {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        self.source.rewrite_exprs_recursive(rewriter);

        let new_cast_exprs = std::mem::take(&mut self.cast_exprs)
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect::<Vec<_>>();
        self.cast_exprs = new_cast_exprs;

        let new_returning_list = std::mem::take(&mut self.returning_list)
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect::<Vec<_>>();
        self.returning_list = new_returning_list;
    }
}

impl Binder {
    pub(super) fn bind_insert(
        &mut self,
        name: ObjectName,
        columns: Vec<Ident>,
        source: Query,
        returning_items: Vec<SelectItem>,
    ) -> Result<BoundInsert> {
        let (schema_name, table_name) = Self::resolve_schema_qualified_name(&self.db_name, name)?;
        self.bind_table(schema_name.as_deref(), &table_name, None)?;

        let table_catalog = self.resolve_dml_table(schema_name.as_deref(), &table_name, true)?;
        let table_id = table_catalog.id;
        let owner = table_catalog.owner;
        let table_version_id = table_catalog.version_id().expect("table must be versioned");
        let columns_to_insert = table_catalog.columns_to_insert().cloned().collect_vec();

        let generated_column_names: HashSet<_> = table_catalog.generated_column_names().collect();
        for query_col in &columns {
            let query_col_name = query_col.real_value();
            if generated_column_names.contains(query_col_name.as_str()) {
                return Err(RwError::from(ErrorCode::BindError(format!(
                    "cannot insert a non-DEFAULT value into column \"{0}\".  Column \"{0}\" is a generated column.",
                    &query_col_name
                ))));
            }
        }

        // TODO(yuhao): refine this if row_id is always the last column.
        //
        // `row_id_index` in bin insert operation should rule out generated column
        let row_id_index = {
            if let Some(row_id_index) = table_catalog.row_id_index {
                let mut cnt = 0;
                for col in table_catalog.columns().iter().take(row_id_index + 1) {
                    if col.is_generated() {
                        cnt += 1;
                    }
                }
                Some(row_id_index - cnt)
            } else {
                None
            }
        };

        let (returning_list, fields) = self.bind_returning_list(returning_items)?;
        let is_returning = !returning_list.is_empty();

        // create table t (a int, b real, c varchar)
        // case 1: insert into t (target_columns) values (values)
        // case 2: insert into t values (values)
        let has_target_col = columns.len() > 0;
        if has_target_col {
            let mut target_table_col_indices: Vec<usize> = vec![];
            let mut cols_to_insert_name_idx_map: HashMap<String, usize> = HashMap::new();
            for (col_idx, col) in columns_to_insert.iter().enumerate() {
                cols_to_insert_name_idx_map.insert(col.name().to_string(), col_idx);
            }
            for target_col in &columns {
                let target_col_name = &target_col.real_value();
                match cols_to_insert_name_idx_map.get_mut(target_col_name) {
                    Some(value_ref) => {
                        if *value_ref == usize::MAX {
                            return Err(RwError::from(ErrorCode::BindError(
                                "Column specified more than once".to_string(),
                            )));
                        }
                        target_table_col_indices.push(*value_ref);
                        *value_ref = usize::MAX;
                    }
                    None => {
                        // Invalid column name found
                        return Err(RwError::from(ErrorCode::BindError(format!(
                            "Column {} not found in table {}",
                            target_col_name, table_name
                        ))));
                    }
                }
            }

            // columns that are in the target table but not in the provided target columns
            if target_table_col_indices.len() != columns_to_insert.len() {
                for col in &columns_to_insert {
                    if let Some(col_to_insert_idx) = cols_to_insert_name_idx_map.get(col.name()) {
                        if *col_to_insert_idx != usize::MAX {
                            target_table_col_indices.push(*col_to_insert_idx);
                        }
                    } else {
                        unreachable!();
                    }
                }
            }

            let expected_types: Vec<DataType> = target_table_col_indices
                .iter()
                .map(|idx| columns_to_insert[*idx].data_type().clone())
                .collect();

            // When the column types of `source` query do not match `expected_types`, casting is
            // needed.
            //
            // In PG, when the `source` is a `VALUES` without order / limit / offset, special
            // treatment is given and it is NOT equivalent to assignment cast over
            // potential implicit cast inside. For example, the following is valid:
            // ```
            //   create table t (v1 time);
            //   insert into t values (timestamp '2020-01-01 01:02:03'), (time '03:04:05');
            // ```
            // But the followings are not:
            // ```
            //   values (timestamp '2020-01-01 01:02:03'), (time '03:04:05');
            //   insert into t values (timestamp '2020-01-01 01:02:03'), (time '03:04:05') limit 1;
            // ```
            // Because `timestamp` can cast to `time` in assignment context, but no casting between
            // them is allowed implicitly.
            //
            // In this case, assignment cast should be used directly in `VALUES`, suppressing its
            // internal implicit cast.
            // In other cases, the `source` query is handled on its own and assignment cast is done
            // afterwards.
            let (source, cast_exprs) = match source {
                Query {
                    with: None,
                    body: SetExpr::Values(values),
                    order_by: order,
                    limit: None,
                    offset: None,
                    fetch: None,
                } if order.is_empty() => {
                    assert!(!values.0.is_empty());
                    let err_msg = match columns.len().cmp(&values.0[0].len()) {
                        std::cmp::Ordering::Equal => None,
                        // e.g. create table t (a int, b real)
                        //      insert into t (v1, v2) values (7)
                        std::cmp::Ordering::Greater => {
                            Some("INSERT has more target columns than values")
                        }
                        // e.g. create table t (a int, b real)
                        //      insert into t (v1) values (7, 13)
                        std::cmp::Ordering::Less => {
                            Some("INSERT has less target columns than values")
                        }
                    };

                    if let Some(msg) = err_msg {
                        return Err(RwError::from(ErrorCode::BindError(msg.to_string())));
                    }

                    let values = self.bind_values(values, Some(expected_types.clone()))?;
                    let body = BoundSetExpr::Values(values.into());
                    (
                        BoundQuery {
                            body,
                            order: vec![],
                            limit: None,
                            offset: None,
                            with_ties: false,
                            extra_order_exprs: vec![],
                        },
                        vec![],
                    )
                }
                query => {
                    let bound = self.bind_query(query)?;
                    let actual_types = bound.data_types();
                    let cast_exprs = match expected_types == actual_types {
                        true => vec![],
                        false => Self::cast_on_insert(
                            &expected_types,
                            actual_types
                                .into_iter()
                                .enumerate()
                                .map(|(i, t)| InputRef::new(i, t).into())
                                .collect(),
                        )?,
                    };
                    (bound, cast_exprs)
                }
            };
            let insert = BoundInsert {
                table_id,
                table_version_id,
                table_name,
                owner,
                row_id_index,
                column_indices: target_table_col_indices,
                source,
                cast_exprs,
                returning_list,
                returning_schema: if is_returning {
                    Some(Schema { fields })
                } else {
                    None
                },
            };
            return Ok(insert);
        } else {
            let expected_types: Vec<DataType> = columns_to_insert
                .iter()
                .map(|c| c.data_type().clone())
                .collect();
            let (source, cast_exprs) = match source {
                Query {
                    with: None,
                    body: SetExpr::Values(values),
                    order_by: order,
                    limit: None,
                    offset: None,
                    fetch: None,
                } if order.is_empty() => {
                    assert!(!values.0.is_empty());
                    let err_msg = match columns_to_insert.len().cmp(&values.0[0].len()) {
                        std::cmp::Ordering::Equal => None,
                        // e.g. create table t (a int, b real)
                        //      insert into t values (7)
                        // this kind of usage is fine, null values will be provided implicitly.
                        std::cmp::Ordering::Greater => None,
                        // e.g. create table t (a int, b real)
                        //      insert into t values (7, 13, 17)
                        std::cmp::Ordering::Less => {
                            Some("INSERT has more expressions than target columns")
                        }
                    };

                    if let Some(msg) = err_msg {
                        return Err(RwError::from(ErrorCode::BindError(msg.to_string())));
                    }

                    let values = self.bind_values(values, Some(expected_types.clone()))?;
                    let body = BoundSetExpr::Values(values.into());
                    (
                        BoundQuery {
                            body,
                            order: vec![],
                            limit: None,
                            offset: None,
                            with_ties: false,
                            extra_order_exprs: vec![],
                        },
                        vec![],
                    )
                }
                query => {
                    let bound = self.bind_query(query)?;
                    let actual_types = bound.data_types();
                    let cast_exprs = match expected_types == actual_types {
                        true => vec![],
                        false => Self::cast_on_insert(
                            &expected_types,
                            actual_types
                                .into_iter()
                                .enumerate()
                                .map(|(i, t)| InputRef::new(i, t).into())
                                .collect(),
                        )?,
                    };
                    (bound, cast_exprs)
                }
            };
            let insert = BoundInsert {
                table_id,
                table_version_id,
                table_name,
                owner,
                row_id_index,
                column_indices: (0..columns_to_insert.len()).collect::<Vec<usize>>(),
                source,
                cast_exprs,
                returning_list,
                returning_schema: if is_returning {
                    Some(Schema { fields })
                } else {
                    None
                },
            };
            return Ok(insert);
        }
    }

    /// Cast a list of `exprs` to corresponding `expected_types` IN ASSIGNMENT CONTEXT. Make sure
    /// you understand the difference of implicit, assignment and explicit cast before reusing it.
    pub(super) fn cast_on_insert(
        expected_types: &Vec<DataType>,
        exprs: Vec<ExprImpl>,
    ) -> Result<Vec<ExprImpl>> {
        let msg = match expected_types.len().cmp(&exprs.len()) {
            std::cmp::Ordering::Equal => {
                return exprs
                    .into_iter()
                    .zip_eq_fast(expected_types)
                    .map(|(e, t)| e.cast_assign(t.clone()).map_err(Into::into))
                    .try_collect();
            }
            std::cmp::Ordering::Less => "INSERT has more expressions than target columns",
            std::cmp::Ordering::Greater => "INSERT has more target columns than expressions",
        };
        Err(ErrorCode::BindError(msg.into()).into())
    }
}
