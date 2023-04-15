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
use risingwave_common::catalog::{ColumnCatalog, Schema, TableVersionId};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Ident, ObjectName, Query, SelectItem};

use super::statement::RewriteExprsRecursive;
use super::BoundQuery;
use crate::binder::Binder;
use crate::catalog::TableId;
use crate::expr::{ExprImpl, InputRef};
use crate::user::UserId;

#[derive(Debug, Clone)]
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

    /// Columns that user fails to specify
    /// Will set to default value (current null)
    pub default_columns: Option<Vec<(usize, ExprImpl)>>,

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
        cols_to_insert_by_user: Vec<Ident>,
        source: Query,
        returning_items: Vec<SelectItem>,
    ) -> Result<BoundInsert> {
        let (schema_name, table_name) = Self::resolve_schema_qualified_name(&self.db_name, name)?;
        self.bind_table(schema_name.as_deref(), &table_name, None)?;

        let table_catalog = self.resolve_dml_table(schema_name.as_deref(), &table_name, true)?;
        let table_id = table_catalog.id;
        let owner = table_catalog.owner;
        let table_version_id = table_catalog.version_id().expect("table must be versioned");
        let cols_to_insert_in_table = table_catalog.columns_to_insert().cloned().collect_vec();

        let generated_column_names: HashSet<_> = table_catalog.generated_column_names().collect();
        for col in &cols_to_insert_by_user {
            let query_col_name = col.real_value();
            if generated_column_names.contains(query_col_name.as_str()) {
                return Err(RwError::from(ErrorCode::BindError(format!(
                    "cannot insert a non-DEFAULT value into column \"{0}\".  Column \"{0}\" is a generated column.",
                    &query_col_name
                ))));
            }
        }

        // TODO(yuhao): refine this if row_id is always the last column.
        //
        // `row_id_index` in insert operation should rule out generated column
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

        let (mut col_indices_to_insert, default_column_indices) = get_col_indices_to_insert(
            &cols_to_insert_in_table,
            &cols_to_insert_by_user,
            &table_name,
        )?;
        let expected_types: Vec<DataType> = col_indices_to_insert
            .iter()
            .map(|idx| cols_to_insert_in_table[*idx].data_type().clone())
            .collect();

        // When the column types of `source` query do not match `expected_types`,
        // casting is needed.
        //
        // In PG, when the `source` is a `VALUES` without order / limit / offset, special treatment
        // is given and it is NOT equivalent to assignment cast over potential implicit cast inside.
        // For example, the following is valid:
        //
        // ```
        //   create table t (v1 time);
        //   insert into t values (timestamp '2020-01-01 01:02:03'), (time '03:04:05');
        // ```
        //
        // But the followings are not:
        //
        // ```
        //   values (timestamp '2020-01-01 01:02:03'), (time '03:04:05');
        //   insert into t values (timestamp '2020-01-01 01:02:03'), (time '03:04:05') limit 1;
        // ```
        //
        // Because `timestamp` can cast to `time` in assignment context, but no casting between them
        // is allowed implicitly.
        //
        // In this case, assignment cast should be used directly in `VALUES`, suppressing its
        // internal implicit cast.
        // In other cases, the `source` query is handled on its own and assignment cast is done
        // afterwards.
        let bound_query;
        let cast_exprs;

        let bounded_column_nums = match source.as_simple_values() {
            None => {
                bound_query = self.bind_query(source)?;
                let actual_types = bound_query.data_types();
                cast_exprs = match expected_types == actual_types {
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
                bound_query.schema().len()
            }
            Some(values) => {
                let values_len = values
                    .0
                    .first()
                    .expect("values list should not be empty")
                    .len();
                let values = self.bind_values(values.clone(), Some(expected_types))?;
                bound_query = BoundQuery::with_values(values);
                cast_exprs = vec![];
                values_len
            }
        };

        let has_user_specified_columns = !cols_to_insert_by_user.is_empty();
        let num_target_cols = if has_user_specified_columns {
            cols_to_insert_by_user.len()
        } else {
            cols_to_insert_in_table.len()
        };

        let (err_msg, default_column_indices) = match num_target_cols.cmp(&bounded_column_nums) {
            std::cmp::Ordering::Equal => (None, default_column_indices),
            std::cmp::Ordering::Greater => {
                if has_user_specified_columns {
                    // e.g. insert into t (v1, v2) values (7)
                    (
                        Some("INSERT has more target columns than expressions"),
                        None,
                    )
                } else {
                    // e.g. create table t (a int, b real)
                    //      insert into t values (7)
                    // this kind of usage is fine, null values will be provided
                    // implicitly.
                    let mut new_default_column_indices =
                        col_indices_to_insert.split_off(bounded_column_nums);
                    new_default_column_indices.extend(default_column_indices.unwrap_or_default());
                    (None, Some(new_default_column_indices))
                }
            }
            std::cmp::Ordering::Less => {
                // e.g. create table t (a int, b real)
                //      insert into t (v1) values (7, 13)
                // or   insert into t values (7, 13, 17)
                (
                    Some("INSERT has more expressions than target columns"),
                    None,
                )
            }
        };
        if let Some(msg) = err_msg {
            return Err(RwError::from(ErrorCode::BindError(msg.to_string())));
        }

        let default_columns = if let Some(default_column_indices) = default_column_indices {
            Some(
                default_column_indices
                    .into_iter()
                    .map(|i| {
                        (
                            i,
                            ExprImpl::literal_null(cols_to_insert_in_table[i].data_type().clone()),
                        )
                    })
                    .collect_vec(),
            )
        } else {
            None
        };

        println!(
            "col_indices_to_insert: {:?}, default_column_indices: {:?}",
            col_indices_to_insert.clone(),
            default_column_indices.clone().unwrap_or_default()
        );

        let insert = BoundInsert {
            table_id,
            table_version_id,
            table_name,
            owner,
            row_id_index,
            column_indices: col_indices_to_insert,
            default_columns,
            source: bound_query,
            cast_exprs,
            returning_list,
            returning_schema: if is_returning {
                Some(Schema { fields })
            } else {
                None
            },
        };
        Ok(insert)
    }

    /// Cast a list of `exprs` to corresponding `expected_types` IN ASSIGNMENT CONTEXT. Make sure
    /// you understand the difference of implicit, assignment and explicit cast before reusing it.
    pub(super) fn cast_on_insert(
        expected_types: &Vec<DataType>,
        exprs: Vec<ExprImpl>,
    ) -> Result<Vec<ExprImpl>> {
        let msg = match expected_types.len().cmp(&exprs.len()) {
            std::cmp::Ordering::Less => "INSERT has more expressions than target columns",
            _ => {
                return exprs
                    .into_iter()
                    .zip(expected_types)
                    .map(|(e, t)| e.cast_assign(t.clone()).map_err(Into::into))
                    .try_collect();
            }
        };
        Err(ErrorCode::BindError(msg.into()).into())
    }
}

/// Returned indices have the same length as `cols_to_insert_in_table`.
/// The first elements have the same order as `cols_to_insert_by_user`.
/// The rest are what's not specified by the user.
///
/// Also checks there are no duplicate nor unknown columns provided by the user.
fn get_col_indices_to_insert(
    cols_to_insert_in_table: &[ColumnCatalog],
    cols_to_insert_by_user: &[Ident],
    table_name: &str,
) -> Result<(Vec<usize>, Option<Vec<usize>>)> {
    if cols_to_insert_by_user.is_empty() {
        return Ok(((0..cols_to_insert_in_table.len()).collect(), None));
    }

    let mut col_indices_to_insert: Vec<usize> = Vec::new();

    let mut col_name_to_idx: HashMap<String, usize> = HashMap::new();
    for (col_idx, col) in cols_to_insert_in_table.iter().enumerate() {
        col_name_to_idx.insert(col.name().to_string(), col_idx);
    }

    for col_name in cols_to_insert_by_user {
        let col_name = &col_name.real_value();
        match col_name_to_idx.get_mut(col_name) {
            Some(value_ref) => {
                if *value_ref == usize::MAX {
                    return Err(RwError::from(ErrorCode::BindError(
                        "Column specified more than once".to_string(),
                    )));
                }
                col_indices_to_insert.push(*value_ref);
                *value_ref = usize::MAX; // mark this column name, for duplicate
                                         // detection
            }
            None => {
                // Invalid column name found
                return Err(RwError::from(ErrorCode::BindError(format!(
                    "Column {} not found in table {}",
                    col_name, table_name
                ))));
            }
        }
    }

    // columns that are in the target table but not in the provided target columns
    let default_column_indices = if col_indices_to_insert.len() != cols_to_insert_in_table.len() {
        let mut cols = vec![];
        for col in cols_to_insert_in_table {
            if let Some(col_to_insert_idx) = col_name_to_idx.get(col.name()) {
                if *col_to_insert_idx != usize::MAX {
                    cols.push(*col_to_insert_idx);
                }
            } else {
                unreachable!();
            }
        }
        Some(cols)
    } else {
        None
    };

    Ok((col_indices_to_insert, default_column_indices))
}
