// Copyright 2022 RisingWave Labs
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

use anyhow::Context;
use itertools::Itertools;
use risingwave_common::acl::AclMode;
use risingwave_common::catalog::{ColumnCatalog, Schema, TableVersionId};
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::expr::expr_node::Type as ExprType;
use risingwave_pb::plan_common::DefaultColumnDesc;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_sqlparser::ast::{Ident, ObjectName, Query, SelectItem};

use super::BoundQuery;
use super::statement::RewriteExprsRecursive;
use crate::binder::{Binder, Clause};
use crate::catalog::TableId;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{Expr, ExprImpl, FunctionCall, InputRef};
use crate::handler::privilege::ObjectCheckItem;
use crate::user::UserId;
use crate::utils::ordinal;

#[derive(Debug, Clone)]
pub struct BoundInsert {
    /// Id of the table to perform inserting.
    pub table_id: TableId,

    /// Version id of the table.
    pub table_version_id: TableVersionId,

    /// Name of the table to perform inserting.
    pub table_name: String,

    /// All visible columns of the table, used as the output schema of `Insert` plan node if
    /// `RETURNING` is specified.
    pub table_visible_columns: Vec<ColumnCatalog>,

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
    pub default_columns: Vec<(usize, ExprImpl)>,

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
        let (schema_name, table_name) = Self::resolve_schema_qualified_name(&self.db_name, &name)?;
        // bind insert table
        self.context.clause = Some(Clause::Insert);
        let bound_table = self.bind_table(schema_name.as_deref(), &table_name)?;
        let table_catalog = &bound_table.table_catalog;
        Self::check_for_dml(table_catalog, true)?;
        self.check_privilege(
            ObjectCheckItem::new(
                table_catalog.owner,
                AclMode::Insert,
                table_name.clone(),
                table_catalog.id,
            ),
            table_catalog.database_id,
        )?;

        let has_user_specified_columns = !cols_to_insert_by_user.is_empty();
        let table_id = table_catalog.id;
        let owner = table_catalog.owner;
        let table_version_id = table_catalog.version_id().expect("table must be versioned");
        let table_visible_columns = table_catalog
            .columns()
            .iter()
            .filter(|c| !c.is_hidden())
            .cloned()
            .collect_vec();
        let cols_to_insert_in_table = table_catalog.columns_to_insert().cloned().collect_vec();
        // Reorder default columns based on `cols_to_insert_in_table`.
        let default_columns_from_catalog = cols_to_insert_in_table
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if let Some(GeneratedOrDefaultColumn::DefaultColumn(DefaultColumnDesc {
                    expr,
                    ..
                })) = col.column_desc.generated_or_default_column.as_ref()
                {
                    Some((
                        idx,
                        ExprImpl::from_expr_proto(expr.as_ref().unwrap())
                            .expect("expr in default columns corrupted"),
                    ))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();

        let generated_column_names = table_catalog
            .generated_column_names()
            .collect::<HashSet<_>>();

        let check_generated_insert_violation = |bound_column_nums: Option<usize>| -> Result<()> {
            let generated_column_name = if let Some(column_num) = bound_column_nums {
                table_catalog
                    .first_generated_column()
                    .and_then(|(index, column_name)| {
                        (column_num > index).then_some(column_name.to_owned())
                    })
            } else {
                cols_to_insert_by_user
                    .iter()
                    .map(|col| col.real_value())
                    .find(|col_name| generated_column_names.contains(col_name.as_str()))
            };

            if let Some(column_name) = generated_column_name {
                return Err(ErrorCode::InsertViolation(format!(
                    "cannot insert a non-DEFAULT value into column \"{0}\"\n  DETAIL: Column \"{0}\" is a generated column.",
                    column_name
                ))
                .into());
            }

            Ok(())
        };

        if has_user_specified_columns {
            check_generated_insert_violation(None)?;
        }

        if !generated_column_names.is_empty() && !returning_items.is_empty() {
            return Err(RwError::from(ErrorCode::BindError(
                "`RETURNING` clause is not supported for tables with generated columns".to_owned(),
            )));
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
        // Collect the types of columns explicitly specified by the user.
        let expected_types: Vec<DataType> = col_indices_to_insert
            .iter()
            .map(|idx| cols_to_insert_in_table[*idx].data_type().clone())
            .collect();

        // Collect the nullable of columns explicitly specified by the user.
        let nullables: Vec<(bool, &str)> = col_indices_to_insert
            .iter()
            .map(|idx| {
                (
                    cols_to_insert_in_table[*idx].nullable(),
                    cols_to_insert_in_table[*idx].name(),
                )
            })
            .collect();

        // When the column types of `source` query do not match `expected_types`,
        // casting is needed.
        //
        // In PG, when the `source` is a `VALUES` without order / limit / offset, special treatment
        // is given and it is NOT equivalent to assignment cast over potential implicit cast inside.
        // For example, the following is valid:
        //
        // ```sql
        //   create table t (v1 time);
        //   insert into t values (timestamp '2020-01-01 01:02:03'), (time '03:04:05');
        // ```
        //
        // But the followings are not:
        //
        // ```sql
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
        let all_nullable = nullables.iter().all(|(nullable, _)| *nullable);

        let bound_column_nums = match source.as_simple_values() {
            None => {
                bound_query = self.bind_query(&source)?;
                if !has_user_specified_columns {
                    check_generated_insert_violation(Some(bound_query.schema().len()))?;
                }
                let actual_types = bound_query.data_types();
                let type_match = expected_types == actual_types;
                cast_exprs = if all_nullable && type_match {
                    vec![]
                } else {
                    let mut cast_exprs = actual_types
                        .into_iter()
                        .enumerate()
                        .map(|(i, t)| InputRef::new(i, t).into())
                        .collect();
                    if !type_match {
                        cast_exprs = Self::cast_on_insert(&expected_types, cast_exprs)?
                    }
                    if !all_nullable {
                        cast_exprs =
                            Self::check_not_null(&nullables, cast_exprs, table_name.as_str())?
                    }
                    cast_exprs
                };
                bound_query.schema().len()
            }
            Some(values) => {
                let values_len = values
                    .0
                    .first()
                    .expect("values list should not be empty")
                    .len();
                if !has_user_specified_columns {
                    check_generated_insert_violation(Some(values_len))?;
                }
                let mut values = self.bind_values(values, Some(&expected_types))?;
                // let mut bound_values = values.clone();

                if !all_nullable {
                    values.rows = values
                        .rows
                        .into_iter()
                        .map(|vec| Self::check_not_null(&nullables, vec, table_name.as_str()))
                        .try_collect()?;
                }

                bound_query = BoundQuery::with_values(values);
                cast_exprs = vec![];
                values_len
            }
        };

        let num_target_cols = if has_user_specified_columns {
            cols_to_insert_by_user.len()
        } else {
            cols_to_insert_in_table.len()
        };

        let (err_msg, default_column_indices) = match num_target_cols.cmp(&bound_column_nums) {
            std::cmp::Ordering::Equal => (None, default_column_indices),
            std::cmp::Ordering::Greater => {
                if has_user_specified_columns {
                    // e.g. insert into t (v1, v2) values (7)
                    (
                        Some("INSERT has more target columns than expressions"),
                        vec![],
                    )
                } else {
                    // e.g. create table t (a int, b real)
                    //      insert into t values (7)
                    // this kind of usage is fine, null values will be provided
                    // implicitly.
                    (None, col_indices_to_insert.split_off(bound_column_nums))
                }
            }
            std::cmp::Ordering::Less => {
                // e.g. create table t (a int, b real)
                //      insert into t (v1) values (7, 13)
                // or   insert into t values (7, 13, 17)
                (
                    Some("INSERT has more expressions than target columns"),
                    vec![],
                )
            }
        };
        if let Some(msg) = err_msg {
            return Err(RwError::from(ErrorCode::BindError(msg.to_owned())));
        }

        let default_columns = default_column_indices
            .into_iter()
            .map(|i| {
                let column = &cols_to_insert_in_table[i];
                let expr = default_columns_from_catalog
                    .get(&i)
                    .cloned()
                    .unwrap_or_else(|| ExprImpl::literal_null(column.data_type().clone()));

                let expr = if column.nullable() {
                    expr
                } else {
                    FunctionCall::new_unchecked(
                        ExprType::CheckNotNull,
                        vec![
                            expr,
                            ExprImpl::literal_varchar(column.name().to_owned()),
                            ExprImpl::literal_varchar(table_name.clone()),
                        ],
                        column.data_type().clone(),
                    )
                    .into()
                };

                (i, expr)
            })
            .collect_vec();

        let insert = BoundInsert {
            table_id,
            table_version_id,
            table_name,
            table_visible_columns,
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
        expected_types: &[DataType],
        exprs: Vec<ExprImpl>,
    ) -> Result<Vec<ExprImpl>> {
        let msg = match expected_types.len().cmp(&exprs.len()) {
            std::cmp::Ordering::Less => "INSERT has more expressions than target columns",
            _ => {
                let expr_len = exprs.len();
                return exprs
                    .into_iter()
                    .zip_eq_fast(expected_types.iter().take(expr_len))
                    .enumerate()
                    .map(|(i, (e, t))| {
                        let res = e.cast_assign(t);
                        if expr_len > 1 {
                            res.with_context(|| {
                                format!("failed to cast the {} column", ordinal(i + 1))
                            })
                            .map_err(Into::into)
                        } else {
                            res.map_err(Into::into)
                        }
                    })
                    .try_collect();
            }
        };
        Err(ErrorCode::BindError(msg.into()).into())
    }

    /// Add not null check for the columns that are not nullable.
    pub(super) fn check_not_null(
        nullables: &Vec<(bool, &str)>,
        exprs: Vec<ExprImpl>,
        table_name: &str,
    ) -> Result<Vec<ExprImpl>> {
        let msg = match nullables.len().cmp(&exprs.len()) {
            std::cmp::Ordering::Less => "INSERT has more expressions than target columns",
            _ => {
                let expr_len = exprs.len();
                return exprs
                    .into_iter()
                    .zip_eq_fast(nullables.iter().take(expr_len))
                    .map(|(expr, (nullable, col_name))| {
                        if !nullable {
                            let return_type = expr.return_type();
                            let check_not_null = FunctionCall::new_unchecked(
                                ExprType::CheckNotNull,
                                vec![
                                    expr,
                                    ExprImpl::literal_varchar((*col_name).to_owned()),
                                    ExprImpl::literal_varchar(table_name.to_owned()),
                                ],
                                return_type,
                            );
                            // let res = expr.cast_assign(t.clone());
                            Ok(check_not_null.into())
                        } else {
                            Ok(expr)
                        }
                    })
                    .try_collect();
            }
        };
        Err(ErrorCode::BindError(msg.into()).into())
    }
}

/// # Parameters
/// - `cols_to_insert_in_table`: the list of columns that are visible and non-generated
///
/// - `cols_to_insert_by_user`:
///   The list of column identifiers explicitly specified by the user
///   in the INSERT statement.
///
/// - `table_name`: The name of the target table.
///
/// # Return
/// - `(col_indices_to_insert, default_column_indices)`
///
///   - `col_indices_to_insert`:
///     Column indices corresponding to user-specified columns in the INSERT statement,
///     preserving the user-defined order.
///
///   - `default_column_indices`:
///     Column indices of remaining columns in the target table that are not explicitly
///     provided by the user and should be filled with DEFAULT values.
fn get_col_indices_to_insert(
    cols_to_insert_in_table: &[ColumnCatalog],
    cols_to_insert_by_user: &[Ident],
    table_name: &str,
) -> Result<(Vec<usize>, Vec<usize>)> {
    if cols_to_insert_by_user.is_empty() {
        return Ok(((0..cols_to_insert_in_table.len()).collect(), vec![]));
    }

    let mut col_indices_to_insert: Vec<usize> = Vec::new();

    // Build a map from column name to column index in the table catalog
    let col_name_to_idx: HashMap<String, usize> = cols_to_insert_in_table
        .iter()
        .enumerate()
        .map(|(idx, col)| (col.name().to_owned(), idx))
        .collect();

    let mut seen = HashSet::with_capacity(cols_to_insert_by_user.len());
    for col_name in cols_to_insert_by_user {
        let col_name = col_name.real_value();
        if !seen.insert(col_name.clone()) {
            return Err(RwError::from(ErrorCode::BindError(
                "Column specified more than once".to_owned(),
            )));
        }

        let idx = col_name_to_idx.get(&col_name).ok_or_else(|| {
            RwError::from(ErrorCode::BindError(format!(
                "Column {} not found in table {}",
                col_name, table_name
            )))
        })?;

        col_indices_to_insert.push(*idx);
    }

    // columns that are in the target table but not in the provided target columns
    let default_column_indices = if col_indices_to_insert.len() != cols_to_insert_in_table.len() {
        cols_to_insert_in_table
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                let column_name = col.name();
                (!seen.contains(column_name)).then_some(idx)
            })
            .collect()
    } else {
        vec![]
    };

    Ok((col_indices_to_insert, default_column_indices))
}
