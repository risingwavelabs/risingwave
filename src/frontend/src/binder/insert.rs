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
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Ident, ObjectName, Query, SelectItem, SetExpr};

use super::{BoundQuery, BoundSetExpr};
use crate::catalog::TableId;
use crate::binder::{Binder, BoundTableSource};
use crate::expr::{ExprImpl, InputRef, Expr};
use crate::user::UserId;

#[derive(Debug)]
pub struct BoundInsert {
    /// Id of the table to perform inserting.
    pub table_id: TableId,

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

    /// Used as part of an extra `Project` when the column types of `source` query does not match
    /// `table_source`. This does not include a simple `VALUE`. See comments in code for details.
    pub cast_exprs: Vec<ExprImpl>,

    // used for the 'RETURNING" keyword to indicate the returning items and schema
    // if the list is empty and the schema is None, the output schema will be a INT64 as the
    // affected row cnt
    pub returning_list: Vec<ExprImpl>,

    pub returning_schema: Option<Schema>,
}

impl Binder {
    pub(super) fn bind_insert(
        &mut self,
        name: ObjectName,
        columns: Vec<Ident>,
        source: Query,
        returning_items: Vec<SelectItem>,
    ) -> Result<BoundInsert> {
        let (schema_name, table_name) =
            Self::resolve_schema_qualified_name(&self.db_name, name)?;
        let table_source = self.bind_table_source(schema_name.as_deref(), &table_name)?;
        self.bind_table(schema_name.as_deref(), &table_name, None)?;

        let table_catalog = self.resolve_dml_table(schema_name.as_deref(), &table_name, true)?;
        let table_id = table_catalog.id;
        let owner = table_catalog.owner;
        let columns_to_insert = table_catalog
            .columns
            .clone()
            .into_iter()
            .filter(|c| !c.is_hidden())
            .collect_vec();
        let row_id_index = table_catalog.row_id_index;

        let expected_types: Vec<DataType> = columns_to_insert
            .iter()
            .map(|c| c.data_type().clone())
            .collect();

        // When the column types of `source` query do not match `expected_types`, casting is
        // needed.
        //
        // In PG, when the `source` is a `VALUES` without order / limit / offset, special treatment
        // is given and it is NOT equivalent to assignment cast over potential implicit cast inside.
        // For example, the following is valid:
        // ```
        //   create table t (v1 time);
        //   insert into t values (timestamp '2020-01-01 01:02:03'), (time '03:04:05');
        // ```
        // But the followings are not:
        // ```
        //   values (timestamp '2020-01-01 01:02:03'), (time '03:04:05');
        //   insert into t values (timestamp '2020-01-01 01:02:03'), (time '03:04:05') limit 1;
        // ```
        // Because `timestamp` can cast to `time` in assignment context, but no casting between them
        // is allowed implicitly.
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

        let mut target_table_col_indices: Vec<usize> = vec![];
        'outer: for query_column in &columns {
            let column_name = query_column.real_value();
            for (col_idx, table_column) in columns_to_insert.iter().enumerate() {
                if column_name == table_column.name() {
                    target_table_col_indices.push(col_idx);
                    continue 'outer;
                }
            }
            // Invalid column name found
            return Err(RwError::from(ErrorCode::BindError(format!(
                "Column {} not found in table {}",
                column_name, table_name
            ))));
        }

        let (returning_list, fields) = self.bind_returning_list(returning_items)?;
        let returning = !returning_list.is_empty();
        // validate that query has a value for each target column, if target columns are used
        // create table t1 (v1 int, v2 int);
        // insert into t1 (v1, v2, v2) values (5, 6); // ...more target columns than values
        // insert into t1 (v1) values (5, 6);         // ...less target columns than values
        let (eq_len, msg) = match target_table_col_indices.len().cmp(&expected_types.len()) {
            std::cmp::Ordering::Equal => (true, ""),
            std::cmp::Ordering::Greater => (false, "INSERT has more target columns than values"),
            std::cmp::Ordering::Less => (false, "INSERT has less target columns than values"),
        };
        if !eq_len && !target_table_col_indices.is_empty() {
            return Err(RwError::from(ErrorCode::BindError(msg.to_string())));
        }

        // Check if column was used multiple times in query e.g.
        // insert into t1 (v1, v1) values (1, 5);
        let mut uniq_cols = target_table_col_indices.clone();
        uniq_cols.sort_unstable();
        uniq_cols.dedup();
        if target_table_col_indices.len() != uniq_cols.len() {
            return Err(RwError::from(ErrorCode::BindError(
                "Column specified more than once".to_string(),
            )));
        }

        let insert = BoundInsert {
            table_id,
            table_name,
            owner,
            row_id_index,
            column_indices: target_table_col_indices,
            source,
            cast_exprs,
            returning_list,
            returning_schema: if returning {
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
            std::cmp::Ordering::Equal => {
                return exprs
                    .into_iter()
                    .zip_eq(expected_types)
                    .map(|(e, t)| e.cast_assign(t.clone()))
                    .try_collect();
            }
            std::cmp::Ordering::Less => "INSERT has more expressions than target columns",
            std::cmp::Ordering::Greater => "INSERT has more target columns than expressions",
        };
        Err(ErrorCode::BindError(msg.into()).into())
    }
}
