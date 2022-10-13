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

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Ident, ObjectName, Query, SetExpr};

use super::{BoundQuery, BoundSetExpr};
use crate::binder::{Binder, BoundTableSource};
use crate::expr::{ExprImpl, InputRef};

#[derive(Debug)]
pub struct BoundInsert {
    /// Used for injecting deletion chunks to the source.
    pub table_source: BoundTableSource,

    // TODOs: Translate names of cols into ids
    pub column_idxs: Vec<i32>, // maybe use alias see e.g. ColumnID

    pub source: BoundQuery,

    /// Used as part of an extra `Project` when the column types of `source` query does not match
    /// `table_source`. This does not include a simple `VALUE`. See comments in code for details.
    pub cast_exprs: Vec<ExprImpl>,
}

impl Binder {
    // maybe we do not bind to the correct columns?
    pub(super) fn bind_insert(
        &mut self,
        source_name: ObjectName,
        columns: Vec<Ident>,
        source: Query,
    ) -> Result<BoundInsert> {
        let (schema_name, source_name) =
            Self::resolve_table_or_source_name(&self.db_name, source_name)?;
        let table_source = self.bind_table_source(schema_name.as_deref(), &source_name)?;

        // changing the expected types does not help us
        // if we have two cols c1::int and c2::int both are int
        // we cannot infer the insertion order from the types
        let expected_types = table_source
            .columns
            .iter()
            .map(|c| c.data_type.clone())
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
                let values = self.bind_values(values, Some(expected_types))?;
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
                        expected_types,
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

        // I think this looks good...
        //
        // create table t (v1 int, v2 int); insert into t (v1, v2) values (1, 2);
        //
        // cast_exprs is empty
        //
        // table_source:
        // BoundTableSource { name: "t", source_id: TableId { table_id: 1001 },
        // associated_mview_id: TableId { table_id: 1002 }, columns: [ColumnDesc { data_type: Int32,
        // column_id: #0, name: "v1", field_descs: [], type_name: "" }, ColumnDesc { data_type:
        // Int32, column_id: #1, name: "v2", field_descs: [], type_name: "" }], append_only: false,
        // owner: 1 } source: BoundQuery { body: Values(BoundValues { rows: [[1:Int32,
        // 2:Int32]], schema: Schema { fields: [*VALUES*_0.column_0:Int32,
        // *VALUES*_0.column_1:Int32] } }), order: [], limit: None, offset: None, with_ties: false,
        // extra_order_exprs: [] }
        //
        // source:
        // BoundQuery { body: Values(BoundValues { rows: [[1:Int32, 2:Int32]], schema: Schema {
        // fields: [*VALUES*_0.column_0:Int32, *VALUES*_0.column_1:Int32] } }), order: [], limit:
        // None, offset: None, with_ties: false, extra_order_exprs: [] }
        //
        // source is part of the table_source

        // Column
        // [Insert { table_name: ObjectName([Ident { value: "t", quote_style: None }]), columns:
        // [Ident { value: "v3", quote_style: None }, Ident { value: "v1", quote_style: None }],
        // source: Query { with: None, body: Values(Values([[Value(Number("1")),
        // Value(Number("5"))]])), order_by: [], limit: None, offset: None, fetch: None } }]
        // thread 'risingwave-main' panicked at '[Ident { value: "v3", quote_style: None }, Ident {
        // value: "v1", quote_style: None }]'

        // TODO: Nullable currently not supported. Open issue that a column can also be non-nullable
        // Check if column is nullable -> currently all columns are always nullable

        let mut column_idxs: Vec<i32> = vec![];
        for query_column in columns {
            let column_name = query_column.value; // value or real_value()
            let mut matched_existing_col = false;
            for (col_idx, table_column) in table_source.columns.iter().enumerate() {
                if column_name == table_column.name {
                    // is there a better comparison then by col name?
                    column_idxs.push(col_idx as i32);
                    matched_existing_col = true;
                    break;
                }
            }
            // Invalid column name found
            if !matched_existing_col {
                return Err(RwError::from(ErrorCode::BindError(format!(
                    "Column '{}' not found in table '{}'",
                    column_name, table_source.name
                ))));
            }
        }

        // Check if column was mentioned multiple times in query
        // e.g. insert into t (v1, v1) values (1, 5);
        let mut sorted = column_idxs.clone();
        sorted.dedup();
        if column_idxs.len() != sorted.len() {
            return Err(RwError::from(ErrorCode::BindError(format!(
                "Column specified more than once"
            ))));
        }

        let insert = BoundInsert {
            table_source,
            source,
            cast_exprs,
            column_idxs,
        };

        Ok(insert)
    }

    /// Cast a list of `exprs` to corresponding `expected_types` IN ASSIGNMENT CONTEXT. Make sure
    /// you understand the difference of implicit, assignment and explicit cast before reusing it.
    pub(super) fn cast_on_insert(
        expected_types: Vec<DataType>,
        exprs: Vec<ExprImpl>,
    ) -> Result<Vec<ExprImpl>> {
        let msg = match expected_types.len().cmp(&exprs.len()) {
            std::cmp::Ordering::Equal => {
                return exprs
                    .into_iter()
                    .zip_eq(expected_types)
                    .map(|(e, t)| e.cast_assign(t))
                    .try_collect();
            }
            std::cmp::Ordering::Less => "INSERT has more expressions than target columns",
            std::cmp::Ordering::Greater => "INSERT has more target columns than expressions",
        };
        Err(ErrorCode::BindError(msg.into()).into())
    }
}
