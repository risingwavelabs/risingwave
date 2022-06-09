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
use risingwave_common::array::StructValue;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_sqlparser::ast::{Ident, ObjectName, Query, SetExpr};

use super::{BoundQuery, BoundSetExpr};
use crate::binder::{Binder, BoundTableSource};
use crate::expr::{Expr, ExprImpl, InputRef, Literal};

#[derive(Debug)]
pub struct BoundInsert {
    /// Used for injecting deletion chunks to the source.
    pub table_source: BoundTableSource,

    pub source: BoundQuery,

    /// Used as part of an extra `Project` when the column types of `source` query does not match
    /// `table_source`. This does not include a simple `VALUE`. See comments in code for details.
    pub cast_exprs: Vec<ExprImpl>,
}

impl Binder {
    pub(super) fn bind_insert(
        &mut self,
        source_name: ObjectName,
        _columns: Vec<Ident>,
        source: Query,
    ) -> Result<BoundInsert> {
        let table_source = self.bind_table_source(source_name)?;

        let expected_types = table_source
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect();

        // When the column types of `source` query does not match `expected_types`, casting is
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

        let insert = BoundInsert {
            table_source,
            source,
            cast_exprs,
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
                    .map(|(mut e, t)| {
                        // If Literal is struct, check whether can use the expect type to change
                        // origin to get the type of None value.
                        if let ExprImpl::Literal(literal) = &e {
                            if let Some(ScalarImpl::Struct(value)) = literal.get_data() {
                                if Self::check_struct_type(value, e.return_type(), t.clone()) {
                                    e = Literal::new(
                                        Some(ScalarImpl::Struct(value.clone())),
                                        t.clone(),
                                    )
                                    .into();
                                }
                            }
                        }
                        e.cast_assign(t)
                    })
                    .try_collect();
            }
            std::cmp::Ordering::Less => "INSERT has more expressions than target columns",
            std::cmp::Ordering::Greater => "INSERT has more target columns than expressions",
        };
        Err(ErrorCode::BindError(msg.into()).into())
    }

    /// Check whether can use target type to switch source type to determine the `data_type` of
    /// `None` value.
    pub fn check_struct_type(value: &StructValue, source: DataType, target: DataType) -> bool {
        match (source, target) {
            (DataType::Struct { fields: source }, DataType::Struct { fields: target }) => {
                if source.len() != target.len() {
                    return false;
                }
                for i in 0..value.fields().len() {
                    if let Some(ScalarImpl::Struct(v)) = &value.fields()[i] {
                        if !Self::check_struct_type(v, source[i].clone(), target[i].clone()) {
                            return false;
                        }
                    } else if source[i] != target[i] && value.fields()[i].is_some() {
                        return false;
                    }
                }
                true
            }
            (_, _) => false,
        }
    }
}
