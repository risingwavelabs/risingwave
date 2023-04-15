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

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::Values;

use super::bind_context::Clause;
use super::statement::RewriteExprsRecursive;
use crate::binder::Binder;
use crate::expr::{align_types, CorrelatedId, Depth, ExprImpl};

#[derive(Debug, Clone)]
pub struct BoundValues {
    pub rows: Vec<Vec<ExprImpl>>,
    pub schema: Schema,
}

impl RewriteExprsRecursive for BoundValues {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        let new_rows = std::mem::take(&mut self.rows)
            .into_iter()
            .map(|exprs| {
                exprs
                    .into_iter()
                    .map(|expr| rewriter.rewrite_expr(expr))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        self.rows = new_rows;
    }
}

impl BoundValues {
    /// The schema returned of this [`BoundValues`].
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn exprs(&self) -> impl Iterator<Item = &ExprImpl> {
        self.rows.iter().flatten()
    }

    pub fn exprs_mut(&mut self) -> impl Iterator<Item = &mut ExprImpl> {
        self.rows.iter_mut().flatten()
    }

    pub fn is_correlated(&self, depth: Depth) -> bool {
        self.exprs()
            .any(|expr| expr.has_correlated_input_ref_by_depth(depth))
    }

    pub fn collect_correlated_indices_by_depth_and_assign_id(
        &mut self,
        depth: Depth,
        correlated_id: CorrelatedId,
    ) -> Vec<usize> {
        self.exprs_mut()
            .flat_map(|expr| {
                expr.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id)
            })
            .collect()
    }
}

fn values_column_name(values_id: usize, col_id: usize) -> String {
    format!("*VALUES*_{}.column_{}", values_id, col_id)
}

impl Binder {
    /// Bind [`Values`] with given `expected_types`. If no types are expected, a compatible type for
    /// all rows will be used.
    /// If values are shorter than expected, `NULL`s will be filled.
    pub(super) fn bind_values(
        &mut self,
        values: Values,
        expected_types: Option<Vec<DataType>>,
    ) -> Result<BoundValues> {
        assert!(!values.0.is_empty());

        self.context.clause = Some(Clause::Values);
        let vec2d = values.0;
        let mut bound = vec2d
            .into_iter()
            .map(|vec| vec.into_iter().map(|expr| self.bind_expr(expr)).collect())
            .collect::<Result<Vec<Vec<_>>>>()?;
        self.context.clause = None;

        // Adding Null values in case user did not specify all columns. E.g.
        // create table t1 (v1 int, v2 int); insert into t1 (v2) values (5);
        let num_columns = bound[0].len();
        if bound.iter().any(|row| row.len() != num_columns) {
            return Err(
                ErrorCode::BindError("VALUES lists must all be the same length".into()).into(),
            );
        }

        // Calculate column types.
        let types = match expected_types {
            Some(types) => {
                bound = bound
                    .into_iter()
                    .map(|vec| Self::cast_on_insert(&types.clone(), vec))
                    .try_collect()?;

                types
            }
            None => (0..num_columns)
                .map(|col_index| align_types(bound.iter_mut().map(|row| &mut row[col_index])))
                .try_collect()?,
        };

        let values_id = self.next_values_id();
        #[expect(clippy::disallowed_methods)]
        let schema = Schema::new(
            types
                .into_iter()
                .zip(0..num_columns)
                .map(|(ty, col_id)| Field::with_name(ty, values_column_name(values_id, col_id)))
                .collect(),
        );

        let bound_values = BoundValues {
            rows: bound,
            schema,
        };
        if bound_values
            .rows
            .iter()
            .flatten()
            .any(|expr| expr.has_subquery())
        {
            return Err(ErrorCode::NotImplemented("Subquery in VALUES".into(), None.into()).into());
        }
        if bound_values.is_correlated(1) {
            return Err(ErrorCode::NotImplemented(
                "CorrelatedInputRef in VALUES".into(),
                None.into(),
            )
            .into());
        }
        Ok(bound_values)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::util::iter_util::{zip_eq_fast, ZipEqFast};
    use risingwave_sqlparser::ast::{Expr, Value};

    use super::*;
    use crate::binder::test_utils::mock_binder;
    use crate::expr::Expr as _;

    #[tokio::test]
    async fn test_bind_values() {
        let mut binder = mock_binder();

        // Test i32 -> decimal.
        let expr1 = Expr::Value(Value::Number("1".to_string()));
        let expr2 = Expr::Value(Value::Number("1.1".to_string()));
        let values = Values(vec![vec![expr1], vec![expr2]]);
        let res = binder.bind_values(values, None).unwrap();

        let types = vec![DataType::Decimal];
        let n_cols = types.len();
        let schema = Schema::new(
            types
                .into_iter()
                .zip_eq_fast(0..n_cols)
                .map(|(ty, col_id)| Field::with_name(ty, values_column_name(0, col_id)))
                .collect(),
        );

        assert_eq!(res.schema, schema);
        for vec in res.rows {
            for (expr, ty) in zip_eq_fast(vec, schema.data_types()) {
                assert_eq!(expr.return_type(), ty);
            }
        }
    }
}
