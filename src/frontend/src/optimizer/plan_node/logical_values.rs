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

use std::sync::Arc;
use std::{fmt, vec};

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;

use super::{
    BatchValues, ColPrunable, ExprRewritable, LogicalFilter, PlanBase, PlanRef,
    PredicatePushdown, StreamValues, ToBatch, ToStream,
};
use crate::expr::{Expr, ExprImpl, ExprRewriter, Literal};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::{FunctionalDependencySet};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalValues` builds rows according to a list of expressions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalValues {
    pub base: PlanBase,
    rows: Arc<[Vec<ExprImpl>]>,
}

impl LogicalValues {
    /// Create a [`LogicalValues`] node. Used internally by optimizer.
    pub fn new(rows: Vec<Vec<ExprImpl>>, schema: Schema, ctx: OptimizerContextRef) -> Self {
        for exprs in &rows {
            for (i, expr) in exprs.iter().enumerate() {
                assert_eq!(schema.fields()[i].data_type(), expr.return_type())
            }
        }
        let functional_dependency = FunctionalDependencySet::new(schema.len());
        let base = PlanBase::new_logical(ctx, schema, vec![], functional_dependency);
        Self {
            rows: rows.into(),
            base,
        }
    }

    /// Used only by `LogicalValues.rewrite_logical_for_stream, set the `_row_id` column as pk
    fn new_with_pk(
        rows: Vec<Vec<ExprImpl>>,
        schema: Schema,
        ctx: OptimizerContextRef,
        pk_index: usize,
    ) -> Self {
        for exprs in &rows {
            for (i, expr) in exprs.iter().enumerate() {
                assert_eq!(schema.fields()[i].data_type(), expr.return_type())
            }
        }
        let functional_dependency = FunctionalDependencySet::new(schema.len());
        let base = PlanBase::new_logical(ctx, schema, vec![pk_index], functional_dependency);
        Self {
            rows: rows.into(),
            base,
        }
    }

    /// Create a [`LogicalValues`] node. Used by planner.
    pub fn create(rows: Vec<Vec<ExprImpl>>, schema: Schema, ctx: OptimizerContextRef) -> PlanRef {
        // No additional checks after binder.
        Self::new(rows, schema, ctx).into()
    }

    /// Get a reference to the logical values' rows.
    pub fn rows(&self) -> &[Vec<ExprImpl>] {
        self.rows.as_ref()
    }
}

impl_plan_tree_node_for_leaf! { LogicalValues }

impl fmt::Display for LogicalValues {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogicalValues")
            .field("rows", &self.rows)
            .field("schema", &self.schema())
            .finish()
    }
}

impl ExprRewritable for LogicalValues {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut new = self.clone();
        new.rows = new
            .rows
            .iter()
            .map(|exprs| {
                exprs
                    .iter()
                    .map(|e| r.rewrite_expr(e.clone()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .into();
        new.base = new.base.clone_with_new_plan_id();
        new.into()
    }
}

impl ColPrunable for LogicalValues {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let rows = self
            .rows
            .iter()
            .map(|row| required_cols.iter().map(|i| row[*i].clone()).collect())
            .collect();
        let fields = required_cols
            .iter()
            .map(|i| self.schema().fields[*i].clone())
            .collect();
        Self::new(rows, Schema { fields }, self.base.ctx.clone()).into()
    }
}

impl PredicatePushdown for LogicalValues {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalValues {
    fn to_batch(&self) -> Result<PlanRef> {
        Ok(BatchValues::new(self.clone()).into())
    }
}

impl ToStream for LogicalValues {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        Ok(StreamValues::new(self.clone()).into())
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let row_id_index = self.schema().len();
        let col_index_mapping = ColIndexMapping::identity_or_none(row_id_index, row_id_index + 1);
        let ctx = self.ctx().clone();
        let mut schema = self.schema().clone();
        schema.fields.push(Field {
            data_type: DataType::Int64,
            name: "_row_id".to_string(),
            sub_fields: vec![],
            type_name: "int64".to_string(),
        });
        let rows = self.rows().clone().to_owned();
        let row_with_id = (0..rows.len())
            .into_iter()
            .zip_eq_fast(rows.into_iter())
            .map(|(i, mut r)| {
                r.extend_one(Literal::new(Some(ScalarImpl::Int64(i as i64)), DataType::Int64).into());
                r
            })
            .collect_vec();
        let logical_values = Self::new_with_pk(row_with_id, schema, ctx, row_id_index);
        Ok((logical_values.into(), col_index_mapping))
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::Field;
    use risingwave_common::types::{DataType, Datum};

    use super::*;
    use crate::expr::Literal;
    use crate::optimizer::optimizer_context::OptimizerContext;

    fn literal(val: i32) -> ExprImpl {
        Literal::new(Datum::Some(val.into()), DataType::Int32).into()
    }

    /// Pruning
    /// ```text
    /// Values([[0, 1, 2], [3, 4, 5])
    /// ```
    /// with required columns [0, 2] will result in
    /// ```text
    /// Values([[0, 2], [3, 5])
    /// ```
    #[tokio::test]
    async fn test_prune_filter() {
        let ctx = OptimizerContext::mock().await;
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Int32, "v3"),
        ]);
        // Values([[0, 1, 2], [3, 4, 5])
        let values: PlanRef = LogicalValues::new(
            vec![
                vec![literal(0), literal(1), literal(2)],
                vec![literal(3), literal(4), literal(5)],
            ],
            schema,
            ctx,
        )
        .into();

        let required_cols = vec![0, 2];
        let pruned = values.prune_col(
            &required_cols,
            &mut ColumnPruningContext::new(values.clone()),
        );

        let values = pruned.as_logical_values().unwrap();
        let rows: &[Vec<ExprImpl>] = values.rows();

        // expected output: Values([[0, 2], [3, 5])
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 2);
        assert_eq!(rows[0][0], literal(0));
        assert_eq!(rows[0][1], literal(2));
        assert_eq!(rows[1].len(), 2);
        assert_eq!(rows[1][0], literal(3));
        assert_eq!(rows[1][1], literal(5));
    }
}
