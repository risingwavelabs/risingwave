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

use itertools::Itertools;

use super::generic::GenericPlanRef;
use super::utils::impl_distill_by_unit;
use super::{
    BatchPlanRef, ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef, LogicalProject,
    PlanBase, PlanTreeNodeUnary, PredicatePushdown, StreamPlanRef, ToBatch, ToStream, generic,
};
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalLocalityProvider` provides locality for operators during backfilling.
/// It buffers input data into a state table using locality columns as primary key prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalLocalityProvider {
    pub base: PlanBase<Logical>,
    core: generic::LocalityProvider<PlanRef>,
}

impl LogicalLocalityProvider {
    pub fn new(input: PlanRef, locality_columns: Vec<usize>) -> Self {
        let core = generic::LocalityProvider::new(input, locality_columns);
        let base = PlanBase::new_logical_with_core(&core);
        LogicalLocalityProvider { base, core }
    }

    /// Create a `LogicalLocalityProvider` with the given input and locality columns
    pub fn create(input: PlanRef, locality_columns: Vec<usize>) -> PlanRef {
        LogicalLocalityProvider::new(input, locality_columns).into()
    }

    /// Get the locality columns of the locality provider.
    pub fn locality_columns(&self) -> &[usize] {
        &self.core.locality_columns
    }
}

impl PlanTreeNodeUnary<Logical> for LogicalLocalityProvider {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.locality_columns().to_vec())
    }

    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let locality_columns = self
            .locality_columns()
            .iter()
            .map(|&i| input_col_change.map(i))
            .collect();

        (Self::new(input, locality_columns), input_col_change)
    }
}

impl_plan_tree_node_for_unary! { Logical, LogicalLocalityProvider}
impl_distill_by_unit!(LogicalLocalityProvider, core, "LogicalLocalityProvider");

impl ColPrunable for LogicalLocalityProvider {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        // No pruning.
        let input_required_cols = (0..self.input().schema().len()).collect_vec();
        LogicalProject::with_out_col_idx(
            self.clone_with_input(self.input().prune_col(&input_required_cols, ctx))
                .into(),
            required_cols.iter().cloned(),
        )
        .into()
    }
}

impl PredicatePushdown for LogicalLocalityProvider {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let new_input = self.input().predicate_pushdown(predicate, ctx);
        let new_provider = self.clone_with_input(new_input);
        new_provider.into()
    }
}

impl ToBatch for LogicalLocalityProvider {
    fn to_batch(&self) -> Result<BatchPlanRef> {
        // LocalityProvider is a streaming-only operator
        Err(crate::error::ErrorCode::NotSupported(
            "LocalityProvider in batch mode".to_string(),
            "LocalityProvider is only supported in streaming mode for backfilling".to_string(),
        )
        .into())
    }
}

impl ToStream for LogicalLocalityProvider {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<StreamPlanRef> {
        use super::StreamLocalityProvider;
        let input = self.input().to_stream(ctx)?;
        let stream_core = generic::LocalityProvider::new(input, self.locality_columns().to_vec());
        Ok(StreamLocalityProvider::new(stream_core).into())
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (locality_provider, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((locality_provider.into(), out_col_change))
    }
}

impl ExprRewritable<Logical> for LogicalLocalityProvider {
    fn has_rewritable_expr(&self) -> bool {
        false
    }

    fn rewrite_exprs(&self, _r: &mut dyn ExprRewriter) -> PlanRef {
        self.clone().into()
    }
}

impl ExprVisitable for LogicalLocalityProvider {
    fn visit_exprs(&self, _v: &mut dyn ExprVisitor) {
        // No expressions to visit
    }
}

impl LogicalLocalityProvider {
    /// Try to provide better locality by transforming input
    pub fn try_better_locality(&self, columns: &[usize]) -> Option<PlanRef> {
        if columns == self.locality_columns() {
            Some(self.clone().into())
        } else {
            if let Some(better_input) = self.input().try_better_locality(columns) {
                Some(better_input)
            } else {
                Some(Self::new(self.input(), columns.to_owned()).into())
            }
        }
    }
}
