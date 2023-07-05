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

use risingwave_common::error::{ErrorCode, Result, RwError};

use super::utils::impl_distill_by_unit;
use super::{
    gen_filter_and_pushdown, generic, BatchLimit, ColPrunable, ExprRewritable, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream,
};
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalLimit` fetches up to `limit` rows from `offset`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalLimit {
    pub base: PlanBase,
    pub(super) core: generic::Limit<PlanRef>,
}

impl LogicalLimit {
    pub fn new(core: generic::Limit<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        LogicalLimit { base, core }
    }

    /// the function will check if the cond is bool expression
    pub fn create(input: PlanRef, limit: u64, offset: u64) -> PlanRef {
        Self::new(generic::Limit::new(input, limit, offset)).into()
    }

    pub fn limit(&self) -> u64 {
        self.core.limit
    }

    pub fn offset(&self) -> u64 {
        self.core.offset
    }
}

impl PlanTreeNodeUnary for LogicalLimit {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        (self.clone_with_input(input), input_col_change)
    }
}
impl_plan_tree_node_for_unary! {LogicalLimit}
impl_distill_by_unit!(LogicalLimit, core, "LogicalLimit");

impl ColPrunable for LogicalLimit {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let new_input = self.input().prune_col(required_cols, ctx);
        self.clone_with_input(new_input).into()
    }
}

impl ExprRewritable for LogicalLimit {}

impl PredicatePushdown for LogicalLimit {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // filter can not transpose limit
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ToBatch for LogicalLimit {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let mut new_logical = self.core.clone();
        new_logical.input = new_input;
        Ok(BatchLimit::new(new_logical).into())
    }
}

impl ToStream for LogicalLimit {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        Err(RwError::from(ErrorCode::InvalidInputSyntax(
            "The LIMIT clause can not appear alone in a streaming query. Please add an ORDER BY clause before the LIMIT"
                .to_string(),
        )))
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (filter, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((filter.into(), out_col_change))
    }
}
