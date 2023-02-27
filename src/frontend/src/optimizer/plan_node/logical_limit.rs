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

use std::fmt;

use risingwave_common::error::{ErrorCode, Result, RwError};

use super::{
    gen_filter_and_pushdown, BatchLimit, ColPrunable, ExprRewritable, PlanBase, PlanRef,
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
    input: PlanRef,
    pub(super) limit: u64,
    pub(super) offset: u64,
}

impl LogicalLimit {
    pub fn new(input: PlanRef, limit: u64, offset: u64) -> Self {
        let ctx = input.ctx();
        let schema = input.schema().clone();
        let pk_indices = input.logical_pk().to_vec();
        let functional_dependency = input.functional_dependency().clone();
        let base = PlanBase::new_logical(ctx, schema, pk_indices, functional_dependency);
        LogicalLimit {
            base,
            input,
            limit,
            offset,
        }
    }

    /// the function will check if the cond is bool expression
    pub fn create(input: PlanRef, limit: u64, offset: u64) -> PlanRef {
        Self::new(input, limit, offset).into()
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}

impl PlanTreeNodeUnary for LogicalLimit {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.limit, self.offset)
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        (Self::new(input, self.limit, self.offset), input_col_change)
    }
}
impl_plan_tree_node_for_unary! {LogicalLimit}
impl fmt::Display for LogicalLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LogicalLimit {{ limit: {}, offset: {} }}",
            self.limit, self.offset
        )
    }
}

impl ColPrunable for LogicalLimit {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let new_input = self.input.prune_col(required_cols, ctx);
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
        let new_logical = self.clone_with_input(new_input);
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
        let (input, input_col_change) = self.input.logical_rewrite_for_stream(ctx)?;
        let (filter, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((filter.into(), out_col_change))
    }
}
