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

use pretty_xmlish::XmlNode;
use risingwave_common::bail;
use risingwave_common::error::Result;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

use super::utils::Distill;
use super::{
    gen_filter_and_pushdown, generic, BatchLimit, ColPrunable, ExprRewritable, Logical, PlanBase,
    PlanRef, PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream,
};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, LogicalTopN, PredicatePushdownContext, RewriteStreamContext,
    ToStreamContext,
};
use crate::optimizer::property::Order;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalLimit` fetches up to `limit` rows from `offset`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalLimit {
    pub base: PlanBase<Logical>,
    pub(super) core: generic::Limit<PlanRef>,

    /// Whether to report an error when the number of rows from the input
    /// exceeds the limit.
    pub check_exceeding: bool,
}

impl LogicalLimit {
    fn new(core: generic::Limit<PlanRef>, check_exceeding: bool) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        LogicalLimit {
            base,
            core,
            check_exceeding,
        }
    }

    /// the function will check if the cond is bool expression
    pub fn create(input: PlanRef, limit: u64, offset: u64, check_exceeding: bool) -> PlanRef {
        Self::new(generic::Limit::new(input, limit, offset), check_exceeding).into()
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
        Self::new(core, self.check_exceeding)
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

impl Distill for LogicalLimit {
    fn distill<'a>(&self) -> XmlNode<'a> {
        self.core
            .distill_inner("LogicalLimit", self.check_exceeding)
    }
}

impl ColPrunable for LogicalLimit {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let new_input = self.input().prune_col(required_cols, ctx);
        self.clone_with_input(new_input).into()
    }
}

impl ExprRewritable for LogicalLimit {}

impl ExprVisitable for LogicalLimit {}

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
        Ok(BatchLimit::new(new_logical, self.check_exceeding).into())
    }
}

impl ToStream for LogicalLimit {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        if self.check_exceeding {
            // `check_exceeding` is currently only used for the runtime check of
            // scalar subqueries, while it's not supported in streaming mode, so
            // we raise a precise error here.
            bail!("Scalar subquery might produce more than one row.");
        }

        // use the first column as an order to provide determinism for streaming queries.
        let order = Order::new(vec![ColumnOrder::new(0, OrderType::ascending())]);
        let topn = LogicalTopN::new(
            self.input(),
            self.limit(),
            self.offset(),
            false,
            order,
            vec![],
        );
        topn.to_stream(ctx)
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
