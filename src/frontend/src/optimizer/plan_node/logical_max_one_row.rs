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

use pretty_xmlish::XmlNode;

use super::generic::DistillUnit;
use super::utils::Distill;
use super::{
    BatchMaxOneRow, ColPrunable, ExprRewritable, Logical, PlanBase, PlanRef, PlanTreeNodeUnary,
    PredicatePushdown, ToBatch, ToStream, gen_filter_and_pushdown, generic,
};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// [`LogicalMaxOneRow`] fetches up to one row from the input, returning an error
/// if the input contains more than one row at runtime. Only available in batch mode.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalMaxOneRow {
    pub base: PlanBase<Logical>,
    pub(super) core: generic::MaxOneRow<PlanRef>,
}

impl LogicalMaxOneRow {
    fn new(core: generic::MaxOneRow<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        LogicalMaxOneRow { base, core }
    }

    pub fn create(input: PlanRef) -> PlanRef {
        let core = generic::MaxOneRow { input };
        Self::new(core).into()
    }
}

impl PlanTreeNodeUnary for LogicalMaxOneRow {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let core = generic::MaxOneRow { input };
        Self::new(core)
    }

    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        (self.clone_with_input(input), input_col_change)
    }
}
impl_plan_tree_node_for_unary! {LogicalMaxOneRow}

impl Distill for LogicalMaxOneRow {
    fn distill<'a>(&self) -> XmlNode<'a> {
        self.core.distill_with_name("LogicalMaxOneRow")
    }
}

impl ColPrunable for LogicalMaxOneRow {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let new_input = self.input().prune_col(required_cols, ctx);
        self.clone_with_input(new_input).into()
    }
}

impl ExprRewritable for LogicalMaxOneRow {}

impl ExprVisitable for LogicalMaxOneRow {}

impl PredicatePushdown for LogicalMaxOneRow {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // Filter can not transpose MaxOneRow
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ToBatch for LogicalMaxOneRow {
    fn to_batch(&self) -> Result<PlanRef> {
        let input = self.input().to_batch()?;
        let core = generic::MaxOneRow { input };
        Ok(BatchMaxOneRow::new(core).into())
    }
}

impl ToStream for LogicalMaxOneRow {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        // Check `LogicalOptimizer::gen_optimized_logical_plan_for_stream`.
        unreachable!("should already bail out after subquery unnesting")
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (this, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((this.into(), out_col_change))
    }
}
