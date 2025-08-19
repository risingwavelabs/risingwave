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

// Minimal imports for prototype

use super::{
    ColPrunable, ColumnPruningContext, ExprRewritable, ExprVisitable, Logical, LogicalFilter,
    LogicalPlanRef as PlanRef, PlanBase, PlanTreeNodeUnary, PredicatePushdown,
    PredicatePushdownContext, ToBatch, ToStream, ToStreamContext, generic,
};
use crate::binder::BoundFillStrategy;
use crate::error::Result;
use crate::expr::{ExprImpl, InputRef};
use crate::optimizer::plan_node::utils::impl_distill_by_unit;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalGapFill` implements [`super::Logical`] to represent a gap-filling operation on a time
/// series.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalGapFill {
    pub base: PlanBase<super::Logical>,
    core: generic::GapFill<PlanRef>,
}

impl LogicalGapFill {
    pub fn new(
        input: PlanRef,
        time_col: InputRef,
        interval: ExprImpl,
        fill_strategies: Vec<BoundFillStrategy>,
    ) -> Self {
        let core = generic::GapFill {
            input,
            time_col,
            interval,
            fill_strategies,
        };
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    pub fn time_col(&self) -> &InputRef {
        &self.core.time_col
    }

    pub fn interval(&self) -> &ExprImpl {
        &self.core.interval
    }

    pub fn fill_strategies(&self) -> &[BoundFillStrategy] {
        &self.core.fill_strategies
    }
}

impl PlanTreeNodeUnary<Logical> for LogicalGapFill {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            input,
            self.time_col().clone(),
            self.interval().clone(),
            self.fill_strategies().to_vec(),
        )
    }
}

impl_plan_tree_node_for_unary! { Logical, LogicalGapFill }
impl_distill_by_unit!(LogicalGapFill, core, "LogicalGapFill");

impl ColPrunable for LogicalGapFill {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        // For minimal prototype: simply pass through all columns without optimization
        let new_input = self.input().prune_col(required_cols, ctx);
        self.clone_with_input(new_input).into()
    }
}

impl ExprRewritable<Logical> for LogicalGapFill {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn crate::expr::ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base: self.base.clone(),
            core,
        }
        .into()
    }
}

impl ExprVisitable for LogicalGapFill {
    fn visit_exprs(&self, v: &mut dyn crate::expr::ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl PredicatePushdown for LogicalGapFill {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalGapFill {
    fn to_batch(&self) -> Result<super::BatchPlanRef> {
        unimplemented!("batch gap fill")
    }
}

impl ToStream for LogicalGapFill {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<super::StreamPlanRef> {
        use super::{StreamEowcGapFill, StreamGapFill};
        use crate::optimizer::property::RequiredDist;

        let stream_input = self.input().to_stream(ctx)?;

        // GapFill (both normal and EOWC) always uses singleton distribution for correctness.
        // It needs to see complete time series data to identify and fill gaps properly.
        let new_input = RequiredDist::single().streaming_enforce_if_not_satisfies(stream_input)?;

        let core = generic::GapFill {
            input: new_input,
            time_col: self.core.time_col.clone(),
            interval: self.core.interval.clone(),
            fill_strategies: self.core.fill_strategies.clone(),
        };

        if ctx.emit_on_window_close() {
            Ok(StreamEowcGapFill::new(core).into())
        } else {
            Ok(StreamGapFill::new(core).into())
        }
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut super::convert::RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, mut col_index_mapping) = self.input().logical_rewrite_for_stream(_ctx)?;
        let mut new_core = self.core.clone();
        new_core.input = input;

        if col_index_mapping.is_identity() {
            return Ok((
                Self {
                    base: self.base.clone_with_new_plan_id(),
                    core: new_core,
                }
                .into(),
                col_index_mapping,
            ));
        }

        new_core.rewrite_with_col_index_mapping(&mut col_index_mapping);

        Ok((
            Self {
                base: self.base.clone_with_new_plan_id(),
                core: new_core,
            }
            .into(),
            col_index_mapping,
        ))
    }
}
