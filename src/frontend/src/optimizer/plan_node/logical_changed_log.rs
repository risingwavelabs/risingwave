// Copyright 2024 RisingWave Labs
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

use super::expr_visitable::ExprVisitable;
use super::generic::GenericPlanRef;
use super::utils::impl_distill_by_unit;
use super::{
    gen_filter_and_pushdown, generic, ColPrunable, ColumnPruningContext, ExprRewritable, Logical,
    PlanBase, PlanTreeNodeUnary, PredicatePushdown, RewriteStreamContext, StreamChangedLog,
    StreamRowIdGen, ToBatch, ToStream, ToStreamContext,
};
use crate::error::Result;
use crate::optimizer::property::Distribution;
use crate::utils::{ColIndexMapping, Condition};
use crate::PlanRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalChangedLog {
    pub base: PlanBase<Logical>,
    core: generic::ChangeLog<PlanRef>,
}

impl LogicalChangedLog {
    pub fn create(input: PlanRef) -> PlanRef {
        Self::new(input, true, true).into()
    }

    pub fn new(input: PlanRef, need_op: bool, need_changed_log_row_id: bool) -> Self {
        let core = generic::ChangeLog::new(input, need_op, need_changed_log_row_id);
        Self::with_core(core)
    }

    pub fn with_core(core: generic::ChangeLog<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        LogicalChangedLog { base, core }
    }
}

impl PlanTreeNodeUnary for LogicalChangedLog {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.core.need_op, self.core.need_changed_log_row_id)
    }

    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let changed_log = Self::new(input, self.core.need_op, true);
        if self.core.need_op {
            let mut output_vec = input_col_change.to_parts().0.to_vec();
            let len = input_col_change.to_parts().1;
            output_vec.push(Some(len));
            let out_col_change = ColIndexMapping::new(output_vec, len + 1);
            (changed_log, out_col_change)
        } else {
            (changed_log, input_col_change)
        }
    }
}

impl_plan_tree_node_for_unary! {LogicalChangedLog}
impl_distill_by_unit!(LogicalChangedLog, core, "LogicalChangedLog");

impl ExprRewritable for LogicalChangedLog {}

impl ExprVisitable for LogicalChangedLog {}

impl PredicatePushdown for LogicalChangedLog {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut super::PredicatePushdownContext,
    ) -> PlanRef {
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ColPrunable for LogicalChangedLog {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let mut new_required_cols = required_cols.to_vec();
        let fields = self.schema().fields();
        let mut need_op = false;
        let mut need_changed_log_row_id = false;
        required_cols.iter().for_each(|a| {
            if let Some(f) = fields.get(*a) {
                if f.name == "op" {
                    new_required_cols.pop();
                    need_op = true;
                } else if f.name == "_changed_log_row_id" {
                    new_required_cols.pop();
                    need_changed_log_row_id = true;
                }
            }
        });

        let new_input = self.input().prune_col(&new_required_cols, ctx);
        Self::new(new_input, need_op, need_changed_log_row_id).into()
    }
}

impl ToBatch for LogicalChangedLog {
    fn to_batch(&self) -> Result<PlanRef> {
        unimplemented!()
    }
}

impl ToStream for LogicalChangedLog {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let new_input = self.input().to_stream(ctx)?;

        let mut new_logical = self.core.clone();
        new_logical.input = new_input;
        let plan = StreamChangedLog::new(new_logical).into();
        let row_id_index = self.schema().fields().len() - 1;
        let plan = StreamRowIdGen::new_with_dist(
            plan,
            row_id_index,
            Distribution::HashShard(vec![row_id_index]),
        )
        .into();

        Ok(plan)
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (changed_log, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((changed_log.into(), out_col_change))
    }
}
