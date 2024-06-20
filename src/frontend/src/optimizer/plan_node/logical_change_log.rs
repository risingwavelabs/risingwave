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

use itertools::Itertools;

use super::expr_visitable::ExprVisitable;
use super::generic::{GenericPlanRef, CHANGE_LOG_OP, _CHANGE_LOG_ROW_ID};
use super::utils::impl_distill_by_unit;
use super::{
    gen_filter_and_pushdown, generic, ColPrunable, ColumnPruningContext, ExprRewritable, Logical,
    LogicalProject, PlanBase, PlanTreeNodeUnary, PredicatePushdown, RewriteStreamContext,
    StreamChangeLog, StreamRowIdGen, ToBatch, ToStream, ToStreamContext,
};
use crate::error::ErrorCode::BindError;
use crate::error::Result;
use crate::expr::{ExprImpl, InputRef};
use crate::optimizer::property::Distribution;
use crate::utils::{ColIndexMapping, Condition};
use crate::PlanRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalChangeLog {
    pub base: PlanBase<Logical>,
    core: generic::ChangeLog<PlanRef>,
}

impl LogicalChangeLog {
    pub fn create(input: PlanRef) -> PlanRef {
        Self::new(input, true, true).into()
    }

    pub fn new(input: PlanRef, need_op: bool, need_change_log_row_id: bool) -> Self {
        let core = generic::ChangeLog::new(input, need_op, need_change_log_row_id);
        Self::with_core(core)
    }

    pub fn with_core(core: generic::ChangeLog<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        LogicalChangeLog { base, core }
    }
}

impl PlanTreeNodeUnary for LogicalChangeLog {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.core.need_op, self.core.need_change_log_row_id)
    }

    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let change_log = Self::new(input, self.core.need_op, true);
        if self.core.need_op {
            let mut output_vec = input_col_change.to_parts().0.to_vec();
            let len = input_col_change.to_parts().1;
            output_vec.push(Some(len));
            let out_col_change = ColIndexMapping::new(output_vec, len + 1);
            (change_log, out_col_change)
        } else {
            (change_log, input_col_change)
        }
    }
}

impl_plan_tree_node_for_unary! {LogicalChangeLog}
impl_distill_by_unit!(LogicalChangeLog, core, "LogicalChangeLog");

impl ExprRewritable for LogicalChangeLog {}

impl ExprVisitable for LogicalChangeLog {}

impl PredicatePushdown for LogicalChangeLog {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut super::PredicatePushdownContext,
    ) -> PlanRef {
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ColPrunable for LogicalChangeLog {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let fields = self.schema().fields();
        let mut need_op = false;
        let mut need_change_log_row_id = false;
        let new_required_cols: Vec<_> = required_cols
            .iter()
            .filter_map(|a| {
                if let Some(f) = fields.get(*a) {
                    if f.name == CHANGE_LOG_OP {
                        need_op = true;
                        None
                    } else if f.name == _CHANGE_LOG_ROW_ID {
                        need_change_log_row_id = true;
                        None
                    } else {
                        Some(*a)
                    }
                } else {
                    Some(*a)
                }
            })
            .collect();

        let new_input = self.input().prune_col(&new_required_cols, ctx);
        Self::new(new_input, need_op, need_change_log_row_id).into()
    }
}

impl ToBatch for LogicalChangeLog {
    fn to_batch(&self) -> Result<PlanRef> {
        Err(BindError("With changelog cte only support with create mv/sink".to_string()).into())
    }
}

impl ToStream for LogicalChangeLog {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let new_input = self.input().to_stream(ctx)?;

        let mut new_logical = self.core.clone();
        new_logical.input = new_input;
        let plan = StreamChangeLog::new(new_logical).into();
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
        let original_schema = self.input().schema().clone();
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let exprs = (0..original_schema.len())
            .map(|x| {
                ExprImpl::InputRef(
                    InputRef::new(
                        input_col_change.map(x),
                        original_schema.fields[x].data_type.clone(),
                    )
                    .into(),
                )
            })
            .collect_vec();
        let project = LogicalProject::new(input.clone(), exprs);
        let (project, out_col_change) = project.rewrite_with_input(input, input_col_change);
        let (change_log, out_col_change) = self.rewrite_with_input(project.into(), out_col_change);
        Ok((change_log.into(), out_col_change))
    }
}
