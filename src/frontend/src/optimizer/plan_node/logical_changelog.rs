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
use super::generic::{_CHANGELOG_ROW_ID, CHANGELOG_OP, GenericPlanRef};
use super::utils::impl_distill_by_unit;
use super::{
    BatchPlanRef, ColPrunable, ColumnPruningContext, ExprRewritable, Logical,
    LogicalPlanRef as PlanRef, LogicalProject, PlanBase, PlanTreeNodeUnary, PredicatePushdown,
    RewriteStreamContext, StreamChangeLog, StreamPlanRef, ToBatch, ToStream, ToStreamContext,
    gen_filter_and_pushdown, generic,
};
use crate::error::ErrorCode::BindError;
use crate::error::Result;
use crate::optimizer::plan_node::generic::PhysicalPlanRef;
use crate::optimizer::property::Distribution;
use crate::utils::{ColIndexMapping, Condition};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalChangeLog {
    pub base: PlanBase<Logical>,
    core: generic::ChangeLog<PlanRef>,
}

impl LogicalChangeLog {
    pub fn create(input: PlanRef, key_indices: Option<Vec<usize>>) -> PlanRef {
        Self::new(input, key_indices, true, true).into()
    }

    pub fn new(
        input: PlanRef,
        key_indices: Option<Vec<usize>>,
        need_op: bool,
        need_changelog_row_id: bool,
    ) -> Self {
        let core = generic::ChangeLog::new(input, key_indices, need_op, need_changelog_row_id);
        Self::with_core(core)
    }

    pub fn with_core(core: generic::ChangeLog<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        LogicalChangeLog { base, core }
    }
}

impl PlanTreeNodeUnary<Logical> for LogicalChangeLog {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let core = self.core.clone_with_input(input);

        Self::with_core(core)
    }

    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let key_indices = self.core.key_indices.as_ref().map(|key| {
            key.iter()
                .map(|&index| input_col_change.map(index))
                .collect()
        });
        let changelog = Self::new(input, key_indices, self.core.need_op, true);

        let out_col_change = if self.core.need_op {
            let (mut output_vec, len) = input_col_change.into_parts();
            output_vec.push(Some(len));
            ColIndexMapping::new(output_vec, len + 1)
        } else {
            input_col_change
        };

        let (mut output_vec, len) = out_col_change.into_parts();
        let out_col_change = if self.core.need_changelog_row_id {
            output_vec.push(Some(len));
            ColIndexMapping::new(output_vec, len + 1)
        } else {
            ColIndexMapping::new(output_vec, len + 1)
        };

        (changelog, out_col_change)
    }
}

impl_plan_tree_node_for_unary! { Logical, LogicalChangeLog}
impl_distill_by_unit!(LogicalChangeLog, core, "LogicalChangeLog");

impl ExprRewritable<Logical> for LogicalChangeLog {}

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
        let mut need_changelog_row_id = false;
        let mut input_required_cols = required_cols
            .iter()
            .filter_map(|a| {
                if let Some(f) = fields.get(*a) {
                    if f.name == CHANGELOG_OP {
                        need_op = true;
                        None
                    } else if f.name == _CHANGELOG_ROW_ID {
                        need_changelog_row_id = true;
                        None
                    } else {
                        Some(*a)
                    }
                } else {
                    Some(*a)
                }
            })
            .collect_vec();

        if let Some(key) = &self.core.key_indices {
            for &index in key {
                if !input_required_cols.contains(&index) {
                    input_required_cols.push(index);
                }
            }
        }

        let new_input = self.input().prune_col(&input_required_cols, ctx);
        let input_mapping = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        let key_indices = self
            .core
            .key_indices
            .as_ref()
            .map(|key| key.iter().map(|&index| input_mapping.map(index)).collect());

        let changelog: PlanRef =
            Self::new(new_input, key_indices, need_op, need_changelog_row_id).into();

        let (mut output_mapping, new_output_len) = input_mapping.into_parts();

        if self.core.need_op {
            output_mapping.push(need_op.then_some(new_output_len));
        }

        if self.core.need_changelog_row_id {
            output_mapping
                .push(need_changelog_row_id.then_some(new_output_len + usize::from(need_op)));
        }

        let output_len = changelog.schema().len();
        let output_mapping = ColIndexMapping::new(output_mapping, output_len);
        let output_required_cols = required_cols
            .iter()
            .map(|&index| output_mapping.map(index))
            .collect_vec();

        if output_required_cols.iter().copied().eq(0..output_len) {
            changelog
        } else {
            let output_mapping =
                ColIndexMapping::with_remaining_columns(&output_required_cols, output_len);

            LogicalProject::with_mapping(changelog, output_mapping).into()
        }
    }
}

impl ToBatch for LogicalChangeLog {
    fn to_batch(&self) -> Result<BatchPlanRef> {
        Err(BindError("With changelog cte only support with create mv/sink".to_owned()).into())
    }
}

impl ToStream for LogicalChangeLog {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<StreamPlanRef> {
        if self.core.key_indices.is_some() {
            return Err(BindError("AS CHANGELOG with KEY is not supported yet".to_owned()).into());
        }

        let input = self.input().to_stream(ctx)?;
        let dist = input.distribution();
        let distribution_keys = match dist {
            Distribution::HashShard(distribution_keys)
            | Distribution::UpstreamHashShard(distribution_keys, _) => distribution_keys.clone(),
            Distribution::Single => {
                vec![]
            }
            _ => {
                return Err(BindError(format!(
                    "ChangeLog requires input to be hash distributed, single, but got {:?}",
                    dist
                ))
                .into());
            }
        };
        let core = self.core.clone_with_input(input);
        let row_id_index = self.schema().fields().len() - 1;
        let plan = StreamChangeLog::new_with_dist(
            core,
            Distribution::HashShard(vec![row_id_index]),
            distribution_keys.into_iter().map(|k| k as u32).collect(),
        )
        .into();

        Ok(plan)
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (changelog, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((changelog.into(), out_col_change))
    }
}
