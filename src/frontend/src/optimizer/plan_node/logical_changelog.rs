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
use crate::optimizer::property::{Distribution, RequiredDist};
use crate::utils::{ColIndexMapping, Condition};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalChangeLog {
    pub base: PlanBase<Logical>,
    core: generic::ChangeLog<PlanRef>,
}

impl LogicalChangeLog {
    pub fn create(input: PlanRef) -> PlanRef {
        Self::new(input, true, true).into()
    }

    pub fn new(input: PlanRef, need_op: bool, need_changelog_row_id: bool) -> Self {
        let core = generic::ChangeLog::new(input, need_op, need_changelog_row_id);
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
        Self::new(input, self.core.need_op, self.core.need_changelog_row_id)
    }

    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let changelog = Self::new(input, self.core.need_op, true);

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
        let requested_input_cols: Vec<_> = required_cols
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
            .collect();

        // `StreamChangeLog` must see the input stream key to co-locate changes for the same row.
        // Keep key columns internally even when they are not selected from the changelog CTE, and
        // project them away above the operator afterwards.
        let mut input_required_cols = requested_input_cols.clone();
        if let Some(stream_key) = self.input().stream_key() {
            for &key in stream_key {
                if !input_required_cols.contains(&key) {
                    input_required_cols.push(key);
                }
            }
        }
        let input_col_change = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        let new_input = self.input().prune_col(&input_required_cols, ctx);
        let new_input_len = new_input.schema().len();
        let changelog: PlanRef = Self::new(new_input, need_op, need_changelog_row_id).into();

        if input_required_cols == requested_input_cols {
            return changelog;
        }

        let output_required_cols = required_cols
            .iter()
            .map(|index| {
                let field = &fields[*index];
                if field.name == CHANGELOG_OP {
                    new_input_len
                } else if field.name == _CHANGELOG_ROW_ID {
                    new_input_len + usize::from(need_op)
                } else {
                    input_col_change.map(*index)
                }
            })
            .collect::<Vec<_>>();
        let source_size = changelog.schema().len();
        LogicalProject::with_mapping(
            changelog,
            ColIndexMapping::with_remaining_columns(&output_required_cols, source_size),
        )
        .into()
    }
}

impl ToBatch for LogicalChangeLog {
    fn to_batch(&self) -> Result<BatchPlanRef> {
        Err(BindError("With changelog cte only support with create mv/sink".to_owned()).into())
    }
}

impl ToStream for LogicalChangeLog {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<StreamPlanRef> {
        let mut input = self.input().to_stream(ctx)?;
        if matches!(input.distribution(), Distribution::SomeShard) {
            input = RequiredDist::hash_shard(input.expect_stream_key())
                .streaming_enforce_if_not_satisfies(input)?;
        }
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

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use risingwave_common::catalog::{CdcTableDesc, ColumnDesc, ColumnId, TableId};
    use risingwave_common::id::SourceId;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
    use risingwave_connector::source::cdc::CdcScanOptions;

    use super::*;
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::{BackfillType, LogicalCdcScan, PlanTreeNodeUnary};

    #[test]
    fn test_changelog_on_cdc_scan_enforces_hash_distribution() {
        let desc = CdcTableDesc {
            table_id: TableId::new(1),
            source_id: SourceId::new(2),
            external_table_name: "mydb.orders".to_owned(),
            pk: vec![ColumnOrder::new(0, OrderType::ascending())],
            columns: vec![
                ColumnDesc::named("id", ColumnId::new(1), DataType::Int32),
                ColumnDesc::named("payload", ColumnId::new(2), DataType::Varchar),
            ],
            stream_key: vec![0],
            ..Default::default()
        };
        let scan = LogicalCdcScan::create(
            "orders".to_owned(),
            Rc::new(desc),
            OptimizerContext::mock(),
            CdcScanOptions {
                disable_backfill: true,
                ..Default::default()
            },
        );
        let changelog = LogicalChangeLog::create(scan.clone().into());
        let stream = changelog
            .to_stream(&mut ToStreamContext::new_with_backfill_type(
                false,
                BackfillType::Replicated,
            ))
            .unwrap();

        let input = stream.as_stream_change_log().unwrap().input();
        assert!(input.as_stream_exchange().is_some());
        assert_eq!(input.distribution(), &Distribution::HashShard(vec![0]));

        // The primary key is retained internally even when it is not selected from the changelog.
        let changelog = LogicalChangeLog::create(scan.into());
        let pruned = changelog.prune_col(
            &[1, 2, 3],
            &mut ColumnPruningContext::new(changelog.clone()),
        );
        let stream = pruned
            .to_stream(&mut ToStreamContext::new_with_backfill_type(
                false,
                BackfillType::Replicated,
            ))
            .unwrap();
        let changelog = stream.as_stream_project().unwrap().input();
        let input = changelog.as_stream_change_log().unwrap().input();
        assert!(input.as_stream_exchange().is_some());
        assert_eq!(input.distribution(), &Distribution::HashShard(vec![1]));
    }
}
