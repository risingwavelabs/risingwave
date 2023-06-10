use std::collections::HashMap;
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
use std::ops::DerefMut;

pub mod plan_node;
pub use plan_node::{Explain, PlanRef};
pub mod property;

mod delta_join_solver;
mod heuristic_optimizer;
mod plan_rewriter;
pub use plan_rewriter::PlanRewriter;
mod plan_visitor;
pub use plan_visitor::{
    ExecutionModeDecider, PlanVisitor, RelationCollectorVisitor, SysTableVisitor,
};
mod logical_optimization;
mod optimizer_context;
mod plan_expr_rewriter;
mod rule;
use fixedbitset::FixedBitSet;
use itertools::Itertools as _;
pub use logical_optimization::*;
pub use optimizer_context::*;
use plan_expr_rewriter::ConstEvalRewriter;
use property::Order;
use risingwave_common::catalog::{ColumnCatalog, ColumnId, ConflictBehavior, Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_pb::catalog::WatermarkDesc;

use self::heuristic_optimizer::ApplyOrder;
use self::plan_node::{
    generic, stream_enforce_eowc_requirement, BatchProject, Convention, LogicalProject,
    LogicalSource, StreamDml, StreamMaterialize, StreamProject, StreamRowIdGen, StreamSink,
    StreamWatermarkFilter, ToStreamContext,
};
use self::plan_visitor::has_batch_exchange;
#[cfg(debug_assertions)]
use self::plan_visitor::InputRefValidator;
use self::property::RequiredDist;
use self::rule::*;
use crate::catalog::table_catalog::{TableType, TableVersion};
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::{
    BatchExchange, PlanNodeType, PlanTreeNode, RewriteExprsRecursive,
};
use crate::optimizer::plan_visitor::TemporalJoinValidator;
use crate::optimizer::property::Distribution;
use crate::utils::ColIndexMappingRewriteExt;
use crate::WithOptions;

/// `PlanRoot` is used to describe a plan. planner will construct a `PlanRoot` with `LogicalNode`.
/// and required distribution and order. And `PlanRoot` can generate corresponding streaming or
/// batch plan with optimization. the required Order and Distribution columns might be more than the
/// output columns. for example:
/// ```sql
///    select v1 from t order by id;
/// ```
/// the plan will return two columns (id, v1), and the required order column is id. the id
/// column is required in optimization, but the final generated plan will remove the unnecessary
/// column in the result.
#[derive(Debug, Clone)]
pub struct PlanRoot {
    plan: PlanRef,
    required_dist: RequiredDist,
    required_order: Order,
    out_fields: FixedBitSet,
    out_names: Vec<String>,
}

impl PlanRoot {
    pub fn new(
        plan: PlanRef,
        required_dist: RequiredDist,
        required_order: Order,
        out_fields: FixedBitSet,
        out_names: Vec<String>,
    ) -> Self {
        let input_schema = plan.schema();
        assert_eq!(input_schema.fields().len(), out_fields.len());
        assert_eq!(out_fields.count_ones(..), out_names.len());

        Self {
            plan,
            required_dist,
            required_order,
            out_fields,
            out_names,
        }
    }

    /// Set customized names of the output fields, used for `CREATE [MATERIALIZED VIEW | SINK] r(a,
    /// b, ..)`.
    ///
    /// If the number of names does not match the number of output fields, an error is returned.
    pub fn set_out_names(&mut self, out_names: Vec<String>) -> Result<()> {
        if out_names.len() != self.out_fields.count_ones(..) {
            Err(ErrorCode::InvalidInputSyntax(
                "number of column names does not match number of columns".to_string(),
            ))?
        }
        self.out_names = out_names;
        Ok(())
    }

    /// Get the plan root's schema, only including the fields to be output.
    pub fn schema(&self) -> Schema {
        // The schema can be derived from the `out_fields` and `out_names`, so we don't maintain it
        // as a field and always construct one on demand here to keep it in sync.
        Schema {
            fields: self
                .out_fields
                .ones()
                .map(|i| self.plan.schema().fields()[i].clone())
                .zip_eq_debug(&self.out_names)
                .map(|(field, name)| Field {
                    name: name.clone(),
                    ..field
                })
                .collect(),
        }
    }

    /// Get out fields of the plan root.
    pub fn out_fields(&self) -> &FixedBitSet {
        &self.out_fields
    }

    /// Transform the [`PlanRoot`] back to a [`PlanRef`] suitable to be used as a subplan, for
    /// example as insert source or subquery. This ignores Order but retains post-Order pruning
    /// (`out_fields`).
    pub fn into_subplan(self) -> PlanRef {
        if self.out_fields.count_ones(..) == self.out_fields.len() {
            return self.plan;
        }
        LogicalProject::with_out_fields(self.plan, &self.out_fields).into()
    }

    /// Apply logical optimization to the plan for stream.
    pub fn gen_optimized_logical_plan_for_stream(&self) -> Result<PlanRef> {
        LogicalOptimizer::gen_optimized_logical_plan_for_stream(self.plan.clone())
    }

    /// Apply logical optimization to the plan for batch.
    pub fn gen_optimized_logical_plan_for_batch(&self) -> Result<PlanRef> {
        LogicalOptimizer::gen_optimized_logical_plan_for_batch(self.plan.clone())
    }

    /// Optimize and generate a singleton batch physical plan without exchange nodes.
    pub fn gen_batch_plan(&mut self) -> Result<PlanRef> {
        // Logical optimization
        let mut plan = self.gen_optimized_logical_plan_for_batch()?;

        if TemporalJoinValidator::exist_dangling_temporal_scan(plan.clone()) {
            return Err(ErrorCode::NotSupported(
                "do not support temporal join for batch queries".to_string(),
                "please use temporal join in streaming queries".to_string(),
            )
            .into());
        }

        // Convert to physical plan node
        plan = plan.to_batch_with_order_required(&self.required_order)?;

        let ctx = plan.ctx();
        // Inline session timezone
        plan = inline_session_timezone_in_exprs(ctx.clone(), plan)?;

        if ctx.is_explain_trace() {
            ctx.trace("Inline Session Timezone:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        // Const eval of exprs at the last minute
        plan = const_eval_exprs(plan)?;

        if ctx.is_explain_trace() {
            ctx.trace("Const eval exprs:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        #[cfg(debug_assertions)]
        InputRefValidator.validate(plan.clone());
        assert!(*plan.distribution() == Distribution::Single, "{}", plan);
        assert!(!has_batch_exchange(plan.clone()), "{}", plan);

        let ctx = plan.ctx();
        if ctx.is_explain_trace() {
            ctx.trace("To Batch Physical Plan:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        Ok(plan)
    }

    /// Optimize and generate a batch query plan for distributed execution.
    pub fn gen_batch_distributed_plan(&mut self, batch_plan: PlanRef) -> Result<PlanRef> {
        self.set_required_dist(RequiredDist::single());
        let mut plan = batch_plan;

        // Convert to distributed plan
        plan = plan.to_distributed_with_required(&self.required_order, &self.required_dist)?;

        // Add Project if the any position of `self.out_fields` is set to zero.
        if self.out_fields.count_ones(..) != self.out_fields.len() {
            plan =
                BatchProject::new(generic::Project::with_out_fields(plan, &self.out_fields)).into();
        }

        let ctx = plan.ctx();
        if ctx.is_explain_trace() {
            ctx.trace("To Batch Distributed Plan:");
            ctx.trace(plan.explain_to_string().unwrap());
        }
        if require_additional_exchange_on_root_in_distributed_mode(plan.clone()) {
            plan =
                BatchExchange::new(plan, self.required_order.clone(), Distribution::Single).into();
        }

        Ok(plan)
    }

    /// Optimize and generate a batch query plan for local execution.
    pub fn gen_batch_local_plan(&mut self, batch_plan: PlanRef) -> Result<PlanRef> {
        let mut plan = batch_plan;

        // Convert to local plan node
        plan = plan.to_local_with_order_required(&self.required_order)?;

        // We remark that since the `to_local_with_order_required` does not enforce single
        // distribution, we enforce at the root if needed.
        let insert_exchange = match plan.distribution() {
            Distribution::Single => require_additional_exchange_on_root_in_local_mode(plan.clone()),
            _ => true,
        };
        if insert_exchange {
            plan =
                BatchExchange::new(plan, self.required_order.clone(), Distribution::Single).into()
        }

        // Add Project if the any position of `self.out_fields` is set to zero.
        if self.out_fields.count_ones(..) != self.out_fields.len() {
            plan =
                BatchProject::new(generic::Project::with_out_fields(plan, &self.out_fields)).into();
        }

        let ctx = plan.ctx();
        if ctx.is_explain_trace() {
            ctx.trace("To Batch Local Plan:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        Ok(plan)
    }

    /// Generate optimized stream plan
    fn gen_optimized_stream_plan(&mut self, emit_on_window_close: bool) -> Result<PlanRef> {
        let ctx = self.plan.ctx();
        let _explain_trace = ctx.is_explain_trace();

        let mut plan = self.gen_stream_plan(emit_on_window_close)?;

        plan = plan.optimize_by_rules(&OptimizationStage::new(
            "Merge StreamProject",
            vec![StreamProjectMergeRule::create()],
            ApplyOrder::BottomUp,
        ));

        if ctx.session_ctx().config().get_streaming_enable_delta_join() {
            // TODO: make it a logical optimization.
            // Rewrite joins with index to delta join
            plan = plan.optimize_by_rules(&OptimizationStage::new(
                "To IndexDeltaJoin",
                vec![IndexDeltaJoinRule::create()],
                ApplyOrder::BottomUp,
            ));
        }

        // Inline session timezone
        plan = inline_session_timezone_in_exprs(ctx.clone(), plan)?;

        if ctx.is_explain_trace() {
            ctx.trace("Inline session timezone:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        // Const eval of exprs at the last minute
        plan = const_eval_exprs(plan)?;

        if ctx.is_explain_trace() {
            ctx.trace("Const eval exprs:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        #[cfg(debug_assertions)]
        InputRefValidator.validate(plan.clone());

        if TemporalJoinValidator::exist_dangling_temporal_scan(plan.clone()) {
            return Err(ErrorCode::NotSupported(
                "exist dangling temporal scan".to_string(),
                "please check your temporal join syntax e.g. consider removing the right outer join if it is being used.".to_string(),
            ).into());
        }

        Ok(plan)
    }

    /// Generate create index or create materialize view plan.
    fn gen_stream_plan(&mut self, emit_on_window_close: bool) -> Result<PlanRef> {
        let ctx = self.plan.ctx();
        let explain_trace = ctx.is_explain_trace();

        let plan = match self.plan.convention() {
            Convention::Logical => {
                let plan = self.gen_optimized_logical_plan_for_stream()?;

                let (plan, out_col_change) = {
                    let (plan, out_col_change) =
                        plan.logical_rewrite_for_stream(&mut Default::default())?;
                    if out_col_change.is_injective() {
                        (plan, out_col_change)
                    } else {
                        let mut output_indices = (0..plan.schema().len()).collect_vec();
                        #[allow(unused_assignments)]
                        let (mut map, mut target_size) = out_col_change.into_parts();

                        // TODO(st1page): https://github.com/risingwavelabs/risingwave/issues/7234
                        // assert_eq!(target_size, output_indices.len());
                        target_size = plan.schema().len();
                        let mut tar_exists = vec![false; target_size];
                        for i in map.iter_mut().flatten() {
                            if tar_exists[*i] {
                                output_indices.push(*i);
                                *i = target_size;
                                target_size += 1;
                            } else {
                                tar_exists[*i] = true;
                            }
                        }
                        let plan =
                            LogicalProject::with_out_col_idx(plan, output_indices.into_iter());
                        let out_col_change = ColIndexMapping::with_target_size(map, target_size);
                        (plan.into(), out_col_change)
                    }
                };

                if explain_trace {
                    ctx.trace("Logical Rewrite For Stream:");
                    ctx.trace(plan.explain_to_string().unwrap());
                }

                self.required_dist =
                    out_col_change.rewrite_required_distribution(&self.required_dist);
                self.required_order = out_col_change
                    .rewrite_required_order(&self.required_order)
                    .unwrap();
                self.out_fields = out_col_change.rewrite_bitset(&self.out_fields);
                let plan = plan.to_stream_with_dist_required(
                    &self.required_dist,
                    &mut ToStreamContext::new(emit_on_window_close),
                )?;
                stream_enforce_eowc_requirement(ctx.clone(), plan, emit_on_window_close)
            }
            _ => unreachable!(),
        }?;

        if explain_trace {
            ctx.trace("To Stream Plan:");
            ctx.trace(plan.explain_to_string().unwrap());
        }
        Ok(plan)
    }

    /// Optimize and generate a create table plan.
    #[allow(clippy::too_many_arguments)]
    pub fn gen_table_plan(
        &mut self,
        context: OptimizerContextRef,
        table_name: String,
        columns: Vec<ColumnCatalog>,
        definition: String,
        pk_column_ids: Vec<ColumnId>,
        row_id_index: Option<usize>,
        append_only: bool,
        watermark_descs: Vec<WatermarkDesc>,
        version: Option<TableVersion>,
    ) -> Result<StreamMaterialize> {
        let mut stream_plan = self.gen_optimized_stream_plan(false)?;

        // Add DML node.
        stream_plan = StreamDml::new(
            stream_plan,
            append_only,
            columns
                .iter()
                .filter_map(|c| (!c.is_generated()).then(|| c.column_desc.clone()))
                .collect(),
        )
        .into();

        // Add generated columns.
        let exprs = LogicalSource::derive_output_exprs_from_generated_columns(&columns)?;
        if let Some(exprs) = exprs {
            let logical_project = generic::Project::new(exprs, stream_plan);
            stream_plan = StreamProject::new(logical_project).into();
        }

        // Add WatermarkFilter node.
        if !watermark_descs.is_empty() {
            stream_plan = StreamWatermarkFilter::new(stream_plan, watermark_descs).into();
        }

        // Add RowIDGen node if needed.
        if let Some(row_id_index) = row_id_index {
            stream_plan = StreamRowIdGen::new(stream_plan, row_id_index).into();
        }

        let conflict_behavior = match append_only {
            true => ConflictBehavior::NoCheck,
            false => ConflictBehavior::Overwrite,
        };

        let pk_column_indices = {
            let mut id_to_idx = HashMap::new();

            columns.iter().enumerate().for_each(|(idx, c)| {
                id_to_idx.insert(c.column_id(), idx);
            });
            pk_column_ids
                .iter()
                .map(|c| id_to_idx.get(c).copied().unwrap()) // pk column id must exist in table columns.
                .collect_vec()
        };

        let table_required_dist = {
            let mut bitset = FixedBitSet::with_capacity(columns.len());
            for idx in &pk_column_indices {
                bitset.insert(*idx);
            }
            RequiredDist::ShardByKey(bitset)
        };

        let stream_plan = inline_session_timezone_in_exprs(context, stream_plan)?;

        StreamMaterialize::create_for_table(
            stream_plan,
            table_name,
            table_required_dist,
            Order::any(),
            columns,
            definition,
            conflict_behavior,
            pk_column_indices,
            row_id_index,
            version,
        )
    }

    /// Optimize and generate a create materialized view plan.
    pub fn gen_materialize_plan(
        &mut self,
        mv_name: String,
        definition: String,
        emit_on_window_close: bool,
    ) -> Result<StreamMaterialize> {
        let stream_plan = self.gen_optimized_stream_plan(emit_on_window_close)?;

        StreamMaterialize::create(
            stream_plan,
            mv_name,
            self.required_dist.clone(),
            self.required_order.clone(),
            self.out_fields.clone(),
            self.out_names.clone(),
            definition,
            TableType::MaterializedView,
        )
    }

    /// Optimize and generate a create index plan.
    pub fn gen_index_plan(
        &mut self,
        index_name: String,
        definition: String,
    ) -> Result<StreamMaterialize> {
        let stream_plan = self.gen_optimized_stream_plan(false)?;

        StreamMaterialize::create(
            stream_plan,
            index_name,
            self.required_dist.clone(),
            self.required_order.clone(),
            self.out_fields.clone(),
            self.out_names.clone(),
            definition,
            TableType::Index,
        )
    }

    /// Optimize and generate a create sink plan.
    pub fn gen_sink_plan(
        &mut self,
        sink_name: String,
        definition: String,
        properties: WithOptions,
    ) -> Result<StreamSink> {
        let stream_plan = self.gen_optimized_stream_plan(false)?;

        StreamSink::create(
            stream_plan,
            sink_name,
            self.required_dist.clone(),
            self.required_order.clone(),
            self.out_fields.clone(),
            self.out_names.clone(),
            definition,
            properties,
        )
    }

    /// Set the plan root's required dist.
    pub fn set_required_dist(&mut self, required_dist: RequiredDist) {
        self.required_dist = required_dist;
    }
}

fn const_eval_exprs(plan: PlanRef) -> Result<PlanRef> {
    let mut const_eval_rewriter = ConstEvalRewriter { error: None };

    let plan = plan.rewrite_exprs_recursive(&mut const_eval_rewriter);
    if let Some(error) = const_eval_rewriter.error {
        return Err(error);
    }
    Ok(plan)
}

fn inline_session_timezone_in_exprs(ctx: OptimizerContextRef, plan: PlanRef) -> Result<PlanRef> {
    let plan = plan.rewrite_exprs_recursive(ctx.session_timezone().deref_mut());
    Ok(plan)
}

fn exist_and_no_exchange_before(plan: &PlanRef, is_candidate: fn(&PlanRef) -> bool) -> bool {
    if plan.node_type() == PlanNodeType::BatchExchange {
        return false;
    }
    is_candidate(plan)
        || plan
            .inputs()
            .iter()
            .any(|input| exist_and_no_exchange_before(input, is_candidate))
}

/// As we always run the root stage locally, for some plan in root stage which need to execute in
/// compute node we insert an additional exhchange before it to avoid to include it in the root
/// stage.
///
/// Returns `true` if we must insert an additional exchange to ensure this.
fn require_additional_exchange_on_root_in_distributed_mode(plan: PlanRef) -> bool {
    fn is_user_table(plan: &PlanRef) -> bool {
        plan.as_batch_seq_scan()
            .map(|node| !node.logical().is_sys_table)
            .unwrap_or(false)
    }

    fn is_source(plan: &PlanRef) -> bool {
        plan.node_type() == PlanNodeType::BatchSource
    }

    fn is_insert(plan: &PlanRef) -> bool {
        plan.node_type() == PlanNodeType::BatchInsert
    }

    fn is_update(plan: &PlanRef) -> bool {
        plan.node_type() == PlanNodeType::BatchUpdate
    }

    fn is_delete(plan: &PlanRef) -> bool {
        plan.node_type() == PlanNodeType::BatchDelete
    }

    assert_eq!(plan.distribution(), &Distribution::Single);
    exist_and_no_exchange_before(&plan, is_user_table)
        || exist_and_no_exchange_before(&plan, is_source)
        || exist_and_no_exchange_before(&plan, is_insert)
        || exist_and_no_exchange_before(&plan, is_update)
        || exist_and_no_exchange_before(&plan, is_delete)
}

/// The purpose is same as `require_additional_exchange_on_root_in_distributed_mode`. We separate
/// them for the different requirement of plan node in different execute mode.
fn require_additional_exchange_on_root_in_local_mode(plan: PlanRef) -> bool {
    fn is_user_table(plan: &PlanRef) -> bool {
        plan.as_batch_seq_scan()
            .map(|node| !node.logical().is_sys_table)
            .unwrap_or(false)
    }

    fn is_source(plan: &PlanRef) -> bool {
        plan.node_type() == PlanNodeType::BatchSource
    }

    fn is_insert(plan: &PlanRef) -> bool {
        plan.node_type() == PlanNodeType::BatchInsert
    }

    assert_eq!(plan.distribution(), &Distribution::Single);
    exist_and_no_exchange_before(&plan, is_user_table)
        || exist_and_no_exchange_before(&plan, is_source)
        || exist_and_no_exchange_before(&plan, is_insert)
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::LogicalValues;

    #[tokio::test]
    async fn test_as_subplan() {
        let ctx = OptimizerContext::mock().await;
        let values = LogicalValues::new(
            vec![],
            Schema::new(vec![
                Field::with_name(DataType::Int32, "v1"),
                Field::with_name(DataType::Varchar, "v2"),
            ]),
            ctx,
        )
        .into();
        let out_fields = FixedBitSet::with_capacity_and_blocks(2, [1]);
        let out_names = vec!["v1".into()];
        let root = PlanRoot::new(
            values,
            RequiredDist::Any,
            Order::any(),
            out_fields,
            out_names,
        );
        let subplan = root.into_subplan();
        assert_eq!(
            subplan.schema(),
            &Schema::new(vec![Field::with_name(DataType::Int32, "v1"),])
        );
    }
}
