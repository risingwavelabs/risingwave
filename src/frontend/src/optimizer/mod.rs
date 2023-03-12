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
pub use plan_visitor::PlanVisitor;
mod optimizer_context;
mod plan_expr_rewriter;
mod rule;
use fixedbitset::FixedBitSet;
use itertools::Itertools as _;
pub use optimizer_context::*;
use plan_expr_rewriter::ConstEvalRewriter;
use plan_rewriter::ShareSourceRewriter;
use property::Order;
use risingwave_common::catalog::{ColumnCatalog, Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::util::iter_util::ZipEqDebug;

use self::heuristic_optimizer::{ApplyOrder, HeuristicOptimizer};
use self::plan_node::{
    BatchProject, Convention, LogicalProject, StreamDml, StreamMaterialize, StreamProject,
    StreamRowIdGen, StreamSink,
};
#[cfg(debug_assertions)]
use self::plan_visitor::InputRefValidator;
use self::plan_visitor::{
    has_batch_delete, has_batch_exchange, has_batch_insert, has_batch_update, has_logical_apply,
    has_logical_over_agg, HasMaxOneRowApply,
};
use self::property::RequiredDist;
use self::rule::*;
use crate::catalog::table_catalog::{TableType, TableVersion};
use crate::expr::InputRef;
use crate::optimizer::plan_node::{
    BatchExchange, ColumnPruningContext, PlanNodeType, PlanTreeNode, PredicatePushdownContext,
    RewriteExprsRecursive,
};
use crate::optimizer::property::Distribution;
use crate::utils::{ColIndexMappingRewriteExt, Condition};
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

    fn optimize_by_rules(
        &self,
        plan: PlanRef,
        stage_name: String,
        rules: Vec<BoxedRule>,
        apply_order: ApplyOrder,
    ) -> PlanRef {
        let mut heuristic_optimizer = HeuristicOptimizer::new(&apply_order, &rules);
        let plan = heuristic_optimizer.optimize(plan);
        let stats = heuristic_optimizer.get_stats();

        let ctx = plan.ctx();
        let explain_trace = ctx.is_explain_trace();
        if explain_trace && stats.has_applied_rule() {
            ctx.trace(format!("{}:", stage_name));
            ctx.trace(format!("{}", stats));
            ctx.trace(plan.explain_to_string().unwrap());
        }

        plan
    }

    fn optimize_by_rules_until_fix_point(
        &self,
        plan: PlanRef,
        stage_name: String,
        rules: Vec<BoxedRule>,
        apply_order: ApplyOrder,
    ) -> PlanRef {
        let mut output_plan = plan;
        loop {
            let mut heuristic_optimizer = HeuristicOptimizer::new(&apply_order, &rules);
            output_plan = heuristic_optimizer.optimize(output_plan);
            let stats = heuristic_optimizer.get_stats();

            let ctx = output_plan.ctx();
            let explain_trace = ctx.is_explain_trace();
            if explain_trace && stats.has_applied_rule() {
                ctx.trace(format!("{}:", stage_name));
                ctx.trace(format!("{}", stats));
                ctx.trace(output_plan.explain_to_string().unwrap());
            }

            if !stats.has_applied_rule() {
                return output_plan;
            }
        }
    }

    /// Apply logical optimization to the plan.
    pub fn gen_optimized_logical_plan(&self) -> Result<PlanRef> {
        self.gen_optimized_logical_plan_inner(false)
    }

    fn gen_optimized_logical_plan_inner(&self, for_stream: bool) -> Result<PlanRef> {
        let mut plan = self.plan.clone();
        let ctx = plan.ctx();
        let explain_trace = ctx.is_explain_trace();

        if explain_trace {
            ctx.trace("Begin:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        // If share plan is disable, we need to remove all the share operator generated by the
        // binder, e.g. CTE and View. However, we still need to share source to ensure self
        // source join can return correct result.
        if ctx.session_ctx().config().get_enable_share_plan() && for_stream {
            // Common sub-plan detection.
            plan = plan.merge_eq_nodes();
            plan = plan.prune_share();
            if explain_trace {
                ctx.trace("Merging equivalent nodes:");
                ctx.trace(plan.explain_to_string().unwrap());
            }
        } else {
            plan = self.optimize_by_rules(
                plan,
                "DAG To Tree".to_string(),
                vec![DagToTreeRule::create()],
                ApplyOrder::TopDown,
            );

            // Replace source to share source.
            // Perform share source at the beginning so that we can benefit from predicate pushdown
            // and column pruning for the share operator.
            if for_stream {
                plan = ShareSourceRewriter::share_source(plan);
                if explain_trace {
                    ctx.trace("Share Source:");
                    ctx.trace(plan.explain_to_string().unwrap());
                }
            }
        }

        plan = self.optimize_by_rules(
            plan,
            "Rewrite Like Expr".to_string(),
            vec![RewriteLikeExprRule::create()],
            ApplyOrder::TopDown,
        );

        // Simple Unnesting.
        plan = self.optimize_by_rules(
            plan,
            "Simple Unnesting".to_string(),
            vec![
                // Eliminate max one row
                MaxOneRowEliminateRule::create(),
                // Convert apply to join.
                ApplyToJoinRule::create(),
                // Pull correlated predicates up the algebra tree to unnest simple subquery.
                PullUpCorrelatedPredicateRule::create(),
            ],
            ApplyOrder::TopDown,
        );
        if HasMaxOneRowApply().visit(plan.clone()) {
            return Err(ErrorCode::InternalError(
                "Scalar subquery might produce more than one row.".into(),
            )
            .into());
        }

        plan = self.optimize_by_rules(
            plan,
            "Union Merge".to_string(),
            vec![UnionMergeRule::create()],
            ApplyOrder::BottomUp,
        );

        // Predicate push down before translate apply, because we need to calculate the domain
        // and predicate push down can reduce the size of domain.
        plan = plan.predicate_pushdown(
            Condition::true_cond(),
            &mut PredicatePushdownContext::new(plan.clone()),
        );
        if explain_trace {
            ctx.trace("Predicate Push Down:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        // General Unnesting.
        // Translate Apply, push Apply down the plan and finally replace Apply with regular inner
        // join.
        plan = self.optimize_by_rules(
            plan,
            "General Unnesting(Translate Apply)".to_string(),
            vec![TranslateApplyRule::create()],
            ApplyOrder::BottomUp,
        );
        plan = self.optimize_by_rules_until_fix_point(
            plan,
            "General Unnesting(Push Down Apply)".to_string(),
            vec![
                ApplyAggTransposeRule::create(),
                ApplyFilterTransposeRule::create(),
                ApplyProjectTransposeRule::create(),
                ApplyJoinTransposeRule::create(),
                ApplyShareEliminateRule::create(),
                ApplyScanRule::create(),
            ],
            ApplyOrder::TopDown,
        );
        if has_logical_apply(plan.clone()) {
            return Err(ErrorCode::InternalError("Subquery can not be unnested.".into()).into());
        }

        // Predicate Push-down
        plan = plan.predicate_pushdown(
            Condition::true_cond(),
            &mut PredicatePushdownContext::new(plan.clone()),
        );
        if explain_trace {
            ctx.trace("Predicate Push Down:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        // Merge inner joins and intermediate filters into multijoin
        // This rule assumes that filters have already been pushed down near to
        // their relevant joins.
        plan = self.optimize_by_rules(
            plan,
            "To MultiJoin".to_string(),
            vec![MergeMultiJoinRule::create()],
            ApplyOrder::TopDown,
        );

        // Reorder multijoin into left-deep join tree.
        plan = if for_stream {
            self.optimize_by_rules(
                plan,
                "Join Reorder Streaming".to_string(),
                vec![ReorderMultiJoinRuleStreaming::create()],
                ApplyOrder::TopDown,
            )
        } else {
            self.optimize_by_rules(
                plan,
                "Join Reorder".to_string(),
                vec![ReorderMultiJoinRule::create()],
                ApplyOrder::TopDown,
            )
        };

        // Predicate Push-down: apply filter pushdown rules again since we pullup all join
        // conditions into a filter above the multijoin.
        plan = plan.predicate_pushdown(
            Condition::true_cond(),
            &mut PredicatePushdownContext::new(plan.clone()),
        );
        if explain_trace {
            ctx.trace("Predicate Push Down:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        // If for stream, push down predicates with now into a left-semi join
        if for_stream {
            plan = self.optimize_by_rules(
                plan,
                "Push down filter with now into a left semijoin".to_string(),
                vec![FilterWithNowToJoinRule::create()],
                ApplyOrder::TopDown,
            );
        }

        // Push down the calculation of inputs of join's condition.
        plan = self.optimize_by_rules(
            plan,
            "Push Down the Calculation of Inputs of Join's Condition".to_string(),
            vec![PushCalculationOfJoinRule::create()],
            ApplyOrder::TopDown,
        );

        // Prune Columns
        //
        // Currently, the expressions in ORDER BY will be merged into the expressions in SELECT and
        // they shouldn't be a part of output columns, so we use `out_fields` to control the
        // visibility of these expressions. To avoid these expressions being pruned, we can't use
        // `self.out_fields` as `required_cols` here.
        let required_cols = (0..self.plan.schema().len()).collect_vec();
        let mut column_pruning_ctx = ColumnPruningContext::new(plan.clone());
        plan = plan.prune_col(&required_cols, &mut column_pruning_ctx);
        // Column pruning may introduce additional projects, and filter can be pushed again.
        if explain_trace {
            ctx.trace("Prune Columns:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        if column_pruning_ctx.need_second_round() {
            // Second round of column pruning and reuse the column pruning context.
            // Try to replace original share operator with the new one.
            plan = plan.prune_col(&required_cols, &mut column_pruning_ctx);
            if explain_trace {
                ctx.trace("Prune Columns (For DAG):");
                ctx.trace(plan.explain_to_string().unwrap());
            }
        }

        plan = plan.predicate_pushdown(
            Condition::true_cond(),
            &mut PredicatePushdownContext::new(plan.clone()),
        );
        if explain_trace {
            ctx.trace("Predicate Push Down:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        // Convert distinct aggregates.
        plan = self.optimize_by_rules(
            plan,
            "Convert Distinct Aggregation".to_string(),
            vec![
                UnionToDistinctRule::create(),
                DistinctAggRule::create(for_stream),
            ],
            ApplyOrder::TopDown,
        );

        plan = self.optimize_by_rules(
            plan,
            "Join Commute".to_string(),
            vec![JoinCommuteRule::create()],
            ApplyOrder::TopDown,
        );

        plan = self.optimize_by_rules(
            plan,
            "Project Remove".to_string(),
            vec![
                // merge should be applied before eliminate
                ProjectMergeRule::create(),
                ProjectEliminateRule::create(),
                TrivialProjectToValuesRule::create(),
                UnionInputValuesMergeRule::create(),
                // project-join merge should be applied after merge
                // eliminate and to values
                ProjectJoinMergeRule::create(),
                AggProjectMergeRule::create(),
            ],
            ApplyOrder::BottomUp,
        );

        plan = self.optimize_by_rules(
            plan,
            "Convert Window Aggregation".to_string(),
            vec![
                OverAggToTopNRule::create(),
                ProjectMergeRule::create(),
                ProjectEliminateRule::create(),
                TrivialProjectToValuesRule::create(),
                UnionInputValuesMergeRule::create(),
            ],
            ApplyOrder::TopDown,
        );

        if has_logical_over_agg(plan.clone()) {
            return Err(ErrorCode::InternalError(format!(
                "OverAgg can not be transformed. Plan:\n{}",
                plan.explain_to_string().unwrap()
            ))
            .into());
        }

        plan = self.optimize_by_rules(
            plan,
            "Dedup Group keys".to_string(),
            vec![AggDedupGroupKeyRule::create()],
            ApplyOrder::TopDown,
        );

        #[cfg(debug_assertions)]
        InputRefValidator.validate(plan.clone());

        if ctx.is_explain_logical() {
            ctx.store_logical(plan.explain_to_string().unwrap());
        }

        Ok(plan)
    }

    /// Optimize and generate a singleton batch physical plan without exchange nodes.
    fn gen_batch_plan(&mut self) -> Result<PlanRef> {
        // Logical optimization
        let mut plan = self.gen_optimized_logical_plan()?;

        // Convert the dag back to the tree, because we don't support physical dag plan for now.
        plan = self.optimize_by_rules(
            plan,
            "DAG To Tree".to_string(),
            vec![DagToTreeRule::create()],
            ApplyOrder::TopDown,
        );

        plan = self.optimize_by_rules(
            plan,
            "Agg on Index".to_string(),
            vec![TopNOnIndexRule::create()],
            ApplyOrder::TopDown,
        );

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

    /// As we always run the root stage locally, we should ensure that singleton table scan is not
    /// the root stage. Returns `true` if we must insert an additional exchange to ensure this.
    fn require_additional_exchange_on_root(plan: PlanRef) -> bool {
        fn is_candidate_table_scan(plan: &PlanRef) -> bool {
            if let Some(node) = plan.as_batch_seq_scan()
            && !node.logical().is_sys_table() {
                true
            } else {
                plan.node_type() == PlanNodeType::BatchSource
            }
        }

        fn no_exchange_before_table_scan(plan: PlanRef) -> bool {
            if plan.node_type() == PlanNodeType::BatchExchange {
                return false;
            }
            is_candidate_table_scan(&plan)
                || plan.inputs().into_iter().any(no_exchange_before_table_scan)
        }

        assert_eq!(plan.distribution(), &Distribution::Single);
        no_exchange_before_table_scan(plan)

        // TODO: join between a normal table and a system table is not supported yet
    }

    /// Optimize and generate a batch query plan for distributed execution.
    pub fn gen_batch_distributed_plan(&mut self) -> Result<PlanRef> {
        self.set_required_dist(RequiredDist::single());
        let mut plan = self.gen_batch_plan()?;

        // Convert to distributed plan
        plan = plan.to_distributed_with_required(&self.required_order, &self.required_dist)?;

        // Add Project if the any position of `self.out_fields` is set to zero.
        if self.out_fields.count_ones(..) != self.out_fields.len() {
            plan =
                BatchProject::new(LogicalProject::with_out_fields(plan, &self.out_fields)).into();
        }

        let ctx = plan.ctx();
        if ctx.is_explain_trace() {
            ctx.trace("To Batch Distributed Plan:");
            ctx.trace(plan.explain_to_string().unwrap());
        }
        if has_batch_insert(plan.clone())
            || has_batch_delete(plan.clone())
            || has_batch_update(plan.clone())
            || Self::require_additional_exchange_on_root(plan.clone())
        {
            plan =
                BatchExchange::new(plan, self.required_order.clone(), Distribution::Single).into();
        }

        Ok(plan)
    }

    /// Optimize and generate a batch query plan for local execution.
    pub fn gen_batch_local_plan(&mut self) -> Result<PlanRef> {
        let mut plan = self.gen_batch_plan()?;

        // Convert to local plan node
        plan = plan.to_local_with_order_required(&self.required_order)?;

        // We remark that since the `to_local_with_order_required` does not enforce single
        // distribution, we enforce at the root if needed.
        let insert_exchange = match plan.distribution() {
            Distribution::Single => Self::require_additional_exchange_on_root(plan.clone()),
            _ => true,
        };
        if insert_exchange {
            plan =
                BatchExchange::new(plan, self.required_order.clone(), Distribution::Single).into()
        }

        // Add Project if the any position of `self.out_fields` is set to zero.
        if self.out_fields.count_ones(..) != self.out_fields.len() {
            plan =
                BatchProject::new(LogicalProject::with_out_fields(plan, &self.out_fields)).into();
        }

        let ctx = plan.ctx();
        if ctx.is_explain_trace() {
            ctx.trace("To Batch Local Plan:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        Ok(plan)
    }

    pub fn gen_optimized_logical_plan_for_stream(&self) -> Result<PlanRef> {
        self.gen_optimized_logical_plan_inner(true)
    }

    /// Generate create index or create materialize view plan.
    fn gen_stream_plan(&mut self) -> Result<PlanRef> {
        let ctx = self.plan.ctx();
        let explain_trace = ctx.is_explain_trace();

        let mut plan = match self.plan.convention() {
            Convention::Logical => {
                let plan = self.gen_optimized_logical_plan_for_stream()?;

                let (plan, out_col_change) =
                    plan.logical_rewrite_for_stream(&mut Default::default())?;

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
                plan.to_stream_with_dist_required(&self.required_dist, &mut Default::default())
            }
            _ => unreachable!(),
        }?;

        if explain_trace {
            ctx.trace("To Stream Plan:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        if ctx.session_ctx().config().get_streaming_enable_delta_join() {
            // TODO: make it a logical optimization.
            // Rewrite joins with index to delta join
            plan = self.optimize_by_rules(
                plan,
                "To IndexDeltaJoin".to_string(),
                vec![IndexDeltaJoinRule::create()],
                ApplyOrder::BottomUp,
            );
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

        Ok(plan)
    }

    /// Optimize and generate a create table plan.
    #[allow(clippy::too_many_arguments)]
    pub fn gen_table_plan(
        &mut self,
        table_name: String,
        columns: Vec<ColumnCatalog>,
        definition: String,
        row_id_index: Option<usize>,
        append_only: bool,
        version: Option<TableVersion>,
    ) -> Result<StreamMaterialize> {
        let mut stream_plan = self.gen_stream_plan()?;

        // Add DML node.
        stream_plan = StreamDml::new(
            stream_plan,
            append_only,
            columns.iter().map(|c| c.column_desc.clone()).collect(),
        )
        .into();
        // Add RowIDGen node if needed.
        if let Some(row_id_index) = row_id_index {
            stream_plan = StreamRowIdGen::new(stream_plan, row_id_index).into();
        }

        StreamMaterialize::create_for_table(
            stream_plan,
            table_name,
            self.required_dist.clone(),
            self.required_order.clone(),
            columns,
            definition,
            !append_only,
            row_id_index,
            version,
        )
    }

    /// Optimize and generate a create materialized view plan.
    pub fn gen_materialize_plan(
        &mut self,
        mv_name: String,
        definition: String,
    ) -> Result<StreamMaterialize> {
        let stream_plan = self.gen_stream_plan()?;

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
        let stream_plan = self.gen_stream_plan()?;

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
        let mut stream_plan = self.gen_stream_plan()?;

        // Add a project node if there is hidden column(s).
        let input_fields = stream_plan.schema().fields();
        if input_fields.len() != self.out_fields.count_ones(..) {
            let exprs = input_fields
                .iter()
                .enumerate()
                .filter_map(|(idx, field)| {
                    if self.out_fields.contains(idx) {
                        Some(InputRef::new(idx, field.data_type.clone()).into())
                    } else {
                        None
                    }
                })
                .collect_vec();
            stream_plan = StreamProject::new(LogicalProject::new(stream_plan, exprs)).into();
        }

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
