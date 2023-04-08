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

use itertools::Itertools;
use lazy_static::lazy_static;
use risingwave_common::error::{ErrorCode, Result};

use crate::optimizer::heuristic_optimizer::{ApplyOrder, HeuristicOptimizer};
use crate::optimizer::plan_node::{ColumnPruningContext, PredicatePushdownContext};
use crate::optimizer::plan_rewriter::ShareSourceRewriter;
#[cfg(debug_assertions)]
use crate::optimizer::plan_visitor::InputRefValidator;
use crate::optimizer::plan_visitor::{
    has_logical_apply, has_logical_over_agg, HasMaxOneRowApply, PlanVisitor,
};
use crate::optimizer::rule::*;
use crate::optimizer::PlanRef;
use crate::utils::Condition;
use crate::{Explain, OptimizerContextRef};

impl PlanRef {
    pub(crate) fn optimize_by_rules(self, stage: &OptimizationStage) -> PlanRef {
        let OptimizationStage {
            stage_name,
            rules,
            apply_order,
        } = stage;

        let mut heuristic_optimizer = HeuristicOptimizer::new(apply_order, rules);
        let plan = heuristic_optimizer.optimize(self);
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

    pub(crate) fn optimize_by_rules_until_fix_point(self, stage: &OptimizationStage) -> PlanRef {
        let OptimizationStage {
            stage_name,
            rules,
            apply_order,
        } = stage;

        let mut output_plan = self;
        loop {
            let mut heuristic_optimizer = HeuristicOptimizer::new(apply_order, rules);
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
}

pub struct OptimizationStage {
    stage_name: String,
    rules: Vec<BoxedRule>,
    apply_order: ApplyOrder,
}

impl OptimizationStage {
    pub fn new<S>(name: S, rules: Vec<BoxedRule>, apply_order: ApplyOrder) -> Self
    where
        S: Into<String>,
    {
        OptimizationStage {
            stage_name: name.into(),
            rules,
            apply_order,
        }
    }
}

pub struct LogicalOptimizer {}

lazy_static! {
    static ref DAG_TO_TREE: OptimizationStage = OptimizationStage::new(
        "DAG To Tree",
        vec![DagToTreeRule::create()],
        ApplyOrder::TopDown,
    );

    static ref SIMPLE_UNNESTING: OptimizationStage = OptimizationStage::new(
        "Simple Unnesting",
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

    static ref UNION_MERGE: OptimizationStage = OptimizationStage::new(
        "Union Merge",
        vec![UnionMergeRule::create()],
        ApplyOrder::BottomUp,
    );


    static ref GENERAL_UNNESTING_TRANS_APPLY_WITH_SHARE: OptimizationStage = OptimizationStage::new(
        "General Unnesting(Translate Apply)",
        vec![TranslateApplyRule::create(true)],
        ApplyOrder::BottomUp,
    );

    static ref GENERAL_UNNESTING_TRANS_APPLY_WITHOUT_SHARE: OptimizationStage = OptimizationStage::new(
        "General Unnesting(Translate Apply)",
        vec![TranslateApplyRule::create(false)],
        ApplyOrder::BottomUp,
    );

    static ref GENERAL_UNNESTING_PUSH_DOWN_APPLY: OptimizationStage = OptimizationStage::new(
        "General Unnesting(Push Down Apply)",
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

    static ref TO_MULTI_JOIN: OptimizationStage = OptimizationStage::new(
        "To MultiJoin",
        vec![MergeMultiJoinRule::create()],
        ApplyOrder::TopDown,
    );

    static ref LEFT_DEEP_JOIN_REORDER: OptimizationStage = OptimizationStage::new(
        "Join Reorder".to_string(),
        vec![LeftDeepTreeJoinOrderingRule::create()],
        ApplyOrder::TopDown,
    );

    static ref BUSHY_TREE_JOIN_REORDER: OptimizationStage = OptimizationStage::new(
        "Bushy tree join ordering Rule".to_string(),
        vec![BushyTreeJoinOrderingRule::create()],
        ApplyOrder::TopDown,
    );

    static ref FILTER_WITH_NOW_TO_JOIN: OptimizationStage = OptimizationStage::new(
        "Push down filter with now into a left semijoin",
        vec![FilterWithNowToJoinRule::create()],
        ApplyOrder::TopDown,
    );

    static ref PUSH_CALC_OF_JOIN: OptimizationStage = OptimizationStage::new(
        "Push Down the Calculation of Inputs of Join's Condition",
        vec![PushCalculationOfJoinRule::create()],
        ApplyOrder::TopDown,
    );

    static ref CONVERT_DISTINCT_AGG_FOR_STREAM: OptimizationStage = OptimizationStage::new(
        "Convert Distinct Aggregation",
        vec![UnionToDistinctRule::create(), DistinctAggRule::create(true)],
        ApplyOrder::TopDown,
    );

    static ref CONVERT_DISTINCT_AGG_FOR_BATCH: OptimizationStage = OptimizationStage::new(
        "Convert Distinct Aggregation",
        vec![UnionToDistinctRule::create(), DistinctAggRule::create(false)],
        ApplyOrder::TopDown,
    );

    static ref JOIN_COMMUTE: OptimizationStage = OptimizationStage::new(
        "Join Commute".to_string(),
        vec![JoinCommuteRule::create()],
        ApplyOrder::TopDown,
    );

    static ref PROJECT_REMOVE: OptimizationStage = OptimizationStage::new(
        "Project Remove",
        vec![
            // merge should be applied before eliminate
            ProjectMergeRule::create(),
            ProjectEliminateRule::create(),
            TrivialProjectToValuesRule::create(),
            UnionInputValuesMergeRule::create(),
            JoinProjectTransposeRule::create(),
            // project-join merge should be applied after merge
            // eliminate and to values
            ProjectJoinMergeRule::create(),
            AggProjectMergeRule::create(),
        ],
        ApplyOrder::BottomUp,
    );

    static ref CONVERT_WINDOW_AGG: OptimizationStage = OptimizationStage::new(
        "Convert Window Aggregation",
        vec![
            OverAggToTopNRule::create(),
            ProjectMergeRule::create(),
            ProjectEliminateRule::create(),
            TrivialProjectToValuesRule::create(),
            UnionInputValuesMergeRule::create(),
        ],
        ApplyOrder::TopDown,
    );


    static ref DEDUP_GROUP_KEYS: OptimizationStage = OptimizationStage::new(
        "Dedup Group keys",
        vec![AggDedupGroupKeyRule::create()],
        ApplyOrder::TopDown,
    );

    static ref REWRITE_LIKE_EXPR: OptimizationStage = OptimizationStage::new(
        "Rewrite Like Expr",
        vec![RewriteLikeExprRule::create()],
        ApplyOrder::TopDown,
    );

    static ref TOP_N_AGG_ON_INDEX: OptimizationStage = OptimizationStage::new(
        "TopN/SimpleAgg on Index",
        vec![TopNOnIndexRule::create(),
             MinMaxOnIndexRule::create()],
        ApplyOrder::TopDown,
    );

    static ref ALWAYS_FALSE_FILTER: OptimizationStage = OptimizationStage::new(
        "Void always-false filter's downstream",
        vec![AlwaysFalseFilterRule::create()],
        ApplyOrder::TopDown,
    );

    static ref PULL_UP_HOP: OptimizationStage = OptimizationStage::new(
        "Pull up hop",
        vec![PullUpHopRule::create()],
        ApplyOrder::BottomUp,
    );
}

impl LogicalOptimizer {
    pub fn predicate_pushdown(
        plan: PlanRef,
        explain_trace: bool,
        ctx: &OptimizerContextRef,
    ) -> PlanRef {
        let plan = plan.predicate_pushdown(
            Condition::true_cond(),
            &mut PredicatePushdownContext::new(plan.clone()),
        );
        if explain_trace {
            ctx.trace("Predicate Push Down:");
            ctx.trace(plan.explain_to_string().unwrap());
        }
        plan
    }

    pub fn subquery_unnesting(
        mut plan: PlanRef,
        enable_share_plan: bool,
        explain_trace: bool,
        ctx: &OptimizerContextRef,
    ) -> Result<PlanRef> {
        // Simple Unnesting.
        plan = plan.optimize_by_rules(&SIMPLE_UNNESTING);
        if HasMaxOneRowApply().visit(plan.clone()) {
            return Err(ErrorCode::InternalError(
                "Scalar subquery might produce more than one row.".into(),
            )
            .into());
        }
        // Predicate push down before translate apply, because we need to calculate the domain
        // and predicate push down can reduce the size of domain.
        plan = Self::predicate_pushdown(plan, explain_trace, ctx);
        // General Unnesting.
        // Translate Apply, push Apply down the plan and finally replace Apply with regular inner
        // join.
        plan = if enable_share_plan {
            plan.optimize_by_rules(&GENERAL_UNNESTING_TRANS_APPLY_WITH_SHARE)
        } else {
            plan.optimize_by_rules(&GENERAL_UNNESTING_TRANS_APPLY_WITHOUT_SHARE)
        };
        plan = plan.optimize_by_rules_until_fix_point(&GENERAL_UNNESTING_PUSH_DOWN_APPLY);
        if has_logical_apply(plan.clone()) {
            return Err(ErrorCode::InternalError("Subquery can not be unnested.".into()).into());
        }
        Ok(plan)
    }

    pub fn column_pruning(
        mut plan: PlanRef,
        explain_trace: bool,
        ctx: &OptimizerContextRef,
    ) -> PlanRef {
        let required_cols = (0..plan.schema().len()).collect_vec();
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
        plan
    }

    pub fn gen_optimized_logical_plan_for_stream(mut plan: PlanRef) -> Result<PlanRef> {
        let ctx = plan.ctx();
        let explain_trace = ctx.is_explain_trace();

        if explain_trace {
            ctx.trace("Begin:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        // If share plan is disable, we need to remove all the share operator generated by the
        // binder, e.g. CTE and View. However, we still need to share source to ensure self
        // source join can return correct result.
        let enable_share_plan = ctx.session_ctx().config().get_enable_share_plan();
        if enable_share_plan {
            // Common sub-plan detection.
            plan = plan.merge_eq_nodes();
            plan = plan.prune_share();
            if explain_trace {
                ctx.trace("Merging equivalent nodes:");
                ctx.trace(plan.explain_to_string().unwrap());
            }
        } else {
            plan = plan.optimize_by_rules(&DAG_TO_TREE);

            // Replace source to share source.
            // Perform share source at the beginning so that we can benefit from predicate pushdown
            // and column pruning for the share operator.
            plan = ShareSourceRewriter::share_source(plan);
            if explain_trace {
                ctx.trace("Share Source:");
                ctx.trace(plan.explain_to_string().unwrap());
            }
        }

        plan = plan.optimize_by_rules(&UNION_MERGE);

        plan = Self::subquery_unnesting(plan, enable_share_plan, explain_trace, &ctx)?;

        // Predicate Push-down
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        // Merge inner joins and intermediate filters into multijoin
        // This rule assumes that filters have already been pushed down near to
        // their relevant joins.
        plan = plan.optimize_by_rules(&TO_MULTI_JOIN);

        // Reorder multijoin into join tree.
        if plan
            .ctx()
            .session_ctx()
            .config()
            .get_streaming_enable_bushy_join()
        {
            plan = plan.optimize_by_rules(&BUSHY_TREE_JOIN_REORDER);
        } else {
            plan = plan.optimize_by_rules(&LEFT_DEEP_JOIN_REORDER);
        }
        // Predicate Push-down: apply filter pushdown rules again since we pullup all join
        // conditions into a filter above the multijoin.
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        // For stream, push down predicates with now into a left-semi join
        plan = plan.optimize_by_rules(&FILTER_WITH_NOW_TO_JOIN);

        // Push down the calculation of inputs of join's condition.
        plan = plan.optimize_by_rules(&PUSH_CALC_OF_JOIN);

        // Prune Columns
        plan = Self::column_pruning(plan, explain_trace, &ctx);

        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        // Convert distinct aggregates.
        plan = plan.optimize_by_rules(&CONVERT_DISTINCT_AGG_FOR_STREAM);

        plan = plan.optimize_by_rules(&JOIN_COMMUTE);

        plan = plan.optimize_by_rules(&PROJECT_REMOVE);

        plan = plan.optimize_by_rules(&CONVERT_WINDOW_AGG);

        if has_logical_over_agg(plan.clone()) {
            return Err(ErrorCode::InternalError(format!(
                "OverAgg can not be transformed. Plan:\n{}",
                plan.explain_to_string().unwrap()
            ))
            .into());
        }

        plan = plan.optimize_by_rules(&DEDUP_GROUP_KEYS);

        #[cfg(debug_assertions)]
        InputRefValidator.validate(plan.clone());

        if ctx.is_explain_logical() {
            ctx.store_logical(plan.explain_to_string().unwrap());
        }

        Ok(plan)
    }

    pub fn gen_optimized_logical_plan_for_batch(mut plan: PlanRef) -> Result<PlanRef> {
        let ctx = plan.ctx();
        let explain_trace = ctx.is_explain_trace();

        if explain_trace {
            ctx.trace("Begin:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        // Convert the dag back to the tree, because we don't support DAG plan for batch.
        plan = plan.optimize_by_rules(&DAG_TO_TREE);

        plan = plan.optimize_by_rules(&REWRITE_LIKE_EXPR);
        plan = plan.optimize_by_rules(&UNION_MERGE);
        plan = plan.optimize_by_rules(&ALWAYS_FALSE_FILTER);

        plan = Self::subquery_unnesting(plan, false, explain_trace, &ctx)?;

        // Predicate Push-down
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        // Merge inner joins and intermediate filters into multijoin
        // This rule assumes that filters have already been pushed down near to
        // their relevant joins.
        plan = plan.optimize_by_rules(&TO_MULTI_JOIN);

        // Reorder multijoin into left-deep join tree.
        plan = plan.optimize_by_rules(&LEFT_DEEP_JOIN_REORDER);

        // Predicate Push-down: apply filter pushdown rules again since we pullup all join
        // conditions into a filter above the multijoin.
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        // Push down the calculation of inputs of join's condition.
        plan = plan.optimize_by_rules(&PUSH_CALC_OF_JOIN);

        // Prune Columns
        plan = Self::column_pruning(plan, explain_trace, &ctx);

        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        // Convert distinct aggregates.
        plan = plan.optimize_by_rules(&CONVERT_DISTINCT_AGG_FOR_BATCH);

        plan = plan.optimize_by_rules(&JOIN_COMMUTE);

        plan = plan.optimize_by_rules(&PROJECT_REMOVE);

        plan = plan.optimize_by_rules(&PULL_UP_HOP);

        plan = plan.optimize_by_rules(&CONVERT_WINDOW_AGG);

        if has_logical_over_agg(plan.clone()) {
            return Err(ErrorCode::InternalError(format!(
                "OverAgg can not be transformed. Plan:\n{}",
                plan.explain_to_string().unwrap()
            ))
            .into());
        }

        plan = plan.optimize_by_rules(&DEDUP_GROUP_KEYS);

        plan = plan.optimize_by_rules(&TOP_N_AGG_ON_INDEX);

        #[cfg(debug_assertions)]
        InputRefValidator.validate(plan.clone());

        if ctx.is_explain_logical() {
            ctx.store_logical(plan.explain_to_string().unwrap());
        }

        Ok(plan)
    }
}
