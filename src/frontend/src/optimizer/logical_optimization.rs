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
use risingwave_common::error::{ErrorCode, Result};

use super::plan_node::RewriteExprsRecursive;
use crate::expr::InlineNowProcTime;
use crate::optimizer::heuristic_optimizer::{ApplyOrder, HeuristicOptimizer};
use crate::optimizer::plan_node::{ColumnPruningContext, PredicatePushdownContext};
use crate::optimizer::plan_rewriter::ShareSourceRewriter;
#[cfg(debug_assertions)]
use crate::optimizer::plan_visitor::InputRefValidator;
use crate::optimizer::plan_visitor::{has_logical_apply, HasMaxOneRowApply, PlanVisitor};
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
            ctx.trace(plan.explain_to_string());
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
                ctx.trace(output_plan.explain_to_string());
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

use std::sync::LazyLock;

pub struct LogicalOptimizer {}

static DAG_TO_TREE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "DAG To Tree",
        vec![DagToTreeRule::create()],
        ApplyOrder::TopDown,
    )
});

static TABLE_FUNCTION_TO_PROJECT_SET: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Table Function To Project Set",
        vec![TableFunctionToProjectSetRule::create()],
        ApplyOrder::TopDown,
    )
});

static VALUES_EXTRACT_PROJECT: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Values Extract Project",
        vec![ValuesExtractProjectRule::create()],
        ApplyOrder::TopDown,
    )
});

static SIMPLE_UNNESTING: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Simple Unnesting",
        vec![
            // Eliminate max one row
            MaxOneRowEliminateRule::create(),
            // Convert apply to join.
            ApplyToJoinRule::create(),
            // Pull correlated predicates up the algebra tree to unnest simple subquery.
            PullUpCorrelatedPredicateRule::create(),
        ],
        ApplyOrder::BottomUp,
    )
});

static SET_OPERATION_MERGE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Set Operation Merge",
        vec![
            UnionMergeRule::create(),
            IntersectMergeRule::create(),
            ExceptMergeRule::create(),
        ],
        ApplyOrder::BottomUp,
    )
});

static GENERAL_UNNESTING_TRANS_APPLY_WITH_SHARE: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "General Unnesting(Translate Apply)",
            vec![
                TranslateApplyRule::create(true),
                // Separate the project from a join if necessary because `ApplyJoinTransposeRule`
                // can't handle a join with `output_indices`.
                ProjectJoinSeparateRule::create(),
            ],
            ApplyOrder::BottomUp,
        )
    });

static GENERAL_UNNESTING_TRANS_APPLY_WITHOUT_SHARE: LazyLock<OptimizationStage> =
    LazyLock::new(|| {
        OptimizationStage::new(
            "General Unnesting(Translate Apply)",
            vec![
                TranslateApplyRule::create(false),
                // Separate the project from a join if necessary because `ApplyJoinTransposeRule`
                // can't handle a join with `output_indices`.
                ProjectJoinSeparateRule::create(),
            ],
            ApplyOrder::BottomUp,
        )
    });

static GENERAL_UNNESTING_PUSH_DOWN_APPLY: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "General Unnesting(Push Down Apply)",
        vec![
            ApplyEliminateRule::create(),
            ApplyAggTransposeRule::create(),
            ApplyDedupTransposeRule::create(),
            ApplyFilterTransposeRule::create(),
            ApplyProjectTransposeRule::create(),
            ApplyProjectSetTransposeRule::create(),
            ApplyTopNTransposeRule::create(),
            ApplyLimitTransposeRule::create(),
            ApplyJoinTransposeRule::create(),
            ApplyUnionTransposeRule::create(),
            ApplyOverWindowTransposeRule::create(),
            ApplyExpandTransposeRule::create(),
            ApplyHopWindowTransposeRule::create(),
            CrossJoinEliminateRule::create(),
            ApplyShareEliminateRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static TO_MULTI_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "To MultiJoin",
        vec![MergeMultiJoinRule::create()],
        ApplyOrder::TopDown,
    )
});

static LEFT_DEEP_JOIN_ORDERING: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Join Ordering".to_string(),
        vec![LeftDeepTreeJoinOrderingRule::create()],
        ApplyOrder::TopDown,
    )
});

static BUSHY_TREE_JOIN_ORDERING: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Join Ordering".to_string(),
        vec![BushyTreeJoinOrderingRule::create()],
        ApplyOrder::TopDown,
    )
});

static FILTER_WITH_NOW_TO_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Push down filter with now into a left semijoin",
        vec![FilterWithNowToJoinRule::create()],
        ApplyOrder::TopDown,
    )
});

static PUSH_CALC_OF_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Push down the calculation of inputs of join's condition",
        vec![PushCalculationOfJoinRule::create()],
        ApplyOrder::TopDown,
    )
});

static CONVERT_DISTINCT_AGG_FOR_STREAM: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Convert Distinct Aggregation",
        vec![UnionToDistinctRule::create(), DistinctAggRule::create(true)],
        ApplyOrder::TopDown,
    )
});

static CONVERT_DISTINCT_AGG_FOR_BATCH: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Convert Distinct Aggregation",
        vec![
            UnionToDistinctRule::create(),
            DistinctAggRule::create(false),
        ],
        ApplyOrder::TopDown,
    )
});

static SIMPLIFY_AGG: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Simplify Aggregation",
        vec![AggGroupBySimplifyRule::create(), AggCallMergeRule::create()],
        ApplyOrder::TopDown,
    )
});

static JOIN_COMMUTE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Join Commute".to_string(),
        vec![JoinCommuteRule::create()],
        ApplyOrder::TopDown,
    )
});

static PROJECT_REMOVE: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
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
    )
});

static SPLIT_OVER_WINDOW: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Split Over Window",
        vec![OverWindowSplitRule::create()],
        ApplyOrder::TopDown,
    )
});

// the `OverWindowToTopNRule` need to match the pattern of Proj-Filter-OverWindow so it is
// 1. conflict with `ProjectJoinMergeRule`, `AggProjectMergeRule` or other rules
// 2. should be after merge the multiple projects
static CONVERT_OVER_WINDOW: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Convert Over Window",
        vec![
            ProjectMergeRule::create(),
            ProjectEliminateRule::create(),
            TrivialProjectToValuesRule::create(),
            UnionInputValuesMergeRule::create(),
            OverWindowToAggAndJoinRule::create(),
            OverWindowToTopNRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static REWRITE_LIKE_EXPR: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Rewrite Like Expr",
        vec![RewriteLikeExprRule::create()],
        ApplyOrder::TopDown,
    )
});

static TOP_N_AGG_ON_INDEX: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "TopN/SimpleAgg on Index",
        vec![TopNOnIndexRule::create(), MinMaxOnIndexRule::create()],
        ApplyOrder::TopDown,
    )
});

static ALWAYS_FALSE_FILTER: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Void always-false filter's downstream",
        vec![AlwaysFalseFilterRule::create()],
        ApplyOrder::TopDown,
    )
});

static LIMIT_PUSH_DOWN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Push Down Limit",
        vec![LimitPushDownRule::create()],
        ApplyOrder::TopDown,
    )
});

static PULL_UP_HOP: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Pull Up Hop",
        vec![PullUpHopRule::create()],
        ApplyOrder::BottomUp,
    )
});

static SET_OPERATION_TO_JOIN: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Set Operation To Join",
        vec![
            IntersectToSemiJoinRule::create(),
            ExceptToAntiJoinRule::create(),
        ],
        ApplyOrder::BottomUp,
    )
});

static GROUPING_SETS: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Grouping Sets",
        vec![
            GroupingSetsToExpandRule::create(),
            ExpandToProjectRule::create(),
        ],
        ApplyOrder::TopDown,
    )
});

static COMMON_SUB_EXPR_EXTRACT: LazyLock<OptimizationStage> = LazyLock::new(|| {
    OptimizationStage::new(
        "Common Sub Expression Extract",
        vec![CommonSubExprExtractRule::create()],
        ApplyOrder::TopDown,
    )
});

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
            ctx.trace(plan.explain_to_string());
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
        // In order to unnest a table function, we need to convert it into a `project_set` first.
        plan = plan.optimize_by_rules(&TABLE_FUNCTION_TO_PROJECT_SET);
        // In order to unnest values with correlated input ref, we need to extract project first.
        plan = plan.optimize_by_rules(&VALUES_EXTRACT_PROJECT);
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
            ctx.trace(plan.explain_to_string());
        }

        if column_pruning_ctx.need_second_round() {
            // Second round of column pruning and reuse the column pruning context.
            // Try to replace original share operator with the new one.
            plan = plan.prune_col(&required_cols, &mut column_pruning_ctx);
            if explain_trace {
                ctx.trace("Prune Columns (For DAG):");
                ctx.trace(plan.explain_to_string());
            }
        }
        plan
    }

    pub fn inline_now_proc_time(plan: PlanRef, ctx: &OptimizerContextRef) -> PlanRef {
        // TODO: if there's no `NOW()` or `PROCTIME()`, we don't need to acquire snapshot.
        let epoch = ctx.session_ctx().pinned_snapshot().epoch();

        let plan = plan.rewrite_exprs_recursive(&mut InlineNowProcTime::new(epoch));

        if ctx.is_explain_trace() {
            ctx.trace("Inline Now and ProcTime:");
            ctx.trace(plan.explain_to_string());
        }
        plan
    }

    pub fn gen_optimized_logical_plan_for_stream(mut plan: PlanRef) -> Result<PlanRef> {
        let ctx = plan.ctx();
        let explain_trace = ctx.is_explain_trace();

        if explain_trace {
            ctx.trace("Begin:");
            ctx.trace(plan.explain_to_string());
        }

        // Convert grouping sets at first because other agg rule can't handle grouping sets.
        plan = plan.optimize_by_rules(&GROUPING_SETS);
        // Remove project to make common sub-plan sharing easier.
        plan = plan.optimize_by_rules(&PROJECT_REMOVE);

        // If share plan is disable, we need to remove all the share operator generated by the
        // binder, e.g. CTE and View. However, we still need to share source to ensure self
        // source join can return correct result.
        let enable_share_plan = ctx.session_ctx().config().get_enable_share_plan();
        if enable_share_plan {
            // Common sub-plan sharing.
            plan = plan.common_subplan_sharing();
            plan = plan.prune_share();
            if explain_trace {
                ctx.trace("Common Sub-plan Sharing:");
                ctx.trace(plan.explain_to_string());
            }
        } else {
            plan = plan.optimize_by_rules(&DAG_TO_TREE);

            // Replace source to share source.
            // Perform share source at the beginning so that we can benefit from predicate pushdown
            // and column pruning for the share operator.
            plan = ShareSourceRewriter::share_source(plan);
            if explain_trace {
                ctx.trace("Share Source:");
                ctx.trace(plan.explain_to_string());
            }
        }
        plan = plan.optimize_by_rules(&SET_OPERATION_MERGE);
        plan = plan.optimize_by_rules(&SET_OPERATION_TO_JOIN);

        plan = Self::subquery_unnesting(plan, enable_share_plan, explain_trace, &ctx)?;

        // Predicate Push-down
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        if plan.ctx().session_ctx().config().get_enable_join_ordering() {
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
                plan = plan.optimize_by_rules(&BUSHY_TREE_JOIN_ORDERING);
            } else {
                plan = plan.optimize_by_rules(&LEFT_DEEP_JOIN_ORDERING);
            }
        }

        // Predicate Push-down: apply filter pushdown rules again since we pullup all join
        // conditions into a filter above the multijoin.
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        // For stream, push down predicates with now into a left-semi join
        plan = plan.optimize_by_rules(&FILTER_WITH_NOW_TO_JOIN);

        // Push down the calculation of inputs of join's condition.
        plan = plan.optimize_by_rules(&PUSH_CALC_OF_JOIN);

        plan = plan.optimize_by_rules(&SPLIT_OVER_WINDOW);
        // Must push down predicates again after split over window so that OverWindow can be
        // optimized to TopN.
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
        plan = plan.optimize_by_rules(&CONVERT_OVER_WINDOW);

        let force_split_distinct_agg = ctx.session_ctx().config().get_force_split_distinct_agg();
        // TODO: better naming of the OptimizationStage
        // Convert distinct aggregates.
        plan = if force_split_distinct_agg {
            plan.optimize_by_rules(&CONVERT_DISTINCT_AGG_FOR_BATCH)
        } else {
            plan.optimize_by_rules(&CONVERT_DISTINCT_AGG_FOR_STREAM)
        };

        plan = plan.optimize_by_rules(&SIMPLIFY_AGG);

        plan = plan.optimize_by_rules(&JOIN_COMMUTE);

        // Do a final column pruning and predicate pushing down to clean up the plan.
        plan = Self::column_pruning(plan, explain_trace, &ctx);
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        plan = plan.optimize_by_rules(&PROJECT_REMOVE);

        plan = plan.optimize_by_rules(&COMMON_SUB_EXPR_EXTRACT);

        #[cfg(debug_assertions)]
        InputRefValidator.validate(plan.clone());

        if ctx.is_explain_logical() {
            ctx.store_logical(plan.explain_to_string());
        }

        Ok(plan)
    }

    pub fn gen_optimized_logical_plan_for_batch(mut plan: PlanRef) -> Result<PlanRef> {
        let ctx = plan.ctx();
        let explain_trace = ctx.is_explain_trace();

        if explain_trace {
            ctx.trace("Begin:");
            ctx.trace(plan.explain_to_string());
        }

        // Inline `NOW()` and `PROCTIME()`, only for batch queries.
        plan = Self::inline_now_proc_time(plan, &ctx);

        // Convert the dag back to the tree, because we don't support DAG plan for batch.
        plan = plan.optimize_by_rules(&DAG_TO_TREE);

        plan = plan.optimize_by_rules(&GROUPING_SETS);
        plan = plan.optimize_by_rules(&REWRITE_LIKE_EXPR);
        plan = plan.optimize_by_rules(&SET_OPERATION_MERGE);
        plan = plan.optimize_by_rules(&SET_OPERATION_TO_JOIN);
        plan = plan.optimize_by_rules(&ALWAYS_FALSE_FILTER);

        plan = Self::subquery_unnesting(plan, false, explain_trace, &ctx)?;

        // Predicate Push-down
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        if plan.ctx().session_ctx().config().get_enable_join_ordering() {
            // Merge inner joins and intermediate filters into multijoin
            // This rule assumes that filters have already been pushed down near to
            // their relevant joins.
            plan = plan.optimize_by_rules(&TO_MULTI_JOIN);

            // Reorder multijoin into left-deep join tree.
            plan = plan.optimize_by_rules(&LEFT_DEEP_JOIN_ORDERING);
        }

        // Predicate Push-down: apply filter pushdown rules again since we pullup all join
        // conditions into a filter above the multijoin.
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        // Push down the calculation of inputs of join's condition.
        plan = plan.optimize_by_rules(&PUSH_CALC_OF_JOIN);

        plan = plan.optimize_by_rules(&SPLIT_OVER_WINDOW);
        // Must push down predicates again after split over window so that OverWindow can be
        // optimized to TopN.
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);
        plan = plan.optimize_by_rules(&CONVERT_OVER_WINDOW);

        // Convert distinct aggregates.
        plan = plan.optimize_by_rules(&CONVERT_DISTINCT_AGG_FOR_BATCH);

        plan = plan.optimize_by_rules(&SIMPLIFY_AGG);

        plan = plan.optimize_by_rules(&JOIN_COMMUTE);

        // Do a final column pruning and predicate pushing down to clean up the plan.
        plan = Self::column_pruning(plan, explain_trace, &ctx);
        plan = Self::predicate_pushdown(plan, explain_trace, &ctx);

        plan = plan.optimize_by_rules(&PROJECT_REMOVE);

        plan = plan.optimize_by_rules(&COMMON_SUB_EXPR_EXTRACT);

        plan = plan.optimize_by_rules(&PULL_UP_HOP);

        plan = plan.optimize_by_rules(&TOP_N_AGG_ON_INDEX);

        plan = plan.optimize_by_rules(&LIMIT_PUSH_DOWN);

        plan = plan.optimize_by_rules(&DAG_TO_TREE);

        #[cfg(debug_assertions)]
        InputRefValidator.validate(plan.clone());

        if ctx.is_explain_logical() {
            ctx.store_logical(plan.explain_to_string());
        }

        Ok(plan)
    }
}
