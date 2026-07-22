// Copyright 2022 RisingWave Labs
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

use super::*;
use crate::optimizer::plan_visitor::ExprCorrelatedIdFinder;
use crate::optimizer::{ExpressionSimplifyRewriter, LogicalPlanRef as PlanRef};

/// The trait for predicate pushdown, only logical plan node will use it, though all plan node impl
/// it.
pub trait PredicatePushdown {
    /// Push predicate down for every logical plan node.
    ///
    /// There are three kinds of predicates:
    ///
    /// 1. those can't be pushed down. We just create a `LogicalFilter` for them above the current
    ///    `PlanNode`. i.e.,
    ///
    ///     ```ignore
    ///     LogicalFilter::create(self.clone().into(), predicate)
    ///     ```
    ///
    /// 2. those can be merged with current `PlanNode` (e.g., `LogicalJoin`). We just merge
    ///    the predicates with the `Condition` of it.
    ///
    /// 3. those can be pushed down. We pass them to current `PlanNode`'s input.
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef;
}

#[inline]
pub fn gen_filter_and_pushdown<T: PlanTreeNodeUnary<Logical> + LogicalPlanNode>(
    node: &T,
    filter_predicate: Condition,
    pushed_predicate: Condition,
    ctx: &mut PredicatePushdownContext,
) -> PlanRef {
    let new_input = node.input().predicate_pushdown(pushed_predicate, ctx);
    let new_node = node.clone_with_input(new_input);
    LogicalFilter::create(new_node.into(), filter_predicate)
}

#[derive(Clone, Debug)]
struct SharePredicate(Condition);

impl ShareRequirement for SharePredicate {
    fn merge(requirements: Vec<Self>) -> Self {
        let merged = requirements
            .into_iter()
            .map(|SharePredicate(mut condition)| Condition {
                conjunctions: condition
                    .conjunctions
                    .extract_if(.., |expr| {
                        // Temporal, impure and correlated predicates must remain above each
                        // parent and cannot participate in an OR below a share.
                        let mut finder = ExprCorrelatedIdFinder::default();
                        finder.visit_expr(expr);
                        expr.count_nows() == 0
                            && expr.is_pure()
                            && !finder.has_correlated_input_ref()
                    })
                    .collect(),
            })
            .reduce(Condition::or)
            .expect("a shared plan must have at least one parent predicate");

        let mut rewriter = ExpressionSimplifyRewriter {};
        let simplified = merged
            .conjunctions
            .into_iter()
            .fold(Condition::true_cond(), |condition, expr| {
                condition.and(Condition::with_expr(rewriter.rewrite_cond(expr)))
            });
        Self(simplified)
    }
}

#[derive(Debug, Clone)]
pub struct PredicatePushdownContext {
    dag: ShareDagContext<SharePredicate>,
}

impl PredicatePushdownContext {
    pub fn new(root: PlanRef) -> Self {
        Self {
            dag: ShareDagContext::new(root),
        }
    }

    pub(in crate::optimizer) fn is_running(&self) -> bool {
        self.dag.is_running()
    }

    pub(in crate::optimizer) fn phase(&self) -> ShareDagPhase {
        self.dag.phase()
    }

    pub(in crate::optimizer) fn get_parent_num(&self, share: &LogicalShare) -> usize {
        self.dag.parent_num(share)
    }

    pub(in crate::optimizer) fn add_predicate(
        &mut self,
        share: &LogicalShare,
        predicate: Condition,
    ) -> Option<Condition> {
        self.dag
            .record_requirement(share, SharePredicate(predicate))
            .map(|predicate| predicate.0)
    }

    pub(in crate::optimizer) fn run(&mut self, root: PlanRef, predicate: Condition) -> PlanRef {
        self.dag.reset(root.clone());
        let optimizer_ctx = root.ctx();
        let transaction = LogicalShareTableTransaction::new(optimizer_ctx.clone());

        self.dag.set_phase(ShareDagPhase::Collect);
        let collected = root.predicate_pushdown_inner(predicate.clone(), self);
        let rebuild_order = self.dag.rebuild_order();
        if rebuild_order.is_empty() {
            self.dag.finish();
            transaction.commit();
            return collected;
        }

        self.dag.set_phase(ShareDagPhase::Rebuild);
        for share_id in rebuild_order {
            let original_input = self.dag.original_input(share_id);
            let merged_predicate = self.dag.merged_requirement(share_id).0;
            let rebuilt_input = original_input.predicate_pushdown(merged_predicate, self);
            let current_version = optimizer_ctx.current_logical_share_version(share_id);
            optimizer_ctx.update_logical_share(share_id, current_version, rebuilt_input);
        }

        self.dag.set_phase(ShareDagPhase::Adapt);
        let result = root.predicate_pushdown(predicate, self);
        self.dag.finish();
        transaction.commit();
        result
    }
}
