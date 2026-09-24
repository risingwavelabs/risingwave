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

use std::collections::HashSet;

use super::{DefaultBehavior, DefaultValue, LogicalPlanVisitor};
use crate::expr::{CorrelatedId, CorrelatedInputRef, ExprVisitor};
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalFilter, LogicalJoin, LogicalPlanRef as PlanRef, LogicalProject,
    LogicalProjectSet, LogicalTableFunction, LogicalValues, PlanTreeNode,
};
use crate::optimizer::plan_visitor::PlanVisitor;

#[derive(Default)]
pub struct PlanCorrelatedIdFinder {
    correlated_id_set: HashSet<CorrelatedId>,
}

impl PlanCorrelatedIdFinder {
    /// Return whether the finder observed the given correlated ID.
    pub fn contains(&self, correlated_id: &CorrelatedId) -> bool {
        self.correlated_id_set.contains(correlated_id)
    }

    /// Visit a logical plan and return whether it contains the given correlated ID.
    pub fn find_correlated_id(plan: PlanRef, correlated_id: &CorrelatedId) -> bool {
        let mut plan_correlated_id_finder = Self::default();
        plan_correlated_id_finder.visit(plan);
        plan_correlated_id_finder.contains(correlated_id)
    }
}

impl LogicalPlanVisitor for PlanCorrelatedIdFinder {
    /// `correlated_input_ref` can appear in expressions owned by logical plan nodes,
    /// including `LogicalValues` rows.
    type Result = ();

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        DefaultValue
    }

    fn visit_logical_join(&mut self, plan: &LogicalJoin) {
        let mut finder = ExprCorrelatedIdFinder::default();
        plan.on().visit_expr(&mut finder);
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }

    fn visit_logical_filter(&mut self, plan: &LogicalFilter) {
        let mut finder = ExprCorrelatedIdFinder::default();
        plan.predicate().visit_expr(&mut finder);
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }

    fn visit_logical_project(&mut self, plan: &LogicalProject) {
        let mut finder = ExprCorrelatedIdFinder::default();
        plan.exprs().iter().for_each(|expr| finder.visit_expr(expr));
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }

    fn visit_logical_agg(&mut self, plan: &LogicalAgg) {
        let mut finder = ExprCorrelatedIdFinder::default();
        plan.agg_calls()
            .iter()
            .for_each(|agg_call| agg_call.filter.visit_expr(&mut finder));
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }

    fn visit_logical_project_set(&mut self, plan: &LogicalProjectSet) {
        let mut finder = ExprCorrelatedIdFinder::default();
        plan.select_list()
            .iter()
            .for_each(|expr| finder.visit_expr(expr));
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }

    fn visit_logical_table_function(&mut self, plan: &LogicalTableFunction) {
        let mut finder = ExprCorrelatedIdFinder::default();
        plan.table_function()
            .args
            .iter()
            .for_each(|expr| finder.visit_expr(expr));
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }

    /// Inspect every expression in every row because correlated references can occur in any row.
    fn visit_logical_values(&mut self, plan: &LogicalValues) {
        let mut finder = ExprCorrelatedIdFinder::default();
        plan.rows()
            .iter()
            .flatten()
            .for_each(|expr| finder.visit_expr(expr));
        self.correlated_id_set.extend(finder.correlated_id_set);
    }
}

#[derive(Default)]
pub struct ExprCorrelatedIdFinder {
    correlated_id_set: HashSet<CorrelatedId>,
}

impl ExprCorrelatedIdFinder {
    pub fn contains(&self, correlated_id: &CorrelatedId) -> bool {
        self.correlated_id_set.contains(correlated_id)
    }

    pub fn has_correlated_input_ref(&self) -> bool {
        !self.correlated_id_set.is_empty()
    }
}

impl ExprVisitor for ExprCorrelatedIdFinder {
    fn visit_correlated_input_ref(&mut self, correlated_input_ref: &CorrelatedInputRef) {
        self.correlated_id_set
            .insert(correlated_input_ref.correlated_id());
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::optimizer::optimizer_context::OptimizerContext;

    #[test]
    fn test_find_correlated_id_in_logical_values() {
        let ctx = OptimizerContext::mock();
        let schema = Schema::new(vec![Field::with_name(DataType::Int32, "v")]);

        let mut correlated = super::CorrelatedInputRef::new(0, DataType::Int32, 1);
        correlated.set_correlated_id(42);

        let values = LogicalValues::new(
            vec![
                vec![1_i32.into()],
                vec![correlated.into()],
            ],
            schema,
            ctx,
        )
        .into();

        assert!(PlanCorrelatedIdFinder::find_correlated_id(values.clone(), &42));
        assert!(!PlanCorrelatedIdFinder::find_correlated_id(values.clone(), &43));

        let left = LogicalValues::new(
            vec![vec![2_i32.into()]],
            Schema::new(vec![Field::with_name(DataType::Int32, "left")]),
            ctx.clone(),
        )
        .into();
        let apply = LogicalApply::create(
            left,
            values,
            risingwave_pb::plan_common::JoinType::Inner,
            crate::utils::Condition::true_cond(),
            42,
            vec![0],
            false,
        );

        // The finder must keep ApplyEliminateRule from removing an Apply whose RHS
        // still contains a correlated reference. Attempting batch conversion should
        // therefore return the existing unsupported-LogicalApply error, rather than
        // reaching protobuf serialization with an unresolved CorrelatedInputRef.
        use crate::optimizer::rule::{ApplyEliminateRule, Rule};
        let rule = ApplyEliminateRule::create();
        let plan = rule.apply(apply.clone()).unwrap_or(apply);
        let err = plan.to_batch().expect_err("LogicalApply must not reach batch conversion");
        assert!(err.to_string().contains("LogicalApply should be unnested"));
    }
}
