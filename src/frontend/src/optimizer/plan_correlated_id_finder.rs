// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;

use crate::expr::{CorrelatedId, CorrelatedInputRef, ExprVisitor};
use crate::optimizer::plan_node::{LogicalFilter, LogicalJoin, LogicalProject, PlanTreeNode};
use crate::optimizer::plan_visitor::PlanVisitor;

pub struct PlanCorrelatedIdFinder {
    correlated_id_set: HashSet<CorrelatedId>,
}

impl PlanCorrelatedIdFinder {
    pub fn new() -> Self {
        Self {
            correlated_id_set: HashSet::new(),
        }
    }

    pub fn contains(&self, correlated_id: &CorrelatedId) -> bool {
        self.correlated_id_set.contains(correlated_id)
    }
}

impl PlanVisitor<()> for PlanCorrelatedIdFinder {
    /// `correlated_input_ref` can only appear in `LogicalProject`, `LogicalFilter` and
    /// `LogicalJoin` now.

    fn visit_logical_join(&mut self, plan: &LogicalJoin) {
        let mut finder = ExprCorrelatedIdFinder::new();
        plan.on()
            .conjunctions
            .iter()
            .for_each(|expr| finder.visit_expr(expr));
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }

    fn visit_logical_filter(&mut self, plan: &LogicalFilter) {
        let mut finder = ExprCorrelatedIdFinder::new();
        plan.predicate()
            .conjunctions
            .iter()
            .for_each(|expr| finder.visit_expr(expr));
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }

    fn visit_logical_project(&mut self, plan: &LogicalProject) {
        let mut finder = ExprCorrelatedIdFinder::new();
        plan.exprs().iter().for_each(|expr| finder.visit_expr(expr));
        self.correlated_id_set.extend(finder.correlated_id_set);

        plan.inputs()
            .into_iter()
            .for_each(|input| self.visit(input));
    }
}

struct ExprCorrelatedIdFinder {
    correlated_id_set: HashSet<CorrelatedId>,
}

impl ExprCorrelatedIdFinder {
    pub fn new() -> Self {
        Self {
            correlated_id_set: HashSet::new(),
        }
    }
}

impl ExprVisitor for ExprCorrelatedIdFinder {
    fn visit_correlated_input_ref(&mut self, correlated_input_ref: &CorrelatedInputRef) {
        self.correlated_id_set
            .insert(correlated_input_ref.correlated_id());
    }
}
