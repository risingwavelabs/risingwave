// Copyright 2025 RisingWave Labs
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

use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::plan_expr_rewriter::CseRewriter;
use crate::optimizer::plan_expr_visitor::CseExprCounter;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::*;

pub struct CommonSubExprExtractRule {}
impl Rule<Logical> for CommonSubExprExtractRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let project: &LogicalProject = plan.as_logical_project()?;

        let mut expr_counter = CseExprCounter::default();
        for expr in project.exprs() {
            expr_counter.visit_expr(expr);
        }

        if expr_counter.counter.values().all(|counter| *counter <= 1) {
            return None;
        }

        let (exprs, input) = project.clone().decompose();
        let input_schema_len = input.schema().len();
        let mut cse_rewriter = CseRewriter::new(expr_counter, input_schema_len);
        let top_project_exprs = exprs
            .into_iter()
            .map(|expr| cse_rewriter.rewrite_expr(expr))
            .collect_vec();
        let bottom_project_exprs = {
            let mut exprs = Vec::with_capacity(input_schema_len + cse_rewriter.cse_mapping.len());
            for (i, field) in input.schema().fields.iter().enumerate() {
                let expr = ExprImpl::InputRef(InputRef::new(i, field.data_type.clone()).into());
                exprs.push(expr);
            }
            exprs.extend(
                cse_rewriter
                    .cse_mapping
                    .into_iter()
                    .sorted_by(|(_, v1), (_, v2)| Ord::cmp(&v1.index, &v2.index))
                    .map(|(k, _)| ExprImpl::FunctionCall(k.into())),
            );
            exprs
        };
        let bottom_project = LogicalProject::new(input, bottom_project_exprs);
        let top_project = LogicalProject::new(bottom_project.into(), top_project_exprs);
        Some(top_project.into())
    }
}

impl CommonSubExprExtractRule {
    pub fn create() -> BoxedRule {
        Box::new(CommonSubExprExtractRule {})
    }
}
