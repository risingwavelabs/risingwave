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

use std::collections::HashSet;

use crate::expr::{ExprImpl, ExprType};
use crate::optimizer::plan_node::LogicalScan;
use crate::optimizer::{BoxedRule, ExpressionSimplifyRewriter, PlanRef};
use crate::optimizer::rule::Rule;
use crate::utils::Condition;

pub struct BatchFilterExpressionSimplifyRule {}
impl Rule for BatchFilterExpressionSimplifyRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let scan: &LogicalScan = plan.as_logical_scan()?;
        let mut rewriter = ExpressionSimplifyRewriter {};
        let predicate = scan.predicate().clone().rewrite_expr(&mut rewriter);
        let predicate = ConditionRewriter::rewrite_condition(predicate);
        Some(scan.clone_with_predicate(predicate).into())
    }
}

impl BatchFilterExpressionSimplifyRule {
    pub fn create() -> BoxedRule {
        Box::new(BatchFilterExpressionSimplifyRule {})
    }
}

pub struct ConditionRewriter {}
impl ConditionRewriter {
    pub fn rewrite_condition(condition: Condition) -> Condition {
        let conjuctions = condition.conjunctions.clone();
        let mut set = HashSet::new();
        for expr in conjuctions {
            let e;
            if let ExprImpl::FunctionCall(func_call) = expr.clone() {
                // Not(e)
                if func_call.func_type() == ExprType::Not && func_call.inputs().len() == 1 {
                    e = func_call.inputs()[0].clone();
                } else {
                    e = expr;
                }
            } else {
                e = expr;
            }
            if set.contains(&e) {
                return Condition::false_cond();
            } else {
                set.insert(e);
            }
        }
        condition
    }
}