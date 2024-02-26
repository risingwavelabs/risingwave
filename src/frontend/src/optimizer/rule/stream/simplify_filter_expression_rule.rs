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

use risingwave_common::types::DataType;
use risingwave_pb::plan_common::JoinType;

use crate::expr::{
    Expr, ExprImpl, ExprRewriter, FunctionCall, InputRef, WatermarkDerivation
};
use crate::expr::{ExprDisplay, ExprType, ExprVisitor, ImpureAnalyzer};
use crate::optimizer::plan_node::{ExprRewritable, LogicalFilter, LogicalJoin, LogicalNow, LogicalShare, PlanTreeNodeUnary};
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::optimizer::PlanRef;

pub struct SimplifyFilterExpressionRule {}
impl Rule for SimplifyFilterExpressionRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &LogicalFilter = plan.as_logical_filter()?;
        println!("filter: {:#?}", filter);
        let mut rewriter = SimplifyFilterExpressionRewriter {};
        let logical_share_plan = filter.input();
        let share: &LogicalShare = logical_share_plan.as_logical_share()?;
        let input = share.input().rewrite_exprs(&mut rewriter);
        let share = LogicalShare::new(input);
        Some(LogicalFilter::create(share.into(), filter.predicate().clone()))
    }
}

impl SimplifyFilterExpressionRule {
    pub fn create() -> BoxedRule {
        Box::new(SimplifyFilterExpressionRule {})
    }
}

/// If ever `Not (e)` and `(e)` appear together
fn check_pattern(e1: ExprImpl, e2: ExprImpl) -> bool {
    println!("e1: {:#?}", e1);
    println!("e2: {:#?}", e2);
    let ExprImpl::FunctionCall(e1_func) = e1.clone() else {
        return false;
    };
    let ExprImpl::FunctionCall(e2_func) = e2.clone() else {
        return false;
    };
    if e1_func.func_type() != ExprType::Not && e2_func.func_type() != ExprType::Not {
        return false;
    }
    if e1_func.func_type() != ExprType::Not {
        if e2_func.inputs().len() != 1 {
            println!("1");
            return false;
        }
            println!("2");
        e1 == e2_func.inputs()[0].clone()
    } else {
        if e1_func.inputs().len() != 1 {
            println!("3");
            return false;
        }
            println!("4");
        e2 == e1_func.inputs()[0].clone()
    }
}

struct SimplifyFilterExpressionRewriter {}

impl ExprRewriter for SimplifyFilterExpressionRewriter {
    /// The pattern we aim to optimize, e.g.,
    /// 1. (NOT (e)) OR (e) => True | (NOT (e)) AND (e) => False
    /// 2. (NOT (e1) AND NOT (e2)) OR (e1 OR e2) => True # TODO
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        println!("expr: {:#?}", expr);
        let ExprImpl::FunctionCall(func_call) = expr.clone() else {
            return expr;
        };
        if func_call.func_type() != ExprType::Or && func_call.func_type() != ExprType::And {
            return expr;
        }
        assert_eq!(func_call.return_type(), DataType::Boolean);
        // Currently just optimize the first rule
        if func_call.inputs().len() != 2 {
            return expr;
        }
        let inputs = func_call.inputs();
        if check_pattern(inputs[0].clone(), inputs[1].clone()) {
            match func_call.func_type() {
                ExprType::Or => ExprImpl::literal_bool(true),
                ExprType::And => ExprImpl::literal_bool(false),
                _ => expr,
            }
        } else {
            expr
        }
    }
}