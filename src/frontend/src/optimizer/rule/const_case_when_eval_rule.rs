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

use super::super::plan_node::*;
use super::{BoxedRule, Rule};
use crate::optimizer::plan_expr_rewriter::ConstCaseWhenRewriter;

pub struct ConstCaseWhenEvalRule {}
impl Rule for ConstCaseWhenEvalRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let values: &LogicalValues = plan.as_logical_values()?;
        let mut const_case_when_rewriter = ConstCaseWhenRewriter { error: None };
        Some(values.rewrite_exprs(&mut const_case_when_rewriter))
    }
}

impl ConstCaseWhenEvalRule {
    pub fn create() -> BoxedRule {
        Box::new(ConstCaseWhenEvalRule {})
    }
}