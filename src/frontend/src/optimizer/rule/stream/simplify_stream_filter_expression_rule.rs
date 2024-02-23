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
    try_derive_watermark, ExprRewriter, FunctionCall, InputRef, WatermarkDerivation,
};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalFilter, LogicalJoin, LogicalNow};
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::optimizer::PlanRef;
use crate::utils::Condition;

pub struct SimplifyStreamFilterExpressionRule {}
impl Rule for SimplifyStreamFilterExpressionRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &LogicalFilter = plan.as_logical_filter()?;

        println!("predicate: {:#?}", filter.predicate());
        todo!();
    }
}

impl SimplifyStreamFilterExpressionRule {
    pub fn create() -> BoxedRule {
        Box::new(SimplifyStreamFilterExpressionRule {})
    }
}