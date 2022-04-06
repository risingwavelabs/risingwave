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

use super::super::plan_node::*;
use super::{BoxedRule, LiftCorrelatedInputRef, Rule};
use crate::utils::ColIndexMapping;

pub struct ApplyFilterRule {}
impl Rule for ApplyFilterRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let right = apply.right();
        let filter = right.as_logical_filter()?;
        let new_right = filter.input();

        println!("ApplyFilterRule is used");

        let mut lift_correlated_input_ref = LiftCorrelatedInputRef {};
        let mut shift_input_ref = ColIndexMapping::with_shift_offset(
            filter.schema().len(),
            apply.left().schema().len() as isize,
        );
        let predicate = filter
            .predicate()
            .clone()
            .rewrite_expr(&mut lift_correlated_input_ref)
            .rewrite_expr(&mut shift_input_ref);

        let new_apply = apply.clone_with_left_right(apply.left(), new_right);
        let lifted_filter: PlanRef = LogicalFilter::new(new_apply.into(), predicate).into();
        Some(lifted_filter)
    }
}

impl ApplyFilterRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyFilterRule {})
    }
}
