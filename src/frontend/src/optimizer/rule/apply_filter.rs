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

        // For correlated_input_ref, we should NOT change its index, because it actually points to
        // the left child of LogicalApply,
        // while for uncorrelated_input_ref, we should shift it with the offset.
        // Therefore, we can firstly shift the input_ref's index, and then
        // lift correlated_input_ref with depth equal to 1 to input_ref keeping its index unchanged.
        let mut lift_correlated_input_ref = LiftCorrelatedInputRef {};
        let mut shift_input_ref = ColIndexMapping::with_shift_offset(
            filter.schema().len(),
            apply.left().schema().len() as isize,
        );
        let predicate = filter
            .predicate()
            .clone()
            .rewrite_expr(&mut shift_input_ref)
            .rewrite_expr(&mut lift_correlated_input_ref);

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
