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

use super::prelude::{PlanRef, *};
use crate::expr::{ExprImpl, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::*;

pub struct ExpandToProjectRule {}

impl ExpandToProjectRule {
    pub fn create() -> BoxedRule {
        Box::new(Self {})
    }
}
impl Rule<Logical> for ExpandToProjectRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let expand: &LogicalExpand = plan.as_logical_expand()?;
        let (input, column_subsets) = expand.clone().decompose();
        assert!(!column_subsets.is_empty());
        if column_subsets.len() > 1 {
            return None;
        }
        assert!(column_subsets.len() == 1);
        let column_subset = column_subsets.first().unwrap();

        // if `column_subsets` len equals 1, convert it into a project
        let mut exprs = Vec::with_capacity(expand.base.schema().len());
        // Add original input column first
        for i in 0..input.schema().len() {
            exprs.push(ExprImpl::InputRef(
                InputRef::new(i, input.schema()[i].data_type()).into(),
            ));
        }
        // Add column sets
        for i in 0..input.schema().len() {
            let expr = if column_subset.contains(&i) {
                ExprImpl::InputRef(InputRef::new(i, input.schema()[i].data_type()).into())
            } else {
                ExprImpl::literal_null(input.schema()[i].data_type())
            };
            exprs.push(expr);
        }
        // Add flag
        exprs.push(ExprImpl::literal_bigint(0));

        Some(LogicalProject::create(input, exprs))
    }
}
