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

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalValues, PlanTreeNodeUnary};
use crate::optimizer::plan_visitor::CountRows;
use crate::optimizer::{PlanRef, PlanVisitor};

pub struct TrivialProjectToValuesRule {}
impl Rule for TrivialProjectToValuesRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let project = plan.as_logical_project()?;
        if project.exprs().iter().all(|e| e.is_const()) {
            let mut count_rows = CountRows;
            count_rows.visit(project.input()).map(|count| {
                LogicalValues::new(
                    vec![project.exprs().clone(); count],
                    project.schema().clone(),
                    project.ctx(),
                )
                .into()
            })
        } else {
            None
        }
    }
}

impl TrivialProjectToValuesRule {
    pub fn create() -> BoxedRule {
        Box::new(TrivialProjectToValuesRule {})
    }
}
