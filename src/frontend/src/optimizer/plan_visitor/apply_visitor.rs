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

use risingwave_common::error::{ErrorCode, RwError};

use super::{DefaultBehavior, Merge};
use crate::optimizer::plan_node::{LogicalApply, PlanTreeNodeBinary};
use crate::optimizer::plan_visitor::PlanVisitor;
use crate::PlanRef;

pub struct HasMaxOneRowApply();

impl PlanVisitor for HasMaxOneRowApply {
    type Result = bool;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|a, b| a | b)
    }

    fn visit_logical_apply(&mut self, plan: &LogicalApply) -> bool {
        plan.max_one_row() | self.visit(plan.left()) | self.visit(plan.right())
    }
}

pub struct CheckApplyElimination {
    result: Result<(), &'static str>,
}

impl PlanVisitor for CheckApplyElimination {
    type Result = ();

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        Merge(|_, _| ())
    }

    fn visit_logical_apply(&mut self, plan: &LogicalApply) {
        if self.result.is_err() {
            return;
        }

        // If there's a runtime max-one-row check on the right side, it's likely to be the
        // reason for the failed unnesting. Report users with a more precise error message.
        if let Some(limit) = plan.right().as_logical_limit()
          && limit.check_exceeding && limit.offset() == 0 && limit.limit() == 1 {
            self.result = Err("Scalar subquery might produce more than one row.");
        } else {
            self.result = Err("Subquery can not be unnested.");
        }
    }
}

#[easy_ext::ext(PlanCheckApplyEliminationExt)]
impl PlanRef {
    /// Checks if all `LogicalApply` nodes in the plan have been eliminated, that is,
    /// subqueries are successfully unnested.
    pub fn check_apply_elimination(&self) -> Result<(), RwError> {
        let mut visitor = CheckApplyElimination { result: Ok(()) };
        visitor.visit(self.clone());
        visitor
            .result
            .map_err(|e| ErrorCode::InternalError(e.to_owned()).into())
    }
}
