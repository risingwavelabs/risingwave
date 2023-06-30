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
use risingwave_common::error::Result;

use crate::binder::{BoundSetExpr, BoundSetOperation};
use crate::optimizer::plan_node::{LogicalExcept, LogicalIntersect, LogicalUnion};
use crate::planner::Planner;
use crate::PlanRef;

impl Planner {
    pub(super) fn plan_set_operation(
        &mut self,
        op: BoundSetOperation,
        all: bool,
        left: BoundSetExpr,
        right: BoundSetExpr,
    ) -> Result<PlanRef> {
        match op {
            BoundSetOperation::Union => {
                let left = self.plan_set_expr(left, vec![], &[])?;
                let right = self.plan_set_expr(right, vec![], &[])?;
                Ok(LogicalUnion::create(all, vec![left, right]))
            }
            BoundSetOperation::Intersect => {
                let left = self.plan_set_expr(left, vec![], &[])?;
                let right = self.plan_set_expr(right, vec![], &[])?;
                Ok(LogicalIntersect::create(all, vec![left, right]))
            }
            BoundSetOperation::Except => {
                let left = self.plan_set_expr(left, vec![], &[])?;
                let right = self.plan_set_expr(right, vec![], &[])?;
                Ok(LogicalExcept::create(all, vec![left, right]))
            }
        }
    }
}
