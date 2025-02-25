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

use risingwave_common::util::column_index_mapping::ColIndexMapping;

use crate::PlanRef;
use crate::binder::{BoundSetExpr, BoundSetOperation};
use crate::error::Result;
use crate::optimizer::plan_node::{LogicalExcept, LogicalIntersect, LogicalProject, LogicalUnion};
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_set_operation(
        &mut self,
        op: BoundSetOperation,
        all: bool,
        corresponding_col_indices: Option<(ColIndexMapping, ColIndexMapping)>,
        left: BoundSetExpr,
        right: BoundSetExpr,
    ) -> Result<PlanRef> {
        let left = self.plan_set_expr(left, vec![], &[])?;
        let right = self.plan_set_expr(right, vec![], &[])?;

        // Map the corresponding columns
        let (left, right) = if let Some((mapping_l, mapping_r)) = corresponding_col_indices {
            (
                LogicalProject::with_mapping(left, mapping_l).into(),
                LogicalProject::with_mapping(right, mapping_r).into(),
            )
        } else {
            (left, right)
        };

        match op {
            BoundSetOperation::Union => Ok(LogicalUnion::create(all, vec![left, right])),
            BoundSetOperation::Intersect => Ok(LogicalIntersect::create(all, vec![left, right])),
            BoundSetOperation::Except => Ok(LogicalExcept::create(all, vec![left, right])),
        }
    }
}
