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

use crate::Planner;
use crate::binder::{BoundSetExpr, ShareId};
use crate::error::Result;
use crate::optimizer::plan_node::{LogicalPlanRef as PlanRef, LogicalRecursiveUnion};

impl Planner {
    pub(super) fn plan_recursive_union(
        &mut self,
        base: BoundSetExpr,
        recursive: BoundSetExpr,
        id: ShareId,
    ) -> Result<PlanRef> {
        let base = self.plan_set_expr(base, vec![], &[])?;
        let recursive = self.plan_set_expr(recursive, vec![], &[])?;
        let plan = LogicalRecursiveUnion::create(base, recursive, id);
        self.ctx.insert_rcte_cache_plan(id, plan.clone());
        Ok(plan)
    }
}
