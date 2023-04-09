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
use risingwave_common::util::sort_util::ColumnOrder;

use crate::binder::BoundSetExpr;
use crate::expr::ExprImpl;
use crate::optimizer::plan_node::PlanRef;
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_set_expr(
        &mut self,
        set_expr: BoundSetExpr,
        extra_order_exprs: Vec<ExprImpl>,
        order: &[ColumnOrder],
    ) -> Result<PlanRef> {
        match set_expr {
            BoundSetExpr::Select(s) => self.plan_select(*s, extra_order_exprs, order),
            BoundSetExpr::Values(v) => self.plan_values(*v),
            BoundSetExpr::Query(q) => Ok(self.plan_query(*q)?.into_subplan()),
            BoundSetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => self.plan_set_operation(op, all, *left, *right),
        }
    }
}
