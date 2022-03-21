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
//
use risingwave_common::error::Result;

use crate::binder::BoundStatement;
use crate::optimizer::PlanRoot;
use crate::session::QueryContextRef;

mod delete;
mod insert;
mod query;
mod select;
mod set_expr;
mod statement;
mod relation;
mod values;

/// `Planner` converts a bound statement to a [`crate::optimizer::plan_node::PlanNode`] tree
pub struct Planner {
    ctx: QueryContextRef,
}

impl Planner {
    pub fn new(ctx: QueryContextRef) -> Planner {
        Planner { ctx }
    }

    /// Plan a [`BoundStatement`]. Need to bind a statement before plan.
    pub fn plan(&mut self, stmt: BoundStatement) -> Result<PlanRoot> {
        self.plan_statement(stmt)
    }

    pub fn ctx(&self) -> QueryContextRef {
        self.ctx.clone()
    }
}
