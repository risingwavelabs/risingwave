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

use std::collections::HashMap;

use risingwave_common::error::Result;

use crate::binder::{BoundStatement, ShareId};
use crate::optimizer::{OptimizerContextRef, PlanRoot};

mod delete;
mod insert;
mod query;
mod relation;
mod select;
mod set_expr;
mod set_operation;
mod statement;
mod update;
mod values;
pub use query::LIMIT_ALL_COUNT;

use crate::PlanRef;

/// `Planner` converts a bound statement to a [`crate::optimizer::plan_node::PlanNode`] tree
pub struct Planner {
    ctx: OptimizerContextRef,
    /// Mapping of `ShareId` to its share plan.
    /// The share plan can be a CTE, a source, a view and so on.
    share_cache: HashMap<ShareId, PlanRef>,
}

impl Planner {
    pub fn new(ctx: OptimizerContextRef) -> Planner {
        Planner {
            ctx,
            share_cache: Default::default(),
        }
    }

    /// Plan a [`BoundStatement`]. Need to bind a statement before plan.
    pub fn plan(&mut self, stmt: BoundStatement) -> Result<PlanRoot> {
        self.plan_statement(stmt)
    }

    pub fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }
}
