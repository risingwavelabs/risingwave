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

use crate::binder::BoundStatement;
use crate::error::Result;
use crate::optimizer::LogicalPlanRoot;
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_statement(&mut self, stmt: BoundStatement) -> Result<LogicalPlanRoot> {
        match stmt {
            BoundStatement::Insert(i) => self.plan_insert(*i),
            BoundStatement::Delete(d) => self.plan_delete(*d),
            BoundStatement::Update(u) => self.plan_update(*u),
            BoundStatement::Query(q) => self.plan_query(*q),
            BoundStatement::DeclareCursor(d) => self.plan_query(*d.query),
            BoundStatement::DeclareSubscriptionCursor(_) => unimplemented!(),
            BoundStatement::FetchCursor(_) => unimplemented!(),
            BoundStatement::CreateView(c) => self.plan_query(*c.query),
            BoundStatement::Compact(_) => {
                // COMPACT statements don't need planning as they are handled directly
                // by the handler. This should not be reached in normal execution.
                unimplemented!("COMPACT statements should be handled by the compact handler")
            }
        }
    }
}
