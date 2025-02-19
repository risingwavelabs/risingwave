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

use crate::optimizer::plan_node::StreamSyncLogStore;
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::PlanRef;

pub struct AddLogstoreRule {}

impl Rule for AddLogstoreRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        plan.as_stream_hash_join()?;
        let log_store_plan = StreamSyncLogStore::new(plan);
        Some(log_store_plan.into())
    }
}

impl AddLogstoreRule {
    pub fn create() -> BoxedRule {
        Box::new(Self {})
    }
}
