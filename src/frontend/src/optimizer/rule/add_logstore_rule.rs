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

use crate::optimizer::plan_node::{
    Stream, StreamExchange, StreamPlanRef as PlanRef, StreamSyncLogStore,
};
use crate::optimizer::rule::{BoxedRule, Rule};

pub struct AddLogstoreRule {}

impl Rule<Stream> for AddLogstoreRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        plan.as_stream_hash_join()?;
        let log_store_plan = StreamSyncLogStore::new(plan);
        Some(log_store_plan.into())
    }
}

impl AddLogstoreRule {
    pub fn create() -> BoxedRule<Stream> {
        Box::new(Self {})
    }
}

pub struct EnsureSyncLogStoreRootRule {}

impl Rule<Stream> for EnsureSyncLogStoreRootRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        if plan.as_stream_exchange().is_some() {
            return None;
        }

        let mut changed = false;
        let inputs: Vec<_> = plan
            .inputs()
            .into_iter()
            .map(|input| {
                if input.as_stream_sync_log_store().is_some() {
                    changed = true;
                    StreamExchange::new_no_shuffle(input).into()
                } else {
                    input
                }
            })
            .collect();

        changed.then(|| plan.clone_root_with_inputs(&inputs))
    }
}

impl EnsureSyncLogStoreRootRule {
    pub fn create() -> BoxedRule<Stream> {
        Box::new(Self {})
    }
}
