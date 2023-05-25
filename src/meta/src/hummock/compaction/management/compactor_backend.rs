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

use async_trait::async_trait;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::CompactTask;

use crate::hummock::HummockManagerRef;
use crate::storage::MetaStore;

#[async_trait]
pub trait CompactorBackend {
    async fn submit(&self, compact_task: &CompactTask) -> bool;
}

pub struct DedicatedCompactorBackend<S>
where
    S: MetaStore,
{
    hummock_manager: HummockManagerRef<S>,
}

impl<S> DedicatedCompactorBackend<S> where S: MetaStore {}

#[async_trait]
impl<S> CompactorBackend for DedicatedCompactorBackend<S>
where
    S: MetaStore,
{
    async fn submit(&self, compact_task: &CompactTask) -> bool {
        // how to trigger a new compaction when failed assign ?

        if let Some(compactor) = self.hummock_manager.get_idle_compactor().await {
            // 2. Assign the compaction task to a compactor.
            match self
                .hummock_manager
                .assign_compaction_task(compact_task, compactor.context_id())
                .await
            {
                Ok(_) => {}
                Err(err) => {
                    // handle error
                    // log
                    return false;
                }
            };

            // 3. Send the compaction task.
            if let Err(e) = compactor
                .send_task(Task::CompactTask(compact_task.clone()))
                .await
            {
                // handle error
                // log

                return false;
            }

            return true;
        } else {
            return false;
        }
    }
}
