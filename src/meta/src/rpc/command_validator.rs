// Copyright 2024 RisingWave Labs
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
use std::ops::Mul;

use risingwave_meta_model_v2::worker::{self, WorkerType};
use risingwave_pb::common::worker_node::State;

use super::ddl_controller::{DdlCommand, DdlController};
use crate::manager::WorkerId;
use crate::{MetaError, MetaResult};

impl DdlController {
    pub async fn validate_command(&self, command: &DdlCommand) -> MetaResult<()> {
        match command {
            DdlCommand::CreateStreamingJob(_, _, _, _) => {
                let worker_actor_count = self.metadata_manager.worker_actor_count().await?;
                let running_worker_parallelism: HashMap<WorkerId, usize> = self
                    .metadata_manager
                    .list_worker_node(Some(WorkerType::ComputeNode), Some(State::Running))
                    .await?
                    .into_iter()
                    .map(|e| (e.id, e.parallelism()))
                    .collect();

                // check whether there are more than `actor_cnt_per_worker_parallelism_hard_limit` actors on a worker per parallelism
                let actor_cnt_per_worker_parallelism_hard_limit =
                    self.env.opts.actor_cnt_per_worker_parallelism_hard_limit;
                let (passed, failed): (Vec<_>, Vec<_>) = worker_actor_count
                    .into_iter()
                    .map(|(worker_id, actor_count)| (worker_id, actor_count))
                    .partition(|(worker_id, actor_count)| {
                        let max_actor_count = running_worker_parallelism
                            .get(worker_id)
                            .map(|c| c.saturating_mul(actor_cnt_per_worker_parallelism_hard_limit))
                            .unwrap_or(usize::MAX);
                        actor_count <= &max_actor_count
                    });

                if !failed.is_empty() {
                    let error_msg = format!(
                        "Too many actors on worker(s).
                        actor_cnt_per_worker_parallelism_hard_limit: {:?}), 
                        failed worker id -> actor count: {:?}, 
                        passed worker id -> actor count: {:?}",
                        actor_cnt_per_worker_parallelism_hard_limit, failed, passed
                    );
                    return MetaError::resource_exhausted(error_msg);
                }
            }
            _ => return Ok(()),
        }

        Ok(())
    }
}
