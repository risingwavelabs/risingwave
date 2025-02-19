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

use std::collections::HashMap;

use risingwave_pb::meta::actor_count_per_parallelism::PbWorkerActorCount;
use risingwave_pb::meta::cluster_limit::PbLimit;
use risingwave_pb::meta::{PbActorCountPerParallelism, PbClusterLimit};
pub enum ClusterLimit {
    ActorCount(ActorCountPerParallelism),
}

impl From<ClusterLimit> for PbClusterLimit {
    fn from(limit: ClusterLimit) -> Self {
        match limit {
            ClusterLimit::ActorCount(actor_count_per_parallelism) => PbClusterLimit {
                limit: Some(PbLimit::ActorCount(actor_count_per_parallelism.into())),
            },
        }
    }
}

impl From<PbClusterLimit> for ClusterLimit {
    fn from(pb_limit: PbClusterLimit) -> Self {
        match pb_limit.limit.unwrap() {
            PbLimit::ActorCount(actor_count_per_parallelism) => {
                ClusterLimit::ActorCount(actor_count_per_parallelism.into())
            }
        }
    }
}

#[derive(Debug)]
pub struct WorkerActorCount {
    pub actor_count: usize,
    pub parallelism: usize,
}

impl From<WorkerActorCount> for PbWorkerActorCount {
    fn from(worker_actor_count: WorkerActorCount) -> Self {
        PbWorkerActorCount {
            actor_count: worker_actor_count.actor_count as u64,
            parallelism: worker_actor_count.parallelism as u64,
        }
    }
}

impl From<PbWorkerActorCount> for WorkerActorCount {
    fn from(pb_worker_actor_count: PbWorkerActorCount) -> Self {
        WorkerActorCount {
            actor_count: pb_worker_actor_count.actor_count as usize,
            parallelism: pb_worker_actor_count.parallelism as usize,
        }
    }
}

pub struct ActorCountPerParallelism {
    pub worker_id_to_actor_count: HashMap<u32, WorkerActorCount>,
    pub hard_limit: usize,
    pub soft_limit: usize,
}

impl From<ActorCountPerParallelism> for PbActorCountPerParallelism {
    fn from(actor_count_per_parallelism: ActorCountPerParallelism) -> Self {
        PbActorCountPerParallelism {
            worker_id_to_actor_count: actor_count_per_parallelism
                .worker_id_to_actor_count
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            hard_limit: actor_count_per_parallelism.hard_limit as u64,
            soft_limit: actor_count_per_parallelism.soft_limit as u64,
        }
    }
}

impl From<PbActorCountPerParallelism> for ActorCountPerParallelism {
    fn from(pb_actor_count_per_parallelism: PbActorCountPerParallelism) -> Self {
        ActorCountPerParallelism {
            worker_id_to_actor_count: pb_actor_count_per_parallelism
                .worker_id_to_actor_count
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            hard_limit: pb_actor_count_per_parallelism.hard_limit as usize,
            soft_limit: pb_actor_count_per_parallelism.soft_limit as usize,
        }
    }
}

impl ActorCountPerParallelism {
    pub fn exceed_hard_limit(&self) -> bool {
        self.worker_id_to_actor_count
            .values()
            .any(|v| v.actor_count > self.hard_limit.saturating_mul(v.parallelism))
    }

    pub fn exceed_soft_limit(&self) -> bool {
        self.worker_id_to_actor_count
            .values()
            .any(|v| v.actor_count > self.soft_limit.saturating_mul(v.parallelism))
    }

    pub fn exceed_limit(&self) -> bool {
        self.exceed_soft_limit() || self.exceed_hard_limit()
    }
}
