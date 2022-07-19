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

use std::collections::HashMap;

use risingwave_pb::common::WorkerNode;

use crate::error::Result;
use crate::types::ParallelUnitId;

pub fn get_workers_by_parallel_unit_ids(
    current_nodes: &[WorkerNode],
    parallel_unit_ids: &[ParallelUnitId],
) -> Result<Vec<WorkerNode>> {
    let mut pu_to_worker: HashMap<ParallelUnitId, WorkerNode> = HashMap::new();
    for node in current_nodes {
        for pu in &node.parallel_units {
            let res = pu_to_worker.insert(pu.id, node.clone());
            assert!(res.is_none(), "duplicate parallel unit id");
        }
    }

    let mut workers = Vec::with_capacity(parallel_unit_ids.len());
    for parallel_unit_id in parallel_unit_ids {
        match pu_to_worker.get(parallel_unit_id) {
            Some(worker) => workers.push(worker.clone()),
            None => bail!(
                "No worker node found for parallel unit id: {}",
                parallel_unit_id
            ),
        }
    }
    Ok(workers)
}
