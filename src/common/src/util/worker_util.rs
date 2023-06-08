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

use risingwave_pb::common::WorkerNode;

use crate::hash::ParallelUnitId;

pub type WorkerNodeId = u32;

pub fn get_pu_to_worker_mapping(nodes: &[WorkerNode]) -> HashMap<ParallelUnitId, WorkerNode> {
    let mut pu_to_worker = HashMap::new();

    for node in nodes {
        for pu in &node.parallel_units {
            let res = pu_to_worker.insert(pu.id, node.clone());
            assert!(res.is_none(), "duplicate parallel unit id");
        }
    }

    pu_to_worker
}
