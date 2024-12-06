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

use risingwave_pb::hummock::PbStateTableInfo;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct StateTableInfo {
    #[serde(default)]
    pub committed_epoch: u64,
    #[serde(default)]
    pub compaction_group_id: u64,
}

impl From<StateTableInfo> for PbStateTableInfo {
    fn from(i: StateTableInfo) -> Self {
        (&i).into()
    }
}

impl From<&StateTableInfo> for PbStateTableInfo {
    fn from(i: &StateTableInfo) -> Self {
        Self {
            committed_epoch: i.committed_epoch,
            compaction_group_id: i.compaction_group_id,
        }
    }
}

impl From<PbStateTableInfo> for StateTableInfo {
    fn from(i: PbStateTableInfo) -> Self {
        (&i).into()
    }
}

impl From<&PbStateTableInfo> for StateTableInfo {
    fn from(i: &PbStateTableInfo) -> Self {
        Self {
            committed_epoch: i.committed_epoch,
            compaction_group_id: i.compaction_group_id,
        }
    }
}
