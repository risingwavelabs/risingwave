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

use itertools::Itertools;

use crate::hummock::compaction::CompactStatus;

impl From<&CompactStatus> for risingwave_pb::hummock::CompactStatus {
    fn from(status: &CompactStatus) -> Self {
        risingwave_pb::hummock::CompactStatus {
            compaction_group_id: status.compaction_group_id,
            level_handlers: status.level_handlers.iter().map_into().collect(),
        }
    }
}

impl From<CompactStatus> for risingwave_pb::hummock::CompactStatus {
    fn from(status: CompactStatus) -> Self {
        (&status).into()
    }
}

impl From<&risingwave_pb::hummock::CompactStatus> for CompactStatus {
    fn from(status: &risingwave_pb::hummock::CompactStatus) -> Self {
        CompactStatus {
            compaction_group_id: status.compaction_group_id,
            level_handlers: status.level_handlers.iter().map_into().collect(),
        }
    }
}
