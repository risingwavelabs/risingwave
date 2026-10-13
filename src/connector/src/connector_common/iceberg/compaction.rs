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

use ::iceberg::spec::TableMetadata;

use crate::sink::catalog::SinkId;

#[derive(Debug, Clone)]
pub struct IcebergCommittedSnapshot {
    pub branch: String,
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

impl IcebergCommittedSnapshot {
    /// Describes the head snapshot of `branch`, or returns `None` if the branch has no snapshot.
    pub fn from_branch_head(metadata: &TableMetadata, branch: &str) -> Option<Self> {
        metadata.snapshot_for_ref(branch).map(|snapshot| Self {
            branch: branch.to_owned(),
            snapshot_id: snapshot.snapshot_id(),
            timestamp_ms: snapshot.timestamp_ms(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct IcebergSinkCompactionUpdate {
    // runtime event information
    pub sink_id: SinkId,
    pub force_compaction: bool,
    pub observed_snapshot: IcebergCommittedSnapshot,
}
