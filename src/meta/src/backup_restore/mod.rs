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

#![allow(dead_code)]

mod backup_manager;
pub use backup_manager::*;
pub mod error;
mod meta_snapshot;
mod storage;
pub use storage::*;
mod restore;
mod utils;
pub use restore::*;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::{HummockSstableId, HummockVersionId};
use risingwave_pb::hummock::HummockVersion;
use serde::{Deserialize, Serialize};

pub type MetaSnapshotId = u64;
pub type MetaBackupJobId = u64;

/// `MetaSnapshotMetadata` is metadata of `MetaSnapshot`.
#[derive(Serialize, Deserialize, Clone)]
pub struct MetaSnapshotMetadata {
    pub id: MetaSnapshotId,
    pub hummock_version_id: HummockVersionId,
    pub ssts: Vec<HummockSstableId>,
}

impl MetaSnapshotMetadata {
    pub fn new(id: MetaSnapshotId, v: &HummockVersion) -> Self {
        Self {
            id,
            hummock_version_id: v.id,
            ssts: v.get_sst_ids(),
        }
    }
}
