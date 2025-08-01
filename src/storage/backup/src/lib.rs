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

#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(trait_alias)]
#![feature(type_alias_impl_trait)]
#![feature(custom_test_frameworks)]
#![feature(map_try_insert)]
#![feature(btree_extract_if)]
#![feature(let_chains)]
#![feature(error_generic_member_access)]
#![feature(coverage_attribute)]

pub mod error;
pub mod meta_snapshot;
pub mod meta_snapshot_v1;
pub mod meta_snapshot_v2;
pub mod storage;

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::RW_VERSION;
use risingwave_hummock_sdk::state_table_info::StateTableInfo;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{HummockRawObjectId, HummockVersionId};
use risingwave_pb::backup_service::{PbMetaSnapshotManifest, PbMetaSnapshotMetadata};
use serde::{Deserialize, Serialize};

use crate::error::{BackupError, BackupResult};

pub type MetaSnapshotId = u64;
pub type MetaBackupJobId = u64;

/// `MetaSnapshotMetadata` is metadata of `MetaSnapshot`.
#[derive(Serialize, Deserialize, Clone)]
pub struct MetaSnapshotMetadata {
    pub id: MetaSnapshotId,
    pub hummock_version_id: HummockVersionId,
    // rename to `ssts` for backward compatibility
    #[serde(rename = "ssts")]
    pub objects: HashSet<HummockRawObjectId>,
    #[serde(default)]
    pub format_version: u32,
    pub remarks: Option<String>,
    #[serde(default)]
    pub state_table_info: HashMap<u32, StateTableInfo>,
    pub rw_version: Option<String>,
}

impl MetaSnapshotMetadata {
    pub fn new(
        id: MetaSnapshotId,
        v: &HummockVersion,
        format_version: u32,
        remarks: Option<String>,
    ) -> Self {
        Self {
            id,
            hummock_version_id: v.id,
            objects: v
                .get_object_ids(false)
                .map(|object_id| object_id.as_raw())
                .collect(),
            format_version,
            remarks,
            state_table_info: v
                .state_table_info
                .info()
                .iter()
                .map(|(id, info)| (id.table_id, info.into()))
                .collect(),
            rw_version: Some(RW_VERSION.to_owned()),
        }
    }
}

/// `MetaSnapshotManifest` is the source of truth for valid `MetaSnapshot`.
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct MetaSnapshotManifest {
    pub manifest_id: u64,
    pub snapshot_metadata: Vec<MetaSnapshotMetadata>,
}

pub fn xxhash64_checksum(data: &[u8]) -> u64 {
    twox_hash::XxHash64::oneshot(0, data)
}

pub fn xxhash64_verify(data: &[u8], checksum: u64) -> BackupResult<()> {
    let data_checksum = xxhash64_checksum(data);
    if data_checksum != checksum {
        return Err(BackupError::ChecksumMismatch {
            expected: checksum,
            found: data_checksum,
        });
    }
    Ok(())
}

impl From<&MetaSnapshotMetadata> for PbMetaSnapshotMetadata {
    fn from(m: &MetaSnapshotMetadata) -> Self {
        Self {
            id: m.id,
            hummock_version_id: m.hummock_version_id.to_u64(),
            format_version: Some(m.format_version),
            remarks: m.remarks.clone(),
            state_table_info: m
                .state_table_info
                .iter()
                .map(|(t, i)| (*t, i.into()))
                .collect(),
            rw_version: m.rw_version.clone(),
        }
    }
}

impl From<&MetaSnapshotManifest> for PbMetaSnapshotManifest {
    fn from(m: &MetaSnapshotManifest) -> Self {
        Self {
            manifest_id: m.manifest_id,
            snapshot_metadata: m.snapshot_metadata.iter().map_into().collect_vec(),
        }
    }
}
