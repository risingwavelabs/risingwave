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

use std::collections::btree_map::BTreeMap;

use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_pb::hummock::{
    AddTablesRequest, AddTablesResponse, CommitEpochRequest, CommitEpochResponse,
    GetNewTableIdRequest, GetNewTableIdResponse, HummockSnapshot, HummockVersion, Level, LevelType,
    PinSnapshotRequest, PinSnapshotResponse, PinVersionRequest, PinVersionResponse,
    UnpinSnapshotRequest, UnpinSnapshotResponse, UnpinVersionRequest, UnpinVersionResponse,
};

use crate::hummock::{
    HummockEpoch, HummockRefCount, HummockSSTableId, HummockVersionId, FIRST_VERSION_ID,
    INVALID_EPOCH,
};

/// Mock of the `HummockService` in meta crate. Not all RPCs are mocked.
/// When you add new RPC mocks, you may also need to modify existing RPC mocks to ensure
/// correctness.
pub struct MockHummockMetaService {
    inner: Mutex<MockHummockMetaServiceInner>,
}

struct MockHummockMetaServiceInner {
    version_ref_counts: BTreeMap<HummockVersionId, HummockRefCount>,
    snapshot_ref_counts: BTreeMap<HummockEpoch, HummockRefCount>,
    versions: BTreeMap<HummockVersionId, HummockVersion>,
    /// version_id used last time.
    current_version_id: HummockVersionId,
    max_committed_epoch: HummockEpoch,
    /// table_id used last time.
    current_table_id: HummockSSTableId,
}

impl MockHummockMetaServiceInner {
    fn new() -> MockHummockMetaServiceInner {
        let mut versions = BTreeMap::new();
        versions.insert(
            FIRST_VERSION_ID,
            HummockVersion {
                id: FIRST_VERSION_ID,
                levels: vec![Level {
                    level_type: LevelType::Overlapping as i32,
                    table_ids: vec![],
                }],
                uncommitted_epochs: vec![],
                max_committed_epoch: INVALID_EPOCH,
                safe_epoch: INVALID_EPOCH,
            },
        );
        MockHummockMetaServiceInner {
            version_ref_counts: BTreeMap::new(),
            snapshot_ref_counts: BTreeMap::new(),
            versions,
            current_version_id: FIRST_VERSION_ID,
            max_committed_epoch: INVALID_EPOCH,
            current_table_id: 0,
        }
    }
}

impl Default for MockHummockMetaService {
    fn default() -> Self {
        Self::new()
    }
}

impl MockHummockMetaService {
    pub fn new() -> MockHummockMetaService {
        MockHummockMetaService {
            inner: Mutex::new(MockHummockMetaServiceInner::new()),
        }
    }

    pub fn pin_version(&self, _request: PinVersionRequest) -> PinVersionResponse {
        let mut guard = self.inner.lock();
        let greatest_version_id = *guard.versions.keys().last().unwrap();
        let greatest_version = guard.versions.get(&greatest_version_id).unwrap().clone();
        let ref_count_entry = guard.version_ref_counts.entry(greatest_version_id);
        *ref_count_entry.or_insert(0) += 1;
        PinVersionResponse {
            status: None,
            pinned_version: Some(greatest_version),
        }
    }

    pub fn unpin_version(&self, request: UnpinVersionRequest) -> UnpinVersionResponse {
        let mut guard = self.inner.lock();
        guard
            .version_ref_counts
            .entry(request.pinned_version_id)
            .and_modify(|c| *c -= 1);
        UnpinVersionResponse { status: None }
    }

    pub fn pin_snapshot(&self, _request: PinSnapshotRequest) -> PinSnapshotResponse {
        let mut guard = self.inner.lock();
        let max_committed_epoch = guard.max_committed_epoch;
        let ref_count_entry = guard.snapshot_ref_counts.entry(max_committed_epoch);
        *ref_count_entry.or_insert(0) += 1;
        PinSnapshotResponse {
            status: None,
            snapshot: Some(HummockSnapshot {
                epoch: max_committed_epoch,
            }),
        }
    }

    pub fn unpin_snapshot(&self, request: UnpinSnapshotRequest) -> UnpinSnapshotResponse {
        let mut guard = self.inner.lock();
        let epoch = request.snapshot.unwrap().epoch;
        guard
            .snapshot_ref_counts
            .entry(epoch)
            .and_modify(|c| *c -= 1);
        UnpinSnapshotResponse { status: None }
    }

    pub fn get_new_table_id(&self, _request: GetNewTableIdRequest) -> GetNewTableIdResponse {
        let mut guard = self.inner.lock();
        guard.current_table_id += 1;
        GetNewTableIdResponse {
            status: None,
            table_id: guard.current_table_id,
        }
    }

    pub fn add_tables(&self, request: AddTablesRequest) -> AddTablesResponse {
        let mut guard = self.inner.lock();
        let mut new_version = guard
            .versions
            .get(&guard.current_version_id)
            .cloned()
            .unwrap();
        new_version.levels[0]
            .table_ids
            .extend(request.tables.iter().map(|table| table.id).collect_vec());
        guard.current_version_id += 1;
        new_version.id = guard.current_version_id;
        guard.versions.insert(new_version.id, new_version.clone());
        AddTablesResponse {
            status: None,
            version: Some(new_version),
        }
    }

    pub fn commit_epoch(&self, request: CommitEpochRequest) -> CommitEpochResponse {
        let mut guard = self.inner.lock();
        if request.epoch > guard.max_committed_epoch {
            let mut new_version = guard
                .versions
                .get(&guard.current_version_id)
                .cloned()
                .unwrap();
            guard.current_version_id += 1;
            new_version.max_committed_epoch = request.epoch;
            new_version.id = guard.current_version_id;
            guard.versions.insert(new_version.id, new_version);
        }
        CommitEpochResponse { status: None }
    }
}
