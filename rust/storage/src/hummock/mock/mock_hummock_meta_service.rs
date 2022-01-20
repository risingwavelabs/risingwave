use std::collections::BTreeMap;

use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_pb::hummock::{
    AddTablesRequest, AddTablesResponse, GetNewTableIdRequest, GetNewTableIdResponse,
    HummockSnapshot, HummockVersion, PinSnapshotRequest, PinSnapshotResponse, PinVersionRequest,
    PinVersionResponse, UncommittedEpoch, UnpinSnapshotRequest, UnpinSnapshotResponse,
    UnpinVersionRequest, UnpinVersionResponse,
};

use crate::hummock::{
    HummockEpoch, HummockRefCount, HummockSSTableId, HummockVersionId, INVALID_EPOCH,
    INVALID_VERSION,
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
            INVALID_VERSION,
            HummockVersion {
                levels: vec![],
                uncommitted_epochs: vec![],
                max_committed_epoch: INVALID_EPOCH,
            },
        );
        MockHummockMetaServiceInner {
            version_ref_counts: BTreeMap::new(),
            snapshot_ref_counts: BTreeMap::new(),
            versions,
            current_version_id: INVALID_VERSION,
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
            pinned_version_id: greatest_version_id,
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
        // let mut guard = self.inner.lock();
        // let ref_count_entry = guard.snapshot_ref_counts.entry(guard.max_committed_epoch);
        // *ref_count_entry.or_insert(0) += 1;
        // TODO #2336 Because e2e checkpoint is not ready yet, we temporarily return the maximum
        // write_batch epoch to enable uncommitted read.
        let guard = self.inner.lock();
        let greatest_version = guard.versions.values().last().unwrap().clone();
        let maximum_uncommitted_epoch = greatest_version
            .uncommitted_epochs
            .iter()
            .map(|uncommitted_epoch| uncommitted_epoch.epoch)
            .max()
            .unwrap_or(INVALID_EPOCH);
        PinSnapshotResponse {
            status: None,
            snapshot: Some(HummockSnapshot {
                epoch: maximum_uncommitted_epoch,
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
        let old_version_id = guard.current_version_id;
        guard.current_version_id += 1;
        let new_version_id = guard.current_version_id;
        let mut greatest_version = guard
            .versions
            .get(&old_version_id)
            .unwrap_or(&HummockVersion {
                levels: vec![],
                uncommitted_epochs: vec![],
                max_committed_epoch: guard.max_committed_epoch,
            })
            .clone();
        greatest_version.uncommitted_epochs.push(UncommittedEpoch {
            epoch: request.epoch,
            table_ids: request.tables.iter().map(|table| table.id).collect_vec(),
        });
        guard.versions.insert(new_version_id, greatest_version);
        AddTablesResponse {
            status: None,
            version_id: new_version_id,
        }
    }
}

// TODO #2156 add MockMockHummockMetaService tests
