// Copyright 2026 RisingWave Labs
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
use std::sync::Arc;

use anyhow::anyhow;
use iceberg::spec::SerializedDataFile;
use parking_lot::RwLock;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_pb::stream_service::barrier_complete_response::IcebergV3SinkMetadata as PbIcebergV3SinkMetadata;
use sea_orm::DatabaseConnection;
use tokio::sync::Mutex;
use tracing::warn;

use super::coordinator::IcebergV3Coordinator;

type CoordinatorRef = Arc<Mutex<IcebergV3Coordinator>>;

/// Manager for the Iceberg V3 sink path, cheap to clone.
#[derive(Clone)]
pub struct IcebergV3SinkManager {
    inner: Arc<ManagerInner>,
}

struct ManagerInner {
    db: DatabaseConnection,
    /// Read on every pre-commit/commit (only to clone out the `CoordinatorRef`, never held across an await);
    /// written only by register/unregister/reset, which are rare control-plane events.
    coordinators: RwLock<HashMap<SinkId, CoordinatorRef>>,
}

impl IcebergV3SinkManager {
    pub fn new(db: DatabaseConnection) -> Self {
        IcebergV3SinkManager {
            inner: Arc::new(ManagerInner {
                db,
                coordinators: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// Register an Iceberg V3 sink so its commit coordinator is ready to receive epoch reports. Builds and
    /// fully initializes the coordinator (loading the iceberg table and draining any recovered pending
    /// commits) BEFORE inserting it, so a successful return means the sink is ready to serve. Idempotent:
    /// registering the same `sink_id` replaces the existing coordinator.
    pub async fn register_v3_sink(
        &self,
        sink_id: SinkId,
        iceberg_config: IcebergConfig,
    ) -> anyhow::Result<()> {
        // Initialize (load + recover + drain) outside the map lock; this is the slow, fallible part.
        let coordinator =
            IcebergV3Coordinator::init(sink_id, iceberg_config, self.inner.db.clone()).await?;

        let prev = self
            .inner
            .coordinators
            .write()
            .insert(sink_id, Arc::new(Mutex::new(coordinator)));
        if prev.is_some() {
            // Replacing an existing coordinator. Any in-flight commit on the old one keeps it alive via its
            // own `Arc` until it finishes; the snapshot_id idempotency check guards against double-commit.
            warn!(%sink_id, "iceberg v3 coordinator re-registered; replacing previous instance");
        }
        Ok(())
    }

    /// Pre-commit phase for one epoch: persist the merged report under `pending_sink_state` (no iceberg IO).
    /// The barrier-complete path awaits this BEFORE issuing hummock `commit_epoch`.
    pub async fn pre_commit_v3_epoch(
        &self,
        sink_id: SinkId,
        prev_epoch: u64,
        reports: Vec<PbIcebergV3SinkMetadata>,
    ) -> anyhow::Result<()> {
        let coordinator = self.coordinator(sink_id)?;
        coordinator
            .lock()
            .await
            .pre_commit(prev_epoch, reports)
            .await
    }

    /// Commit phase for one epoch: run an iceberg `overwrite_files` transaction and mark its pending row
    /// committed. The barrier-complete path awaits this AFTER hummock `commit_epoch`.
    pub async fn commit_v3_epoch(&self, sink_id: SinkId) -> anyhow::Result<()> {
        let coordinator = self.coordinator(sink_id)?;
        coordinator.lock().await.commit().await
    }

    /// Commit a V3 coordinated compaction: resolve file conflicts against the current snapshot and run an
    /// iceberg `overwrite_files` transaction. Returns an error if the coordinator is not registered (sink
    /// not active) or if the iceberg transaction fails.
    pub async fn commit_compaction(
        &self,
        sink_id: SinkId,
        output_files: Vec<SerializedDataFile>,
        input_file_paths: Vec<String>,
        read_snapshot_id: i64,
        pk_column_names: Vec<String>,
    ) -> anyhow::Result<()> {
        let coordinator = self.coordinator(sink_id)?;
        coordinator
            .lock()
            .await
            .commit_compaction(
                output_files,
                input_file_paths,
                read_snapshot_id,
                pk_column_names,
            )
            .await
    }

    /// Unregister the given `sink_id`(s)' coordinator(s) (e.g. at DROP SINK time). Unregistering an unknown
    /// `sink_id` is a no-op.
    pub fn unregister_v3_sinks(&self, sink_ids: Vec<SinkId>) {
        let mut coordinators = self.inner.coordinators.write();
        for sink_id in sink_ids {
            coordinators.remove(&sink_id);
        }
    }

    /// Drop every coordinator. Used at recovery time.
    pub fn reset(&self) {
        self.inner.coordinators.write().clear();
    }

    fn coordinator(&self, sink_id: SinkId) -> anyhow::Result<CoordinatorRef> {
        self.inner
            .coordinators
            .read()
            .get(&sink_id)
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "iceberg v3 coordinator for sink {} is not registered",
                    sink_id
                )
            })
    }
}
