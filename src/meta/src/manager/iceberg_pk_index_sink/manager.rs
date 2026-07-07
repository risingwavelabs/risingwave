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
use risingwave_pb::stream_service::barrier_complete_response::IcebergPkIndexSinkMetadata as PbIcebergPkIndexSinkMetadata;
use sea_orm::DatabaseConnection;
use tokio::sync::Mutex;
use tracing::warn;

use super::coordinator::{IcebergPkIndexSinkCoordinator, PendingRemapReTrigger};

type CoordinatorRef = Arc<Mutex<IcebergPkIndexSinkCoordinator>>;

/// Manager for the Iceberg pk-index sink path, cheap to clone.
#[derive(Clone)]
pub struct IcebergPkIndexSinkManager {
    inner: Arc<ManagerInner>,
}

struct ManagerInner {
    db: DatabaseConnection,
    /// Read on every pre-commit/commit (only to clone out the `CoordinatorRef`, never held across an await);
    /// written only by register/unregister/reset, which are rare control-plane events.
    coordinators: RwLock<HashMap<SinkId, CoordinatorRef>>,
}

impl IcebergPkIndexSinkManager {
    pub fn new(db: DatabaseConnection) -> Self {
        IcebergPkIndexSinkManager {
            inner: Arc::new(ManagerInner {
                db,
                coordinators: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// Register an Iceberg pk-index sink so its commit coordinator is ready to receive epoch reports. Builds and
    /// fully initializes the coordinator (loading the iceberg table and draining any recovered pending
    /// commits) BEFORE inserting it, so a successful return means the sink is ready to serve. Idempotent:
    /// registering the same `sink_id` replaces the existing coordinator.
    pub async fn register_sink(
        &self,
        sink_id: SinkId,
        iceberg_config: IcebergConfig,
    ) -> anyhow::Result<Vec<PendingRemapReTrigger>> {
        // Initialize (load + recover + drain) outside the map lock; this is the slow, fallible part.
        // `init` also reloads durable pending compaction remaps and returns the ones that must be
        // re-injected as barrier mutations (the caller owns the barrier scheduler).
        let (coordinator, pending_remaps) =
            IcebergPkIndexSinkCoordinator::init(sink_id, iceberg_config, self.inner.db.clone())
                .await?;

        let prev = self
            .inner
            .coordinators
            .write()
            .insert(sink_id, Arc::new(Mutex::new(coordinator)));
        if prev.is_some() {
            // Replacing an existing coordinator. Any in-flight commit on the old one keeps it alive via its
            // own `Arc` until it finishes; the snapshot_id idempotency check guards against double-commit.
            warn!(%sink_id, "iceberg pk-index sink coordinator re-registered; replacing previous instance");
        }
        Ok(pending_remaps)
    }

    /// Pre-commit phase for one epoch: persist the merged report under `pending_sink_state` (no iceberg IO).
    /// The barrier-complete path awaits this BEFORE issuing hummock `commit_epoch`.
    pub async fn pre_commit_epoch(
        &self,
        sink_id: SinkId,
        prev_epoch: u64,
        reports: Vec<PbIcebergPkIndexSinkMetadata>,
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
    pub async fn commit_epoch(&self, sink_id: SinkId) -> anyhow::Result<()> {
        let coordinator = self.coordinator(sink_id)?;
        coordinator.lock().await.commit().await
    }

    /// Commit a pk-index **compaction** overwrite for `sink_id`: replace `input_file_paths` with
    /// `output_files` via a single iceberg `overwrite_files` transaction. Routed through the same
    /// per-sink coordinator lock as [`Self::pre_commit_epoch`]/[`Self::commit_epoch`], so it can
    /// never race the sink's own barrier-driven commits. Fails (rather than silently no-oping) if
    /// the sink has no registered coordinator (e.g. not yet recovered after a meta restart) — the
    /// caller should treat that as a retryable failure.
    pub async fn commit_compaction_overwrite(
        &self,
        sink_id: SinkId,
        remap_id: i64,
        output_files: Vec<SerializedDataFile>,
        input_file_paths: Vec<String>,
        mapping_paths: Vec<String>,
        read_snapshot_id: i64,
    ) -> anyhow::Result<()> {
        let coordinator = self.coordinator(sink_id)?;
        coordinator
            .lock()
            .await
            .commit_compaction_overwrite(
                remap_id,
                output_files,
                input_file_paths,
                mapping_paths,
                read_snapshot_id,
            )
            .await
    }

    /// Unregister the given `sink_id`(s)' coordinator(s) (e.g. at DROP SINK time). Unregistering an unknown
    /// `sink_id` is a no-op.
    pub fn unregister_sinks(&self, sink_ids: Vec<SinkId>) {
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
                    "iceberg pk-index sink coordinator for sink {} is not registered",
                    sink_id
                )
            })
    }
}
