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

use std::collections::hash_map::Entry;
use std::time::Instant;

use futures::StreamExt;
use itertools::Itertools;
use risingwave_connector::connector_common::{
    IcebergCommittedSnapshot, recover_pending_commit_count,
};
use risingwave_connector::sink::iceberg::{IcebergWriteMode, commit_branch};
use risingwave_pb::catalog::PbStreamJobStatus;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use super::*;

/// Number of sinks whose Iceberg tables are loaded concurrently during recovery.
const RECOVERY_CONCURRENCY: usize = 16;

/// Compaction backlog of a sink, rebuilt from its Iceberg table metadata.
pub(super) struct RecoveredBacklog {
    pub(super) pending_commit_count: usize,
    pub(super) observed_snapshot: Option<IcebergCommittedSnapshot>,
}

impl IcebergCompactionManager {
    /// Rebuilds the maintenance state of existing Iceberg sinks once after meta starts.
    ///
    /// The state is kept only in memory and is otherwise created by sink commits. Without
    /// this recovery, a copy-on-write sink whose latest commits were not yet published to
    /// `main` would stay stale until it commits again.
    pub fn schedule_recovery_task(manager: Arc<Self>) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            tokio::select! {
                _ = manager.recover_maintenance_state() => {}
                _ = shutdown_rx => {
                    tracing::info!("Iceberg maintenance recovery is stopped");
                }
            }
        });
        (join_handle, shutdown_tx)
    }

    async fn recover_maintenance_state(&self) {
        let sinks = match self.metadata_manager.catalog_controller.list_sinks().await {
            Ok(sinks) => sinks,
            Err(e) => {
                tracing::warn!(
                    iceberg_component = "compaction_scheduler",
                    iceberg_operation = "recover_schedule",
                    error = %e.as_report(),
                    "iceberg_compaction_schedule_recovery_list_sinks_failed",
                );
                return;
            }
        };
        let sink_ids = sinks
            .into_iter()
            .filter(|sink| {
                sink.stream_job_status() == PbStreamJobStatus::Created
                    && is_iceberg_sink(&sink.properties)
            })
            .map(|sink| sink.id)
            .collect_vec();
        self.inner
            .write()
            .recovering_sink_ids
            .extend(sink_ids.iter().copied());

        futures::stream::iter(sink_ids)
            .for_each_concurrent(RECOVERY_CONCURRENCY, |sink_id| async move {
                match self.load_iceberg_config(sink_id).await {
                    Ok(config) => {
                        let backlog = Self::load_recovered_backlog(sink_id, &config).await;
                        self.apply_recovered_maintenance(sink_id, &config, backlog, Instant::now());
                    }
                    Err(e) => {
                        tracing::warn!(
                            iceberg_component = "compaction_scheduler",
                            iceberg_operation = "recover_schedule",
                            sink_id = %sink_id,
                            error = %e.as_report(),
                            "iceberg_compaction_schedule_recovery_config_unavailable",
                        );
                        self.inner.write().recovering_sink_ids.remove(&sink_id);
                    }
                }
            })
            .await;
    }

    async fn load_recovered_backlog(sink_id: SinkId, config: &IcebergConfig) -> RecoveredBacklog {
        if !config.enable_compaction {
            return RecoveredBacklog {
                pending_commit_count: 0,
                observed_snapshot: None,
            };
        }

        let branch = commit_branch(config.r#type.as_str(), config.write_mode);
        match config.load_table().await {
            Ok(table) => RecoveredBacklog {
                pending_commit_count: recover_pending_commit_count(table.metadata(), &branch),
                observed_snapshot: IcebergCommittedSnapshot::from_branch_head(
                    table.metadata(),
                    &branch,
                ),
            },
            Err(e) => {
                tracing::warn!(
                    iceberg_component = "compaction_scheduler",
                    iceberg_operation = "recover_schedule",
                    sink_id = %sink_id,
                    error = %e.as_report(),
                    "iceberg_compaction_schedule_recovery_table_unavailable",
                );
                // Copy-on-write data may be unpublished, so schedule one attempt. Merge-on-read
                // data is already visible, and its automatic rounds need an observed snapshot.
                RecoveredBacklog {
                    pending_commit_count: usize::from(
                        config.write_mode == IcebergWriteMode::CopyOnWrite,
                    ),
                    observed_snapshot: None,
                }
            }
        }
    }

    pub(super) fn apply_recovered_maintenance(
        &self,
        sink_id: SinkId,
        config: &IcebergConfig,
        backlog: RecoveredBacklog,
        now: Instant,
    ) {
        let mut guard = self.inner.write();
        if !guard.recovering_sink_ids.remove(&sink_id) {
            // The sink was dropped while its table was loading.
            return;
        }
        if config.enable_snapshot_expiration {
            guard.snapshot_expiration_sink_ids.insert(sink_id);
        }
        if config.enable_manifest_rewrite {
            guard.manifest_rewrite_sink_ids.insert(sink_id);
        }
        if !config.enable_compaction {
            return;
        }

        // A commit after the restart may have created the track already. It has at least one
        // pending commit, and a copy-on-write compaction publishes the whole ingestion branch.
        let Entry::Vacant(entry) = guard.sink_schedules.entry(sink_id) else {
            return;
        };
        let mut track = self.create_compaction_track(config, now);
        track.restore_backlog(backlog.pending_commit_count, backlog.observed_snapshot);
        entry.insert(track);
        tracing::info!(
            iceberg_component = "compaction_scheduler",
            iceberg_operation = "recover_schedule",
            sink_id = %sink_id,
            pending_commit_count = backlog.pending_commit_count,
            "iceberg_compaction_schedule_recovered",
        );
    }
}
