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

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use anyhow::anyhow;
use parking_lot::Mutex;
use risingwave_common::catalog::{DatabaseId, FragmentTypeFlag, TableId};
use risingwave_meta_model::ActorId;
use risingwave_meta_model::table::RefreshState;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::id::SourceId;
use risingwave_pb::meta::{RefreshRequest, RefreshResponse};
use thiserror_ext::AsReport;

use crate::barrier::{BarrierScheduler, Command, SharedActorInfos};
use crate::manager::MetadataManager;
use crate::{MetaError, MetaResult};

/// Global, per-table refresh progress tracker.
///
/// Lifecycle for each entry (keyed by `TableId`):
/// - Created at refresh start in `RefreshManager::refresh_table` before any `await`,
///   populated with the expected source/fetch actor sets for that table.
/// - Updated on each barrier in checkpoint control when executors report
///   list/load progress; see `CheckpointControl::handle_refresh_table_info`.
/// - Removed when the table is reported as refresh-finished by compute on a
///   barrier (`refresh_finished_table_ids`).
///
/// Failure/retry notes:
/// - If scheduling the refresh fails, the table state is reset to `Idle`
pub static REFRESH_TABLE_PROGRESS_TRACKER: LazyLock<Mutex<GlobalRefreshTableProgressTracker>> =
    LazyLock::new(|| Mutex::new(GlobalRefreshTableProgressTracker::default()));

#[derive(Default, Debug)]
pub struct GlobalRefreshTableProgressTracker {
    pub inner: HashMap<TableId, SingleTableRefreshProgressTracker>,
    pub table_id_by_database_id: HashMap<DatabaseId, HashSet<TableId>>,
}

impl GlobalRefreshTableProgressTracker {
    pub fn remove_tracker_by_database_id(&mut self, database_id: DatabaseId) {
        let table_ids = self
            .table_id_by_database_id
            .remove(&database_id)
            .unwrap_or_default();
        for table_id in table_ids {
            self.inner.remove(&table_id);
        }
    }
}

/// # High level design for refresh table
///
/// - Three tables:
///
/// - Main table: serves queries.
/// - Staging table: receives refreshed content during `Refreshing`.
/// - Progress table: per-VNode progress state for resumable refresh.
///
/// - Phased execution:
///
/// - Normal → Refreshing → Merging → Cleanup → Normal.
/// - Refreshing: load and write to staging.
/// - Merging: chunked sort-merge integrates staging into main; per-VNode progress persists checkpoints.
/// - Cleanup: purge staging and reset progress.
///
/// - Barrier-first responsiveness:
///
/// - Executor uses left-priority `select_with_strategy`, always handling upstream messages/barriers before background merge.
/// - On barriers, the executor persists progress so restarts resume exactly.
///
/// - Meta-managed state:
///
/// - `refresh_state` on each table enforces no concurrent refresh and enables recovery after failures.
/// - Startup recovery resets lingering `Refreshing` tables to `Idle` and lets executors resume `Finishing` safely.
///
/// ## Progress Table (Conceptual)
/// Tracks, per VNode:
/// - last processed position (e.g., last PK),
/// - completion flag,
/// - processed row count,
/// - last checkpoint epoch.
///
/// The executor initializes entries on `RefreshStart`, updates them during merge, and loads them at startup to resume from the last checkpoint.
///
/// ## Barrier Coordination and Completion
/// - Compute reports:
///
/// - `refresh_finished_table_ids`: indicates a materialized view finished refreshing.
/// - `truncate_tables`: staging tables to be cleaned up.
// - Checkpoint control aggregates these across barrier types; completion handlers in meta:
/// - update `refresh_state` to `Idle`,
/// - schedule/handle `LoadFinish`,
/// - drive cleanup work reliably after the storage version commit.
///
/// Manager responsible for handling refresh operations on refreshable tables
pub struct RefreshManager {
    metadata_manager: MetadataManager,
    barrier_scheduler: BarrierScheduler,
}

impl RefreshManager {
    /// Create a new `RefreshManager` instance
    pub fn new(metadata_manager: MetadataManager, barrier_scheduler: BarrierScheduler) -> Self {
        Self {
            metadata_manager,
            barrier_scheduler,
        }
    }

    /// Execute a refresh operation for the specified table
    ///
    /// This method:
    /// 1. Validates that the table exists and is refreshable
    /// 2. Checks current refresh state and ensures no concurrent refresh
    /// 3. Atomically sets the table state to REFRESHING
    /// 4. Sends a refresh command through the barrier system
    /// 5. Returns the result of the refresh operation
    pub async fn refresh_table(
        &self,
        request: RefreshRequest,
        shared_actor_infos: &SharedActorInfos,
    ) -> MetaResult<RefreshResponse> {
        let table_id = request.table_id;
        let associated_source_id = request.associated_source_id;

        // Validate that the table exists and is refreshable
        self.validate_refreshable_table(table_id, associated_source_id)
            .await?;

        tracing::info!("Starting refresh operation for table {}", table_id);

        // Get database_id for the table
        let database_id = self
            .metadata_manager
            .catalog_controller
            .get_object_database_id(table_id.as_raw_id() as _)
            .await?;

        // load actor info for refresh
        let job_fragments = self
            .metadata_manager
            .get_job_fragments_by_id(table_id.as_job_id())
            .await?;

        {
            let fragment_to_actor_mapping = shared_actor_infos.read_guard();
            let mut tracker = SingleTableRefreshProgressTracker::default();
            for (fragment_id, fragment) in &job_fragments.fragments {
                if fragment
                    .fragment_type_mask
                    .contains(FragmentTypeFlag::Source)
                    // should exclude dml fragments to avoid selecting the DML sql
                    && !fragment.fragment_type_mask.contains(FragmentTypeFlag::Dml)
                {
                    let fragment_info = fragment_to_actor_mapping
                        .get_fragment(*fragment_id)
                        .ok_or_else(|| MetaError::fragment_not_found(*fragment_id))?;
                    tracker.expected_list_actors.extend(
                        fragment_info
                            .actors
                            .keys()
                            .map(|actor_id| *actor_id as ActorId),
                    );
                }
                if fragment
                    .fragment_type_mask
                    .contains(FragmentTypeFlag::FsFetch)
                    && let Some(fragment_info) =
                        fragment_to_actor_mapping.get_fragment(*fragment_id)
                {
                    tracker.expected_fetch_actors.extend(
                        fragment_info
                            .actors
                            .keys()
                            .map(|actor_id| *actor_id as ActorId),
                    );
                }
            }

            {
                // Store tracker in global tracker before guard is dropped
                let mut lock_handle = REFRESH_TABLE_PROGRESS_TRACKER.lock();
                lock_handle.inner.insert(table_id, tracker);
                lock_handle
                    .table_id_by_database_id
                    .entry(database_id)
                    .or_default()
                    .insert(table_id);
            }

            Ok::<_, MetaError>(())
        }?;

        // Create refresh command
        let refresh_command = Command::Refresh {
            table_id,
            associated_source_id,
        };

        // Send refresh command through barrier system
        match self
            .barrier_scheduler
            .run_command(database_id, refresh_command)
            .await
        {
            Ok(_) => {
                tracing::info!(
                    table_id = %table_id,
                    "Refresh command completed successfully"
                );

                Ok(RefreshResponse { status: None })
            }
            Err(e) => {
                tracing::error!(
                    error = %e.as_report(),
                    table_id = %table_id,
                    "Failed to execute refresh command, resetting refresh state to Idle"
                );

                self.metadata_manager
                    .catalog_controller
                    .set_table_refresh_state(table_id, RefreshState::Idle)
                    .await?;

                {
                    let mut lock_handle = REFRESH_TABLE_PROGRESS_TRACKER.lock();
                    lock_handle.inner.remove(&table_id);
                    if let Some(table_ids) =
                        lock_handle.table_id_by_database_id.get_mut(&database_id)
                    {
                        table_ids.remove(&table_id);
                    }
                }

                Err(anyhow!(e)
                    .context(format!("Failed to refresh table {}", table_id))
                    .into())
            }
        }
    }

    /// Validate that the specified table exists and supports refresh operations
    async fn validate_refreshable_table(
        &self,
        table_id: TableId,
        associated_source_id: SourceId,
    ) -> MetaResult<()> {
        // Check if table exists in catalog
        let table = self
            .metadata_manager
            .catalog_controller
            .get_table_by_id(table_id)
            .await?;

        // Check if table is refreshable
        if !table.refreshable {
            return Err(MetaError::invalid_parameter(format!(
                "Table '{}' is not refreshable. Only tables created with REFRESHABLE flag support manual refresh.",
                table.name
            )));
        }

        if table.optional_associated_source_id
            != Some(OptionalAssociatedSourceId::AssociatedSourceId(
                associated_source_id.as_raw_id(),
            ))
        {
            return Err(MetaError::invalid_parameter(format!(
                "Table '{}' is not associated with source '{}'. table.optional_associated_source_id: {:?}",
                table.name, associated_source_id, table.optional_associated_source_id
            )));
        }

        let current_state = self
            .metadata_manager
            .catalog_controller
            .get_table_refresh_state(table_id)
            .await?;
        match current_state {
            Some(RefreshState::Idle) | None => {
                // the table is not refreshing. issue a refresh
            }
            state @ (Some(RefreshState::Finishing) | Some(RefreshState::Refreshing)) => {
                return Err(MetaError::invalid_parameter(format!(
                    "Table '{}' is currently in state {:?}. Cannot start a new refresh operation.",
                    table.name,
                    state.unwrap()
                )));
            }
        }

        tracing::debug!(
            table_id = %table_id,
            table_name = %table.name,
            "Table validation passed for refresh operation"
        );

        Ok(())
    }
}

#[derive(Default, Debug)]
pub struct SingleTableRefreshProgressTracker {
    pub expected_list_actors: HashSet<ActorId>,
    pub expected_fetch_actors: HashSet<ActorId>,
    pub list_finished_actors: HashSet<ActorId>,
    pub fetch_finished_actors: HashSet<ActorId>,
}

impl SingleTableRefreshProgressTracker {
    pub fn report_list_finished(&mut self, actor_ids: impl Iterator<Item = ActorId>) {
        self.list_finished_actors.extend(actor_ids);
    }

    pub fn is_list_finished(&self) -> MetaResult<bool> {
        if self.list_finished_actors.len() >= self.expected_list_actors.len() {
            if self.expected_list_actors == self.list_finished_actors {
                Ok(true)
            } else {
                Err(MetaError::from(anyhow!(
                    "list finished actors mismatch: expected: {:?}, actual: {:?}",
                    self.expected_list_actors,
                    self.list_finished_actors
                )))
            }
        } else {
            Ok(false)
        }
    }

    pub fn report_load_finished(&mut self, actor_ids: impl Iterator<Item = ActorId>) {
        self.fetch_finished_actors.extend(actor_ids);
    }

    pub fn is_load_finished(&self) -> MetaResult<bool> {
        if self.fetch_finished_actors.len() >= self.expected_fetch_actors.len() {
            if self.expected_fetch_actors == self.fetch_finished_actors {
                Ok(true)
            } else {
                Err(MetaError::from(anyhow!(
                    "fetch finished actors mismatch: expected: {:?}, actual: {:?}",
                    self.expected_fetch_actors,
                    self.fetch_finished_actors
                )))
            }
        } else {
            Ok(false)
        }
    }
}
