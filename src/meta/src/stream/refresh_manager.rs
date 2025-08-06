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

use anyhow::anyhow;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::meta::{RefreshRequest, RefreshResponse};
use thiserror_ext::AsReport;

use crate::barrier::{BarrierScheduler, Command};
use crate::manager::MetadataManager;
use crate::{MetaError, MetaResult};

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
    pub async fn refresh_table(&self, request: RefreshRequest) -> MetaResult<RefreshResponse> {
        let table_id = TableId::new(request.table_id);
        let associated_source_id = TableId::new(request.associated_source_id);

        tracing::info!("Starting refresh operation for table {}", table_id);

        // Validate that the table exists and is refreshable
        self.validate_refreshable_table(table_id, associated_source_id)
            .await?;

        // Check and atomically update refresh state to prevent concurrent refreshes
        self.check_and_set_refresh_state(table_id).await?;

        // Get database_id for the table
        let database_id = DatabaseId::new(
            self.metadata_manager
                .catalog_controller
                .get_object_database_id(table_id.table_id() as _)
                .await? as _,
        );

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

                // Reset table state to IDLE after successful completion
                if let Err(e) = self.reset_refresh_state(table_id).await {
                    tracing::warn!(
                        error = %e.as_report(),
                        table_id = %table_id,
                        "Failed to reset refresh state after successful completion"
                    );
                }

                Ok(RefreshResponse {
                    status: None, // Success indicated by None status
                })
            }
            Err(e) => {
                tracing::error!(
                    error = %e.as_report(),
                    table_id = %table_id,
                    "Failed to execute refresh command"
                );

                // Reset table state to IDLE after failure
                if let Err(reset_err) = self.reset_refresh_state(table_id).await {
                    tracing::error!(
                        error = %reset_err.as_report(),
                        table_id = %table_id,
                        "Failed to reset refresh state after command failure"
                    );
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
        associated_source_id: TableId,
    ) -> MetaResult<()> {
        // Check if table exists in catalog
        let table = self
            .metadata_manager
            .catalog_controller
            .get_table_by_id(table_id.table_id as _)
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
                associated_source_id.table_id(),
            ))
        {
            return Err(MetaError::invalid_parameter(format!(
                "Table '{}' is not associated with source '{}'. table.optional_associated_source_id: {:?}",
                table.name, associated_source_id, table.optional_associated_source_id
            )));
        }

        tracing::debug!(
            table_id = %table_id,
            table_name = %table.name,
            "Table validation passed for refresh operation"
        );

        Ok(())
    }

    /// Check current refresh state and atomically set it to REFRESHING if it's IDLE
    ///
    /// This prevents concurrent refresh operations on the same table
    async fn check_and_set_refresh_state(&self, table_id: TableId) -> MetaResult<()> {
        use risingwave_pb::catalog::RefreshState;

        // Use the atomic check-and-set operation
        let success = self
            .metadata_manager
            .catalog_controller
            .check_and_set_table_refresh_state(
                table_id.table_id as _,
                RefreshState::Idle as i32,
                RefreshState::Refreshing as i32,
            )
            .await?;

        if !success {
            // Get current state for better error message
            let current_state = self
                .metadata_manager
                .catalog_controller
                .get_table_refresh_state(table_id.table_id as _)
                .await?;

            return Err(MetaError::invalid_parameter(format!(
                "Table {} is currently in refresh state {}. Cannot start a new refresh operation while another is in progress.",
                table_id, current_state
            )));
        }

        tracing::info!(
            table_id = %table_id,
            "Successfully set table refresh state to REFRESHING"
        );

        Ok(())
    }

    /// Reset table refresh state to IDLE
    ///
    /// This should be called after refresh completion (success or failure)
    async fn reset_refresh_state(&self, table_id: TableId) -> MetaResult<()> {
        use risingwave_pb::catalog::RefreshState;

        // Reset state to IDLE
        self.metadata_manager
            .catalog_controller
            .set_table_refresh_state(table_id.table_id as _, RefreshState::Idle as i32)
            .await?;

        tracing::info!(
            table_id = %table_id,
            "Reset table refresh state to IDLE"
        );

        Ok(())
    }

    /// Check for and recover any interrupted refresh operations
    ///
    /// This method should be called during system startup to detect tables
    /// that were in the middle of a refresh operation when the system shut down
    pub async fn recover_interrupted_refreshes(&self) -> MetaResult<Vec<TableId>> {
        use risingwave_pb::catalog::RefreshState;

        let interrupted_tables = self
            .metadata_manager
            .catalog_controller
            .find_tables_by_refresh_state(RefreshState::Refreshing as i32)
            .await?;

        let mut recovered_tables = Vec::new();

        for table_id in interrupted_tables {
            tracing::warn!(
                table_id = %table_id,
                "Detected interrupted refresh operation, resetting state to IDLE"
            );

            // Reset interrupted refreshes to IDLE state
            // The actual refresh will need to be restarted manually
            if let Err(e) = self.reset_refresh_state(TableId::new(table_id)).await {
                tracing::error!(
                    error = %e,
                    table_id = %table_id,
                    "Failed to reset interrupted refresh state"
                );
            } else {
                recovered_tables.push(TableId::new(table_id));
            }
        }

        if !recovered_tables.is_empty() {
            tracing::info!(
                count = recovered_tables.len(),
                tables = ?recovered_tables,
                "Recovered interrupted refresh operations"
            );
        }

        Ok(recovered_tables)
    }
}
