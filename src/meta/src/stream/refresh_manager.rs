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
use risingwave_pb::meta::{RefreshRequest, RefreshResponse};

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
    /// 2. Sends a refresh command through the barrier system
    /// 3. Returns the result of the refresh operation
    pub async fn refresh_table(&self, request: RefreshRequest) -> MetaResult<RefreshResponse> {
        let table_id = TableId::new(request.table_id);

        tracing::info!("Starting refresh operation for table {}", table_id);

        // Validate that the table exists and is refreshable
        self.validate_refreshable_table(table_id).await?;

        // Get database_id for the table
        let database_id = DatabaseId::new(
            self.metadata_manager
                .catalog_controller
                .get_object_database_id(table_id.table_id() as _)
                .await? as _,
        );

        // Create refresh command
        let refresh_command = Command::Refresh { table_id };

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

                Ok(RefreshResponse {
                    status: None, // Success indicated by None status
                })
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    table_id = %table_id,
                    "Failed to execute refresh command"
                );

                Err(anyhow!("Failed to refresh table {}: {}", table_id, e).into())
            }
        }
    }

    /// Validate that the specified table exists and supports refresh operations
    async fn validate_refreshable_table(&self, table_id: TableId) -> MetaResult<()> {
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

        tracing::debug!(
            table_id = %table_id,
            table_name = %table.name,
            "Table validation passed for refresh operation"
        );

        Ok(())
    }
}
