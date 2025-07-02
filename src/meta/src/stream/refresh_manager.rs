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
    /// Create a new RefreshManager instance
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
        let tables = self
            .metadata_manager
            .catalog_controller
            .get_table_by_ids(vec![table_id.table_id() as i32], false)
            .await
            .map_err(|e| {
                MetaError::invalid_parameter(format!(
                    "Failed to query table with ID {}: {}",
                    table_id, e
                ))
            })?;

        let table_catalog = tables.first().ok_or_else(|| {
            MetaError::invalid_parameter(format!("Table with ID {} not found", table_id))
        })?;

        // Check if table is refreshable
        if !table_catalog.refreshable {
            return Err(MetaError::invalid_parameter(format!(
                "Table '{}' is not refreshable. Only tables created with REFRESHABLE flag support manual refresh.",
                table_catalog.name
            )));
        }

        tracing::debug!(
            table_id = %table_id,
            table_name = %table_catalog.name,
            "Table validation passed for refresh operation"
        );

        Ok(())
    }

    /// Get refresh statistics for a table (placeholder for future implementation)
    pub async fn get_refresh_stats(&self, _table_id: TableId) -> MetaResult<RefreshStats> {
        // For MVP, return default stats
        // In the future, this could track actual refresh history and metrics
        Ok(RefreshStats::default())
    }
}

/// Statistics for refresh operations
#[derive(Debug, Clone, Default)]
pub struct RefreshStats {
    /// Last refresh timestamp (epoch)
    pub last_refresh_epoch: Option<u64>,

    /// Number of rows in the table after last refresh
    pub row_count: u64,

    /// Number of refresh operations performed
    pub refresh_count: u64,

    /// Average refresh duration in milliseconds
    pub avg_refresh_duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_refresh_stats_default() {
        let stats = RefreshStats::default();
        assert_eq!(stats.row_count, 0);
        assert_eq!(stats.refresh_count, 0);
        assert_eq!(stats.avg_refresh_duration_ms, 0);
        assert!(stats.last_refresh_epoch.is_none());
    }

    #[test]
    fn test_refresh_stats_clone() {
        let mut stats = RefreshStats::default();
        stats.row_count = 100;
        stats.refresh_count = 5;
        stats.avg_refresh_duration_ms = 1500;
        stats.last_refresh_epoch = Some(12345);

        let cloned_stats = stats.clone();
        assert_eq!(cloned_stats.row_count, 100);
        assert_eq!(cloned_stats.refresh_count, 5);
        assert_eq!(cloned_stats.avg_refresh_duration_ms, 1500);
        assert_eq!(cloned_stats.last_refresh_epoch, Some(12345));
    }
}
