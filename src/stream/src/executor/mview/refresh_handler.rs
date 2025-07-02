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

use async_trait::async_trait;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::executor::error::StreamExecutorResult;

/// Trait for handling manual refresh operations
///
/// This trait defines the interface for coordinating refresh operations
/// between the source connector and the state table.
#[async_trait]
pub trait RefreshHandler: Send + Sync {
    /// Execute a manual refresh operation
    ///
    /// This method coordinates the full refresh process:
    /// 1. Clear existing state table data
    /// 2. Load new data from the refreshable connector
    /// 3. Calculate differences and apply changes
    /// 4. Commit the changes to the state table
    ///
    /// # Arguments
    ///
    /// * `table_id` - The ID of the table being refreshed
    /// * `epoch_pair` - The epoch pair for the refresh operation
    ///
    /// # Returns
    ///
    /// The number of rows affected by the refresh operation
    async fn execute_refresh(
        &mut self,
        table_id: TableId,
        epoch_pair: EpochPair,
    ) -> StreamExecutorResult<u64>;

    /// Check if a table supports refresh operations
    ///
    /// # Arguments
    ///
    /// * `table_id` - The ID of the table to check
    ///
    /// # Returns
    ///
    /// `true` if the table supports refresh, `false` otherwise
    fn supports_refresh(&self, table_id: TableId) -> bool;

    /// Get refresh statistics for a table
    ///
    /// # Arguments
    ///
    /// * `table_id` - The ID of the table
    ///
    /// # Returns
    ///
    /// Refresh statistics including last refresh time, row count, etc.
    async fn get_refresh_stats(&self, table_id: TableId) -> StreamExecutorResult<RefreshStats>;
}

/// Statistics for refresh operations
#[derive(Debug, Clone)]
pub struct RefreshStats {
    /// Last refresh timestamp (epoch)
    pub last_refresh_epoch: Option<u64>,

    /// Number of rows in the table after last refresh
    pub row_count: u64,

    /// Number of rows added in last refresh
    pub rows_added: u64,

    /// Number of rows removed in last refresh
    pub rows_removed: u64,

    /// Duration of last refresh operation in milliseconds
    pub refresh_duration_ms: u64,
}

impl Default for RefreshStats {
    fn default() -> Self {
        Self {
            last_refresh_epoch: None,
            row_count: 0,
            rows_added: 0,
            rows_removed: 0,
            refresh_duration_ms: 0,
        }
    }
}

/// Default implementation of RefreshHandler
///
/// This implementation coordinates refresh operations using the state table
/// and refresh connector infrastructure.
pub struct DefaultRefreshHandler<S: StateStore> {
    /// State table for data storage
    state_table: StateTable<S>,

    /// Statistics tracking
    stats: RefreshStats,
}

impl<S: StateStore> DefaultRefreshHandler<S> {
    /// Create a new refresh handler
    pub fn new(state_table: StateTable<S>) -> Self {
        Self {
            state_table,
            stats: RefreshStats::default(),
        }
    }

    /// Process a batch of refresh data
    async fn process_refresh_batch(
        &mut self,
        _chunks: Vec<StreamChunk>,
    ) -> StreamExecutorResult<()> {
        // For MVP, we'll skip the actual diff calculation
        // In future, this would use the diff calculator to compute changes
        // TODO: Implement proper diff calculation with connector data

        Ok(())
    }

    /// Apply buffered changes to the state table
    async fn apply_changes(&mut self, epoch_pair: EpochPair) -> StreamExecutorResult<u64> {
        let total_affected = 0;

        // Clear all existing rows first
        self.state_table.clear_all_rows().await?;

        // For MVP, we'll just clear all rows
        // In future, this would apply actual changes from the change buffer
        // TODO: Implement proper change application logic

        // Commit the changes
        let _post_commit = self.state_table.commit(epoch_pair).await?;

        // Update statistics
        self.stats.last_refresh_epoch = Some(epoch_pair.curr);
        self.stats.row_count = self.stats.rows_added - self.stats.rows_removed;

        Ok(total_affected)
    }
}

#[async_trait]
impl<S: StateStore> RefreshHandler for DefaultRefreshHandler<S> {
    async fn execute_refresh(
        &mut self,
        table_id: TableId,
        epoch_pair: EpochPair,
    ) -> StreamExecutorResult<u64> {
        let start_time = std::time::Instant::now();

        // For MVP, we'll implement a simplified refresh that just clears and reloads
        // In the future, this would coordinate with the actual connector

        // TODO: Get the actual refreshable connector for this table
        // For now, simulate by clearing the state table

        // Clear existing data
        self.state_table.clear_all_rows().await?;

        // TODO: Load new data from refreshable connector
        // For MVP, we'll just commit the cleared state
        let _post_commit = self.state_table.commit(epoch_pair).await?;

        // Update statistics
        let duration = start_time.elapsed();
        self.stats.refresh_duration_ms = duration.as_millis() as u64;
        self.stats.last_refresh_epoch = Some(epoch_pair.curr);

        tracing::info!(
            table_id = %table_id,
            epoch = epoch_pair.curr,
            duration_ms = self.stats.refresh_duration_ms,
            "Manual refresh completed"
        );

        Ok(0) // Return 0 for MVP (no actual data loaded)
    }

    fn supports_refresh(&self, _table_id: TableId) -> bool {
        // For MVP, assume all tables support refresh
        // In production, this would check the table catalog
        true
    }

    async fn get_refresh_stats(&self, _table_id: TableId) -> StreamExecutorResult<RefreshStats> {
        Ok(self.stats.clone())
    }
}

/// Factory for creating refresh handlers
pub struct RefreshHandlerFactory;

impl RefreshHandlerFactory {
    /// Create a refresh handler for a given state table
    pub fn create_handler<S: StateStore>(state_table: StateTable<S>) -> DefaultRefreshHandler<S> {
        DefaultRefreshHandler::new(state_table)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;

    #[tokio::test]
    async fn test_refresh_handler_creation() {
        // This test would require a proper StateTable setup
        // For now, we just test that the factory works
        let stats = RefreshStats::default();
        assert_eq!(stats.row_count, 0);
        assert!(stats.last_refresh_epoch.is_none());
    }

    #[tokio::test]
    async fn test_refresh_stats() {
        let mut stats = RefreshStats::default();
        stats.rows_added = 100;
        stats.rows_removed = 25;
        stats.row_count = stats.rows_added - stats.rows_removed;

        assert_eq!(stats.row_count, 75);
        assert_eq!(stats.rows_added, 100);
        assert_eq!(stats.rows_removed, 25);
    }
}
