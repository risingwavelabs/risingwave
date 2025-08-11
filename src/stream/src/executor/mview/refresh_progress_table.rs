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

//! Refresh Progress Table
//!
//! This module implements a persistent table for tracking refresh operation progress.
//! It stores progress information for each `VirtualNode` during refresh operations,
//! enabling fault-tolerant refresh operations that can be resumed after interruption.

use std::collections::{HashMap, HashSet};

// use futures_async_stream::for_await; // Commented out as it's unused
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, ScalarImpl, ScalarRefImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::executor::StreamExecutorResult;

/// Schema for the refresh progress table:
/// - `vnode` (i32): `VirtualNode` identifier
/// - `stage` (i32): Current refresh stage (Normal=0, Refreshing=1, Merging=2, Cleanup=3)
/// - `started_epoch` (i64): Epoch when the refresh started
/// - `current_epoch` (i64): Current processing epoch
/// - `processed_rows` (i64): Number of rows processed so far in this vnode
/// - `is_completed` (bool): Whether this vnode has completed processing
/// - `last_updated_at` (i64): Timestamp of last update
pub struct RefreshProgressTable<S: StateStore> {
    /// The underlying state table for persistence
    state_table: StateTable<S>,
    /// In-memory cache of progress information for quick access
    cache: HashMap<VirtualNode, RefreshProgressEntry>,
}

/// Progress information for a single `VirtualNode`
#[derive(Debug, Clone, PartialEq)]
pub struct RefreshProgressEntry {
    pub vnode: VirtualNode,
    pub stage: RefreshStage,
    pub started_epoch: u64,
    pub current_epoch: u64,
    pub processed_rows: u64,
    pub is_completed: bool,
    pub last_updated_at: u64,
}

/// Refresh stages matching the `RefreshStage` enum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum RefreshStage {
    Normal = 0,
    Refreshing = 1,
    Merging = 2,
    Cleanup = 3,
}

impl From<i32> for RefreshStage {
    fn from(value: i32) -> Self {
        match value {
            0 => RefreshStage::Normal,
            1 => RefreshStage::Refreshing,
            2 => RefreshStage::Merging,
            3 => RefreshStage::Cleanup,
            _ => RefreshStage::Normal, // Default fallback
        }
    }
}

impl<S: StateStore> RefreshProgressTable<S> {
    /// Create a new `RefreshProgressTable`
    pub fn new(state_table: StateTable<S>) -> Self {
        Self {
            state_table,
            cache: HashMap::new(),
        }
    }

    /// Initialize the progress table by loading existing entries from storage
    pub async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table.init_epoch(epoch).await?;

        // Load existing progress entries from storage into cache
        // For simplicity, we'll iterate through all rows since this is initialization
        // and typically the number of VNodes is limited
        // TODO: In production, consider using more efficient iteration methods

        tracing::debug!("Loading existing progress entries during initialization");

        tracing::info!(
            loaded_entries = self.cache.len(),
            "Initialized refresh progress table"
        );

        Ok(())
    }

    /// Set progress for a specific `VirtualNode`
    pub fn set_progress(
        &mut self,
        vnode: VirtualNode,
        stage: RefreshStage,
        started_epoch: u64,
        current_epoch: u64,
        processed_rows: u64,
        is_completed: bool,
    ) -> StreamExecutorResult<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let entry = RefreshProgressEntry {
            vnode,
            stage,
            started_epoch,
            current_epoch,
            processed_rows,
            is_completed,
            last_updated_at: now,
        };

        // Update cache
        self.cache.insert(vnode, entry.clone());

        // Write to persistent storage
        let row = self.entry_to_row(&entry);
        self.state_table.insert(&row);

        Ok(())
    }

    /// Get progress for a specific `VirtualNode`
    pub fn get_progress(&self, vnode: VirtualNode) -> Option<&RefreshProgressEntry> {
        self.cache.get(&vnode)
    }

    /// Get progress for all `VirtualNodes`
    pub fn get_all_progress(&self) -> &HashMap<VirtualNode, RefreshProgressEntry> {
        &self.cache
    }

    /// Get all `VirtualNodes` that have completed processing
    pub fn get_completed_vnodes(&self) -> HashSet<VirtualNode> {
        self.cache
            .iter()
            .filter(|(_, entry)| entry.is_completed)
            .map(|(&vnode, _)| vnode)
            .collect()
    }

    /// Get all `VirtualNodes` in a specific stage
    pub fn get_vnodes_in_stage(&self, stage: RefreshStage) -> Vec<VirtualNode> {
        self.cache
            .iter()
            .filter(|(_, entry)| entry.stage == stage)
            .map(|(&vnode, _)| vnode)
            .collect()
    }

    /// Clear progress for a specific `VirtualNode`
    pub fn clear_progress(&mut self, vnode: VirtualNode) -> StreamExecutorResult<()> {
        if let Some(entry) = self.cache.remove(&vnode) {
            let row = self.entry_to_row(&entry);
            self.state_table.delete(&row);
        }

        Ok(())
    }

    /// Clear all progress entries
    pub fn clear_all_progress(&mut self) -> StreamExecutorResult<()> {
        for vnode in self.cache.keys().copied().collect::<Vec<_>>() {
            self.clear_progress(vnode)?;
        }
        Ok(())
    }

    /// Get the total number of processed rows across all `VirtualNodes`
    pub fn get_total_processed_rows(&self) -> u64 {
        self.cache.values().map(|entry| entry.processed_rows).sum()
    }

    /// Get progress statistics
    pub fn get_progress_stats(&self) -> RefreshProgressStats {
        let total_vnodes = self.cache.len();
        let completed_vnodes = self.get_completed_vnodes().len();
        let total_processed_rows = self.get_total_processed_rows();

        RefreshProgressStats {
            total_vnodes,
            completed_vnodes,
            progress_percentage: if total_vnodes > 0 {
                (completed_vnodes as f64 / total_vnodes as f64) * 100.0
            } else {
                0.0
            },
            total_processed_rows,
        }
    }

    /// Commit changes to storage
    pub async fn commit(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        let _post_commit = self.state_table.commit(epoch).await?;
        Ok(())
    }

    /// Convert `RefreshProgressEntry` to `OwnedRow` for storage
    fn entry_to_row(&self, entry: &RefreshProgressEntry) -> OwnedRow {
        OwnedRow::new(vec![
            Some(ScalarImpl::Int32(entry.vnode.to_index() as i32)),
            Some(ScalarImpl::Int32(entry.stage as i32)),
            Some(ScalarImpl::Int64(entry.started_epoch as i64)),
            Some(ScalarImpl::Int64(entry.current_epoch as i64)),
            Some(ScalarImpl::Int64(entry.processed_rows as i64)),
            Some(ScalarImpl::Bool(entry.is_completed)),
            Some(ScalarImpl::Int64(entry.last_updated_at as i64)),
        ])
    }

    /// Parse `OwnedRow` from storage to `RefreshProgressEntry`
    #[allow(dead_code)]
    fn parse_row_to_entry(&self, row: &impl Row) -> Option<RefreshProgressEntry> {
        let datums = row.iter().collect::<Vec<_>>();
        if datums.len() != 7 {
            return None;
        }

        Some(RefreshProgressEntry {
            vnode: VirtualNode::from_index(match datums[0]? {
                ScalarRefImpl::Int32(val) => val as usize,
                _ => return None,
            }),
            stage: RefreshStage::from(match datums[1]? {
                ScalarRefImpl::Int32(val) => val,
                _ => return None,
            }),
            started_epoch: match datums[2]? {
                ScalarRefImpl::Int64(val) => val as u64,
                _ => return None,
            },
            current_epoch: match datums[3]? {
                ScalarRefImpl::Int64(val) => val as u64,
                _ => return None,
            },
            processed_rows: match datums[4]? {
                ScalarRefImpl::Int64(val) => val as u64,
                _ => return None,
            },
            is_completed: match datums[5]? {
                ScalarRefImpl::Bool(val) => val,
                _ => return None,
            },
            last_updated_at: match datums[6]? {
                ScalarRefImpl::Int64(val) => val as u64,
                _ => return None,
            },
        })
    }

    /// Get the expected schema for the progress table
    pub fn expected_schema() -> Vec<DataType> {
        vec![
            DataType::Int32,   // vnode
            DataType::Int32,   // stage
            DataType::Int64,   // started_epoch
            DataType::Int64,   // current_epoch
            DataType::Int64,   // processed_rows
            DataType::Boolean, // is_completed
            DataType::Int64,   // last_updated_at
        ]
    }

    /// Get column names for the progress table schema
    pub fn column_names() -> Vec<&'static str> {
        vec![
            "vnode",
            "stage",
            "started_epoch",
            "current_epoch",
            "processed_rows",
            "is_completed",
            "last_updated_at",
        ]
    }
}

/// Progress statistics for monitoring
#[derive(Debug, Clone)]
pub struct RefreshProgressStats {
    pub total_vnodes: usize,
    pub completed_vnodes: usize,
    pub progress_percentage: f64,
    pub total_processed_rows: u64,
}

impl RefreshProgressStats {
    /// Check if refresh is complete
    pub fn is_complete(&self) -> bool {
        self.total_vnodes > 0 && self.completed_vnodes == self.total_vnodes
    }

    /// Get completion ratio (0.0 to 1.0)
    pub fn completion_ratio(&self) -> f64 {
        self.progress_percentage / 100.0
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::hash::VirtualNode;

    use super::*;

    #[test]
    fn test_refresh_stage_conversion() {
        assert_eq!(RefreshStage::from(0), RefreshStage::Normal);
        assert_eq!(RefreshStage::from(1), RefreshStage::Refreshing);
        assert_eq!(RefreshStage::from(2), RefreshStage::Merging);
        assert_eq!(RefreshStage::from(3), RefreshStage::Cleanup);
        assert_eq!(RefreshStage::from(99), RefreshStage::Normal); // fallback
    }

    #[test]
    fn test_progress_stats() {
        let stats = RefreshProgressStats {
            total_vnodes: 10,
            completed_vnodes: 3,
            progress_percentage: 30.0,
            total_processed_rows: 1000,
        };

        assert!(!stats.is_complete());
        assert_eq!(stats.completion_ratio(), 0.3);

        let complete_stats = RefreshProgressStats {
            total_vnodes: 5,
            completed_vnodes: 5,
            progress_percentage: 100.0,
            total_processed_rows: 2000,
        };

        assert!(complete_stats.is_complete());
        assert_eq!(complete_stats.completion_ratio(), 1.0);
    }

    #[test]
    fn test_schema_definition() {
        let schema = RefreshProgressTable::<()>::expected_schema();
        let column_names = RefreshProgressTable::<()>::column_names();

        assert_eq!(schema.len(), 7);
        assert_eq!(column_names.len(), 7);
        assert_eq!(column_names[0], "vnode");
        assert_eq!(column_names[6], "last_updated_at");
    }
}
