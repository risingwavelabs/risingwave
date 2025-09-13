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
use std::sync::Arc;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, ScalarImpl, ScalarRefImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTablePostCommit;
use crate::executor::StreamExecutorResult;
use crate::executor::prelude::StateTable;

/// Schema for the refresh progress table (simplified, following backfill pattern):
/// - `vnode` (i32): `VirtualNode` identifier
/// - `current_pos` (variable): Current processing position (primary key of last processed row)
/// - `is_completed` (bool): Whether this vnode has completed processing
/// - `processed_rows` (i64): Number of rows processed so far in this vnode
pub struct RefreshProgressTable<S: StateStore> {
    /// The underlying state table for persistence
    pub state_table: StateTable<S>,
    /// In-memory cache of progress information for quick access
    cache: HashMap<VirtualNode, RefreshProgressEntry>,
    /// Primary key length for upstream table (needed for schema)
    pk_len: usize,
}

/// Progress information for a single `VirtualNode`
#[derive(Debug, Clone, PartialEq)]
pub struct RefreshProgressEntry {
    pub vnode: VirtualNode,
    pub current_pos: Option<OwnedRow>,
    pub is_completed: bool,
    pub processed_rows: u64,
}

/// Refresh stages are now tracked by `MaterializeExecutor` in memory
/// This enum is kept for compatibility but no longer stored in progress table
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ProgressRefreshStage {
    Normal = 0,
    Refreshing = 1,
    Merging = 2,
    Cleanup = 3,
}

impl From<i32> for ProgressRefreshStage {
    fn from(value: i32) -> Self {
        match value {
            0 => ProgressRefreshStage::Normal,
            1 => ProgressRefreshStage::Refreshing,
            2 => ProgressRefreshStage::Merging,
            3 => ProgressRefreshStage::Cleanup,
            _ => unreachable!(),
        }
    }
}

impl<S: StateStore> RefreshProgressTable<S> {
    /// Create a new `RefreshProgressTable`
    pub fn new(state_table: StateTable<S>, pk_len: usize) -> Self {
        Self {
            state_table,
            cache: HashMap::new(),
            pk_len,
        }
    }

    /// Initialize the progress table by loading existing entries from storage
    pub async fn recover(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table.init_epoch(epoch).await?;

        // Load existing progress entries from storage into cache
        let mut loaded_count = 0;

        for vnode in self.state_table.vnodes().iter_ones() {
            let row = self
                .state_table
                .get_row(OwnedRow::new(vec![
                    VirtualNode::from_index(vnode).to_datum(),
                ]))
                .await?;
            if row.is_some()
                && let Some(entry) = self.parse_row_to_entry(&row, self.pk_len)
            {
                self.cache.insert(entry.vnode, entry);
                loaded_count += 1;
            }
        }

        tracing::debug!(
            loaded_count,
            "Loading existing progress entries during initialization"
        );

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
        current_pos: Option<OwnedRow>,
        is_completed: bool,
        processed_rows: u64,
    ) -> StreamExecutorResult<()> {
        let entry = RefreshProgressEntry {
            vnode,
            current_pos,
            is_completed,
            processed_rows,
        };

        // Update cache
        self.cache.insert(vnode, entry.clone());

        // Write to persistent storage
        let row = self.entry_to_row(&entry, self.pk_len);
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

    /// Get all `VirtualNodes` in a specific stage (now tracked by executor memory)
    /// This method is kept for compatibility but should not be used with simplified schema
    pub fn get_vnodes_in_stage(&self, _stage: ProgressRefreshStage) -> Vec<VirtualNode> {
        tracing::warn!(
            "get_vnodes_in_stage called on simplified progress table - stage info no longer stored"
        );
        Vec::new()
    }

    /// Clear progress for a specific `VirtualNode`
    pub fn clear_progress(&mut self, vnode: VirtualNode) -> StreamExecutorResult<()> {
        if let Some(entry) = self.cache.remove(&vnode) {
            let row = self.entry_to_row(&entry, self.pk_len);
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
            total_processed_rows,
        }
    }

    /// Commit changes to storage
    pub async fn commit(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<StateTablePostCommit<'_, S>> {
        self.state_table.commit(epoch).await
    }

    /// Convert `RefreshProgressEntry` to `OwnedRow` for storage
    /// New schema: | vnode | `current_pos`... | `is_completed` | `processed_rows` |
    fn entry_to_row(&self, entry: &RefreshProgressEntry, pk_len: usize) -> OwnedRow {
        let mut row_data = vec![entry.vnode.to_datum()];

        // Add current_pos fields (variable length based on primary key)
        if let Some(ref pos) = entry.current_pos {
            row_data.extend(pos.iter().map(|d| d.map(|s| s.into_scalar_impl())));
        } else {
            // Use None placeholders for empty position (similar to backfill finished state)
            for _ in 0..pk_len {
                row_data.push(None);
            }
        }

        // Add metadata fields
        row_data.push(Some(ScalarImpl::Bool(entry.is_completed)));
        row_data.push(Some(ScalarImpl::Int64(entry.processed_rows as i64)));

        OwnedRow::new(row_data)
    }

    /// Parse `OwnedRow` from storage to `RefreshProgressEntry`
    /// New schema: | vnode | `current_pos`... | `is_completed` | `processed_rows` |
    /// Note: The length depends on the primary key length of upstream table
    fn parse_row_to_entry(&self, row: &impl Row, pk_len: usize) -> Option<RefreshProgressEntry> {
        let datums = row.iter().collect::<Vec<_>>();
        let expected_len = 1 + pk_len + 2; // vnode + pk_fields + is_completed + processed_rows

        if datums.len() != expected_len {
            tracing::warn!(
                "Row length mismatch: got {}, expected {} (pk_len={}), row: {:?}",
                datums.len(),
                expected_len,
                pk_len,
                row,
            );
            return None;
        }

        // Parse vnode (first field)
        let vnode = VirtualNode::from_index(match datums[0]? {
            ScalarRefImpl::Int32(val) => val as usize,
            _ => return None,
        });

        // Parse current_pos (pk_len fields after vnode)
        let current_pos = if pk_len > 0 {
            let pos_datums: Vec<_> = datums[1..1 + pk_len]
                .iter()
                .map(|d| d.map(|s| s.into_scalar_impl()))
                .collect();
            // Check if all position fields are None (indicating finished/empty state)
            if pos_datums.iter().all(|d| d.is_none()) {
                None
            } else {
                Some(OwnedRow::new(pos_datums))
            }
        } else {
            None
        };

        // Parse is_completed (second to last field)
        let is_completed = match datums[1 + pk_len]? {
            ScalarRefImpl::Bool(val) => val,
            _ => return None,
        };

        // Parse processed_rows (last field)
        let processed_rows = match datums[1 + pk_len + 1]? {
            ScalarRefImpl::Int64(val) => val as u64,
            _ => return None,
        };

        Some(RefreshProgressEntry {
            vnode,
            current_pos,
            is_completed,
            processed_rows,
        })
    }

    /// Get the expected schema for the progress table
    /// Schema: | vnode | `current_pos`... | `is_completed` | `processed_rows` |
    pub fn expected_schema(pk_data_types: &[DataType]) -> Vec<DataType> {
        let mut schema = vec![DataType::Int32]; // vnode
        schema.extend(pk_data_types.iter().cloned()); // current_pos fields
        schema.push(DataType::Boolean); // is_completed
        schema.push(DataType::Int64); // processed_rows
        schema
    }

    /// Get column names for the progress table schema
    /// Schema: | vnode | `current_pos`... | `is_completed` | `processed_rows` |
    pub fn column_names(pk_column_names: &[&str]) -> Vec<String> {
        let mut names = vec!["vnode".to_owned()];
        for pk_name in pk_column_names {
            names.push(format!("pos_{}", pk_name));
        }
        names.push("is_completed".to_owned());
        names.push("processed_rows".to_owned());
        names
    }

    pub fn vnodes(&self) -> &Arc<Bitmap> {
        self.state_table.vnodes()
    }
}

/// Progress statistics for monitoring
#[derive(Debug, Clone)]
pub struct RefreshProgressStats {
    pub total_vnodes: usize,
    pub completed_vnodes: usize,
    pub total_processed_rows: u64,
}

impl RefreshProgressStats {
    /// Check if refresh is complete
    pub fn is_complete(&self) -> bool {
        self.total_vnodes > 0 && self.completed_vnodes == self.total_vnodes
    }
}
