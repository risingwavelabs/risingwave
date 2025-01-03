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

use std::collections::BTreeMap;

use risingwave_common::util::column_index_mapping::ColIndexMapping;

use crate::utils::IndexSet;

pub type WatermarkGroupId = u32;

/// Represents the output watermark columns of a plan node.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct WatermarkColumns {
    col_idx_to_wtmk_group_id: BTreeMap<usize, WatermarkGroupId>,
}

impl WatermarkColumns {
    /// Create an empty `WatermarkColumns`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if the `WatermarkColumns` is empty.
    pub fn is_empty(&self) -> bool {
        self.col_idx_to_wtmk_group_id.is_empty()
    }

    /// Get the number of watermark columns.
    pub fn n_indices(&self) -> usize {
        self.col_idx_to_wtmk_group_id.len()
    }

    /// Insert a column index with a watermark group ID.
    pub fn insert(&mut self, col_idx: usize, group: WatermarkGroupId) {
        self.col_idx_to_wtmk_group_id.insert(col_idx, group);
    }

    /// Check if the `WatermarkColumns` contains a column index.
    pub fn contains(&self, col_idx: usize) -> bool {
        self.col_idx_to_wtmk_group_id.contains_key(&col_idx)
    }

    /// Get the watermark group ID of a column index.
    pub fn get_group(&self, col_idx: usize) -> Option<WatermarkGroupId> {
        self.col_idx_to_wtmk_group_id.get(&col_idx).copied()
    }

    /// Get all watermark columns as a `IndexSet`.
    pub fn index_set(&self) -> IndexSet {
        self.col_idx_to_wtmk_group_id.keys().copied().collect()
    }

    /// Iterate over all column indices, in ascending order.
    pub fn indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.col_idx_to_wtmk_group_id.keys().copied()
    }

    /// Get all watermark groups and their corresponding column indices.
    pub fn grouped(&self) -> BTreeMap<WatermarkGroupId, IndexSet> {
        let mut groups = BTreeMap::new();
        for (col_idx, group_id) in &self.col_idx_to_wtmk_group_id {
            groups
                .entry(*group_id)
                .or_insert_with(IndexSet::empty)
                .insert(*col_idx);
        }
        groups
    }

    /// Iterate over all column indices and their corresponding watermark group IDs, in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = (usize, WatermarkGroupId)> + '_ {
        self.col_idx_to_wtmk_group_id
            .iter()
            .map(|(&col_idx, &group_id)| (col_idx, group_id))
    }

    /// Clone and shift the column indices to the right (larger) by `column_shift`.
    pub fn right_shift_clone(&self, column_shift: usize) -> Self {
        let col_idx_to_wtmk_group_id = self
            .col_idx_to_wtmk_group_id
            .iter()
            .map(|(&col_idx, &group_id)| (col_idx + column_shift, group_id))
            .collect();
        Self {
            col_idx_to_wtmk_group_id,
        }
    }

    /// Clone and retain only the columns with indices in `col_indices`.
    pub fn retain_clone(&self, col_indices: &[usize]) -> Self {
        let mut new = Self::new();
        for &col_idx in col_indices {
            if let Some(group_id) = self.get_group(col_idx) {
                new.insert(col_idx, group_id);
            }
        }
        new
    }

    /// Clone and map the column indices using `col_mapping`.
    pub fn map_clone(&self, col_mapping: &ColIndexMapping) -> Self {
        let col_idx_to_wtmk_group_id = self
            .col_idx_to_wtmk_group_id
            .iter()
            .filter_map(|(&col_idx, &group_id)| {
                col_mapping
                    .try_map(col_idx)
                    .map(|new_col_idx| (new_col_idx, group_id))
            })
            .collect();
        Self {
            col_idx_to_wtmk_group_id,
        }
    }
}
