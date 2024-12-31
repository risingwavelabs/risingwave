// Copyright 2024 RisingWave Labs
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

use std::collections::{BTreeMap, HashMap};

use itertools::Itertools;
use risingwave_common::util::column_index_mapping::ColIndexMapping;

use crate::utils::IndexSet;

pub type WatermarkGroupId = u32;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct WatermarkColumns {
    col_idx_to_wtmk_group_id: BTreeMap<usize, WatermarkGroupId>,
}

impl WatermarkColumns {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.col_idx_to_wtmk_group_id.is_empty()
    }

    pub fn n_indices(&self) -> usize {
        self.col_idx_to_wtmk_group_id.len()
    }

    pub fn insert(&mut self, col_idx: usize, group: WatermarkGroupId) {
        self.col_idx_to_wtmk_group_id.insert(col_idx, group);
    }

    pub fn contains(&self, col_idx: usize) -> bool {
        self.col_idx_to_wtmk_group_id.contains_key(&col_idx)
    }

    pub fn get_group(&self, col_idx: usize) -> Option<WatermarkGroupId> {
        self.col_idx_to_wtmk_group_id.get(&col_idx).copied()
    }

    pub fn index_set(&self) -> IndexSet {
        self.col_idx_to_wtmk_group_id.keys().copied().collect()
    }

    pub fn indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.col_idx_to_wtmk_group_id.keys().copied()
    }

    pub fn grouped(&self) -> HashMap<WatermarkGroupId, IndexSet> {
        self.col_idx_to_wtmk_group_id
            .iter()
            .map(|(col_idx, group_id)| (*group_id, *col_idx))
            .into_grouping_map()
            .fold_with(
                |_, _| IndexSet::empty(),
                |mut idx_set, group_id, col_idx| {
                    idx_set.insert(col_idx);
                    idx_set
                },
            )
    }

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
                    .and_then(|new_col_idx| Some((new_col_idx, group_id)))
            })
            .collect();
        Self {
            col_idx_to_wtmk_group_id,
        }
    }
}
