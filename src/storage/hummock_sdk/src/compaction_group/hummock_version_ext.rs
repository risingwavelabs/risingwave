// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_pb::hummock::{HummockVersion, Level};

use crate::CompactionGroupId;

pub trait HummockVersionExt {
    /// Gets `compaction_group_id`'s levels
    fn get_compaction_group_levels(&self, compaction_group_id: CompactionGroupId) -> &Vec<Level>;
    /// Gets `compaction_group_id`'s levels
    fn get_compaction_group_levels_mut(
        &mut self,
        compaction_group_id: CompactionGroupId,
    ) -> &mut Vec<Level>;
    /// Gets all levels.
    ///
    /// Levels belonging to the same compaction group retain their relative order.
    fn get_combined_levels(&self) -> Vec<&Level>;
}

impl HummockVersionExt for HummockVersion {
    fn get_compaction_group_levels(&self, compaction_group_id: CompactionGroupId) -> &Vec<Level> {
        &self
            .levels
            .get(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} exists", compaction_group_id))
            .levels
    }

    fn get_compaction_group_levels_mut(
        &mut self,
        compaction_group_id: CompactionGroupId,
    ) -> &mut Vec<Level> {
        &mut self
            .levels
            .get_mut(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} exists", compaction_group_id))
            .levels
    }

    fn get_combined_levels(&self) -> Vec<&Level> {
        let mut combined_levels = vec![];
        for level in self.levels.values() {
            combined_levels.extend(level.levels.iter());
        }
        combined_levels
    }
}
