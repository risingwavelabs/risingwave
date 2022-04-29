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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use risingwave_common::error::Result;
use risingwave_pb::hummock::CompactTaskAssignment;

use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
use crate::hummock::compaction::tier_compaction_picker::TierCompactionPicker;
use crate::hummock::compaction::CompactStatus;
use crate::hummock::compaction_group::{
    CompactionGroup, CompactionGroupId, CompactionPickerImpl, DEFAULT_COMPACTION_GROUP_ID,
};
use crate::model::MetadataModel;
use crate::storage::MetaStore;

pub type CompactionGroupRef = Arc<parking_lot::RwLock<CompactionGroup>>;

#[derive(Default)]
pub struct Compaction {
    pub(crate) compact_status: CompactStatus,
    pub(crate) compaction_groups: HashMap<CompactionGroupId, CompactionGroupRef>,
    pub(crate) compact_task_assignment: BTreeMap<u64, CompactTaskAssignment>,
}

impl Compaction {
    pub async fn init(&mut self, meta_store: &impl MetaStore) -> Result<()> {
        self.compact_status = CompactStatus::get(meta_store)
            .await?
            .unwrap_or_else(CompactStatus::new);

        self.compact_task_assignment = CompactTaskAssignment::list(meta_store)
            .await?
            .into_iter()
            .map(|assigned| (assigned.key().unwrap().id, assigned))
            .collect();

        // TODO: load groups from meta_store
        // TODO: init groups. DEFAULT_COMPACTION_GROUP_ID should be the common level of all groups.
        let picker = CompactionPickerImpl::Tiered(TierCompactionPicker::new(Box::new(
            RangeOverlapStrategy::default(),
        )));
        let default_group = CompactionGroup::new(DEFAULT_COMPACTION_GROUP_ID, picker);
        self.compaction_groups = HashMap::new();
        self.compaction_groups.insert(
            default_group.group_id().to_owned(),
            Arc::new(parking_lot::RwLock::new(default_group)),
        );
        Ok(())
    }

    pub fn get_default_compaction_group(&self) -> CompactionGroupRef {
        self.compaction_groups
            .get(&DEFAULT_COMPACTION_GROUP_ID)
            .expect("default compaction group not found")
            .clone()
    }
}
