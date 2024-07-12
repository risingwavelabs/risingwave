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

use std::collections::{HashMap, HashSet};

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::HummockEpoch;

use crate::hummock::event_handler::uploader::{UnsyncData, UploadTaskInput};
use crate::hummock::event_handler::LocalInstanceId;

#[derive(Default)]
struct EpochSpillableDataInfo {
    instance_ids: HashSet<LocalInstanceId>,
    payload_size: usize,
}

pub(super) struct Spiller<'a> {
    unsync_data: &'a mut UnsyncData,
    epoch_info: HashMap<HummockEpoch, EpochSpillableDataInfo>,
}

impl<'a> Spiller<'a> {
    pub(super) fn new(unsync_data: &'a mut UnsyncData) -> Self {
        // TODO: devide spill payload by epoch-table_ids
        let temp = 0;
        let mut epoch_info: HashMap<_, EpochSpillableDataInfo> = HashMap::new();
        for instance_data in unsync_data
            .table_data
            .values()
            .flat_map(|table_data| table_data.instance_data.values())
        {
            if let Some((epoch, spill_size)) = instance_data.spillable_data_info() {
                let epoch_info = epoch_info.entry(epoch).or_default();
                assert!(epoch_info.instance_ids.insert(instance_data.instance_id));
                epoch_info.payload_size += spill_size;
            }
        }
        Self {
            unsync_data,
            epoch_info,
        }
    }

    pub(super) fn next_spilled_payload(
        &mut self,
    ) -> Option<(HummockEpoch, UploadTaskInput, HashSet<TableId>)> {
        // TODO: may get the epoch with max payload with btree if possible
        if let Some(epoch) = self
            .epoch_info
            .iter()
            .max_by_key(|(_, info)| info.payload_size)
            .map(|(epoch, _)| *epoch)
        {
            let spill_info = self.epoch_info.remove(&epoch).expect("should exist");
            let epoch_info = &mut self.epoch_info;
            let mut payload = HashMap::new();
            let mut spilled_table_ids = HashSet::new();
            for instance_id in spill_info.instance_ids {
                let table_id = *self
                    .unsync_data
                    .instance_table_id
                    .get(&instance_id)
                    .expect("should exist");
                let instance_data = self
                    .unsync_data
                    .table_data
                    .get_mut(&table_id)
                    .expect("should exist")
                    .instance_data
                    .get_mut(&instance_id)
                    .expect("should exist");
                let instance_payload = instance_data.spill(epoch);
                assert!(!instance_payload.is_empty());
                payload.insert(instance_id, instance_payload);
                spilled_table_ids.insert(table_id);

                // update the spill info
                if let Some((new_spill_epoch, size)) = instance_data.spillable_data_info() {
                    let info = epoch_info.entry(new_spill_epoch).or_default();
                    assert!(info.instance_ids.insert(instance_id));
                    info.payload_size += size;
                }
            }
            Some((epoch, payload, spilled_table_ids))
        } else {
            None
        }
    }

    pub(super) fn unsync_data(&mut self) -> &mut UnsyncData {
        self.unsync_data
    }
}

#[cfg(test)]
mod tests {
    // TODO: add more tests on the spill policy
}
