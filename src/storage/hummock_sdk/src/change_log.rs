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

use risingwave_common::buffer::Bitmap;
use risingwave_pb::hummock::{
    PbChangeLogShard, PbEpochNewChangeLog, PbTableChangeLog, SstableInfo,
};

use crate::key::{vnode, TableKeyRange};

#[derive(Debug, Clone, PartialEq)]
pub struct ChangeLogShard {
    pub new_value: Vec<SstableInfo>,
    pub old_value: Vec<SstableInfo>,
    pub vnode_bitmap: Bitmap,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EpochNewChangeLog {
    pub epochs: Vec<u64>,
    pub shards: Vec<ChangeLogShard>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableChangeLog(pub Vec<EpochNewChangeLog>);

impl TableChangeLog {
    pub fn filter(
        &self,
        (min_epoch, max_epoch): (u64, u64),
        key_range: &TableKeyRange,
    ) -> (Vec<SstableInfo>, Vec<SstableInfo>) {
        let mut new_value_sst = Vec::new();
        let mut old_value_sst = Vec::new();
        let vnode = vnode(key_range);
        for epoch_change_log in &self.0 {
            if epoch_change_log.epochs.last().expect("non-empty") < &min_epoch {
                continue;
            }
            if epoch_change_log.epochs.first().expect("non-empty") > &max_epoch {
                break;
            }
            for shard in &epoch_change_log.shards {
                if shard.vnode_bitmap.is_set(vnode.to_index()) {
                    new_value_sst.extend_from_slice(shard.new_value.as_slice());
                    old_value_sst.extend_from_slice(shard.old_value.as_slice());
                }
            }
        }
        (new_value_sst, old_value_sst)
    }
}

impl TableChangeLog {
    pub fn to_protobuf(&self) -> PbTableChangeLog {
        PbTableChangeLog {
            change_logs: self
                .0
                .iter()
                .map(|epoch_new_log| PbEpochNewChangeLog {
                    epochs: epoch_new_log.epochs.clone(),
                    shards: epoch_new_log
                        .shards
                        .iter()
                        .map(|shard| PbChangeLogShard {
                            new_value: shard.new_value.clone(),
                            old_value: shard.old_value.clone(),
                            vnode_bitmap: Some(shard.vnode_bitmap.to_protobuf()),
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    pub fn from_protobuf(val: &PbTableChangeLog) -> Self {
        Self(
            val.change_logs
                .iter()
                .map(|epoch_new_log| EpochNewChangeLog {
                    epochs: epoch_new_log.epochs.clone(),
                    shards: epoch_new_log
                        .shards
                        .iter()
                        .map(|shard| ChangeLogShard {
                            new_value: shard.new_value.clone(),
                            old_value: shard.old_value.clone(),
                            vnode_bitmap: Bitmap::from(shard.vnode_bitmap.as_ref().unwrap()),
                        })
                        .collect(),
                })
                .collect(),
        )
    }
}
