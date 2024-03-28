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

use std::collections::HashMap;
use std::mem::size_of;

use prost::Message;
use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::hummock_version::PbLevels;
use risingwave_pb::hummock::hummock_version_delta::PbGroupDeltas;
use risingwave_pb::hummock::{PbHummockVersion, PbHummockVersionDelta};

use crate::change_log::TableChangeLog;
use crate::table_watermark::TableWatermarks;
use crate::{CompactionGroupId, HummockSstableObjectId};

#[derive(Debug, Clone, PartialEq)]
pub struct HummockVersion {
    pub id: u64,
    pub levels: HashMap<CompactionGroupId, PbLevels>,
    pub max_committed_epoch: u64,
    pub safe_epoch: u64,
    pub table_watermarks: HashMap<TableId, TableWatermarks>,
    pub table_change_log: HashMap<TableId, TableChangeLog>,
}

impl Default for HummockVersion {
    fn default() -> Self {
        HummockVersion::from_protobuf_inner(&PbHummockVersion::default())
    }
}

impl HummockVersion {
    /// Convert the `PbHummockVersion` received from rpc to `HummockVersion`. No need to
    /// maintain backward compatibility.
    pub fn from_rpc_protobuf(pb_version: &PbHummockVersion) -> Self {
        Self::from_protobuf_inner(pb_version)
    }

    /// Convert the `PbHummockVersion` deserialized from persisted state to `HummockVersion`.
    /// We should maintain backward compatibility.
    pub fn from_persisted_protobuf(pb_version: &PbHummockVersion) -> Self {
        Self::from_protobuf_inner(pb_version)
    }

    fn from_protobuf_inner(pb_version: &PbHummockVersion) -> Self {
        Self {
            id: pb_version.id,
            levels: pb_version
                .levels
                .iter()
                .map(|(group_id, levels)| (*group_id as CompactionGroupId, levels.clone()))
                .collect(),
            max_committed_epoch: pb_version.max_committed_epoch,
            safe_epoch: pb_version.safe_epoch,
            table_watermarks: pb_version
                .table_watermarks
                .iter()
                .map(|(table_id, table_watermark)| {
                    (
                        TableId::new(*table_id),
                        TableWatermarks::from_protobuf(table_watermark),
                    )
                })
                .collect(),
            table_change_log: pb_version
                .table_change_logs
                .iter()
                .map(|(table_id, change_log)| {
                    (
                        TableId::new(*table_id),
                        TableChangeLog::from_protobuf(change_log),
                    )
                })
                .collect(),
        }
    }

    pub fn to_protobuf(&self) -> PbHummockVersion {
        PbHummockVersion {
            id: self.id,
            levels: self
                .levels
                .iter()
                .map(|(group_id, levels)| (*group_id as _, levels.clone()))
                .collect(),
            max_committed_epoch: self.max_committed_epoch,
            safe_epoch: self.safe_epoch,
            table_watermarks: self
                .table_watermarks
                .iter()
                .map(|(table_id, watermark)| (table_id.table_id, watermark.to_protobuf()))
                .collect(),
            table_change_logs: self
                .table_change_log
                .iter()
                .map(|(table_id, change_log)| (table_id.table_id, change_log.to_protobuf()))
                .collect(),
        }
    }

    pub fn estimated_encode_len(&self) -> usize {
        self.levels.len() * size_of::<CompactionGroupId>()
            + self
                .levels
                .values()
                .map(|level| level.encoded_len())
                .sum::<usize>()
            + self.table_watermarks.len() * size_of::<u32>()
            + self
                .table_watermarks
                .values()
                .map(|table_watermark| table_watermark.estimated_encode_len())
                .sum::<usize>()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct HummockVersionDelta {
    pub id: u64,
    pub prev_id: u64,
    pub group_deltas: HashMap<CompactionGroupId, PbGroupDeltas>,
    pub max_committed_epoch: u64,
    pub safe_epoch: u64,
    pub trivial_move: bool,
    pub gc_object_ids: Vec<HummockSstableObjectId>,
    pub new_table_watermarks: HashMap<TableId, TableWatermarks>,
    pub removed_table_ids: Vec<TableId>,
}

impl Default for HummockVersionDelta {
    fn default() -> Self {
        HummockVersionDelta::from_protobuf_inner(&PbHummockVersionDelta::default())
    }
}

impl HummockVersionDelta {
    /// Convert the `PbHummockVersionDelta` deserialized from persisted state to `HummockVersionDelta`.
    /// We should maintain backward compatibility.
    pub fn from_persisted_protobuf(delta: &PbHummockVersionDelta) -> Self {
        Self::from_protobuf_inner(delta)
    }

    /// Convert the `PbHummockVersionDelta` received from rpc to `HummockVersionDelta`. No need to
    /// maintain backward compatibility.
    pub fn from_rpc_protobuf(delta: &PbHummockVersionDelta) -> Self {
        Self::from_protobuf_inner(delta)
    }

    fn from_protobuf_inner(delta: &PbHummockVersionDelta) -> Self {
        Self {
            id: delta.id,
            prev_id: delta.prev_id,
            group_deltas: delta.group_deltas.clone(),
            max_committed_epoch: delta.max_committed_epoch,
            safe_epoch: delta.safe_epoch,
            trivial_move: delta.trivial_move,
            gc_object_ids: delta.gc_object_ids.clone(),
            new_table_watermarks: delta
                .new_table_watermarks
                .iter()
                .map(|(table_id, watermarks)| {
                    (
                        TableId::new(*table_id),
                        TableWatermarks::from_protobuf(watermarks),
                    )
                })
                .collect(),
            removed_table_ids: delta
                .removed_table_ids
                .iter()
                .map(|table_id| TableId::new(*table_id))
                .collect(),
        }
    }

    pub fn to_protobuf(&self) -> PbHummockVersionDelta {
        PbHummockVersionDelta {
            id: self.id,
            prev_id: self.prev_id,
            group_deltas: self.group_deltas.clone(),
            max_committed_epoch: self.max_committed_epoch,
            safe_epoch: self.safe_epoch,
            trivial_move: self.trivial_move,
            gc_object_ids: self.gc_object_ids.clone(),
            new_table_watermarks: self
                .new_table_watermarks
                .iter()
                .map(|(table_id, watermarks)| (table_id.table_id, watermarks.to_protobuf()))
                .collect(),
            removed_table_ids: self
                .removed_table_ids
                .iter()
                .map(|table_id| table_id.table_id)
                .collect(),
        }
    }
}
