// Copyright 2023 RisingWave Labs
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

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{TableKey, TableKeyRange};
use risingwave_hummock_sdk::HummockEpoch;

use crate::hummock::iterator::RangeIteratorTyped;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatchId;
use crate::hummock::store::immutable_memtable::MergedImmutableMemtable;
use crate::hummock::store::memtable::{ImmId, ImmutableMemtable};
use crate::hummock::value::HummockValue;
use crate::monitor::StoreLocalStatistic;

/// Abstraction of the immutable memtable used in the read path of `HummockReadVersion`.
/// Only provide limited interfaces needed in the read path.
#[derive(Clone, Debug, PartialEq)]
pub enum ImmutableMemtableImpl {
    Imm(ImmutableMemtable),
    MergedImm(MergedImmutableMemtable),
}

impl ImmutableMemtableImpl {
    pub fn range_exists(&self, table_key_range: &TableKeyRange) -> bool {
        match self {
            ImmutableMemtableImpl::Imm(batch) => batch.range_exists(table_key_range),
            ImmutableMemtableImpl::MergedImm(m) => m.range_exists(table_key_range),
        }
    }

    pub fn start_table_key(&self) -> TableKey<&[u8]> {
        match self {
            ImmutableMemtableImpl::Imm(batch) => batch.start_table_key(),
            ImmutableMemtableImpl::MergedImm(m) => m.start_table_key(),
        }
    }

    pub fn end_table_key(&self) -> TableKey<&[u8]> {
        match self {
            ImmutableMemtableImpl::Imm(batch) => batch.end_table_key(),
            ImmutableMemtableImpl::MergedImm(m) => m.end_table_key(),
        }
    }

    pub fn table_id(&self) -> TableId {
        match self {
            ImmutableMemtableImpl::Imm(batch) => batch.table_id(),
            ImmutableMemtableImpl::MergedImm(m) => m.table_id(),
        }
    }

    /// For merged imm, the epoch will be the minimum epoch of all the merged imms
    pub fn epoch(&self) -> u64 {
        match self {
            ImmutableMemtableImpl::Imm(batch) => batch.epoch(),
            ImmutableMemtableImpl::MergedImm(m) => m.epoch(),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            ImmutableMemtableImpl::Imm(batch) => batch.size(),
            ImmutableMemtableImpl::MergedImm(m) => m.size(),
        }
    }

    pub fn batch_id(&self) -> SharedBufferBatchId {
        match self {
            ImmutableMemtableImpl::Imm(batch) => batch.batch_id(),
            ImmutableMemtableImpl::MergedImm(m) => m.batch_id(),
        }
    }

    pub fn imm_ids(&self) -> Vec<ImmId> {
        match self {
            ImmutableMemtableImpl::Imm(batch) => vec![batch.batch_id()],
            ImmutableMemtableImpl::MergedImm(m) => m.get_merged_imm_ids().clone(),
        }
    }

    pub fn delete_range_iter(&self) -> RangeIteratorTyped {
        match self {
            ImmutableMemtableImpl::Imm(batch) => {
                RangeIteratorTyped::Batch(batch.delete_range_iter())
            }
            ImmutableMemtableImpl::MergedImm(m) => {
                RangeIteratorTyped::MergedImm(m.delete_range_iter())
            }
        }
    }

    pub fn has_range_tombstone(&self) -> bool {
        match self {
            ImmutableMemtableImpl::Imm(batch) => batch.has_range_tombstone(),
            ImmutableMemtableImpl::MergedImm(m) => m.has_range_tombstone(),
        }
    }
}

/// Get `user_value` from `ImmutableMemtableImpl`
pub fn get_from_imm(
    imm: &ImmutableMemtableImpl,
    table_key: TableKey<&[u8]>,
    read_epoch: HummockEpoch,
    local_stats: &mut StoreLocalStatistic,
) -> Option<HummockValue<Bytes>> {
    match imm {
        ImmutableMemtableImpl::Imm(imm) => {
            if imm.check_delete_by_range(table_key) {
                return Some(HummockValue::Delete);
            }
            imm.get(table_key).map(|v| {
                local_stats.get_shared_buffer_hit_counts += 1;
                v
            })
        }
        ImmutableMemtableImpl::MergedImm(merged_imm) => {
            merged_imm.get_from_merged_imm(table_key, read_epoch)
        }
    }
}
