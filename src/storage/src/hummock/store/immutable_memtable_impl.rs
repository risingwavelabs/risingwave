use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::TableKey;
use risingwave_hummock_sdk::HummockEpoch;

use crate::hummock::iterator::RangeIteratorTyped;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatchId;
use crate::hummock::store::immutable_memtable::MergedImmutableMemtable;
use crate::hummock::store::memtable::ImmutableMemtable;
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
