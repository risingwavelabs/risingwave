use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, HashSet};
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::{key, CompactionGroupId, HummockEpoch, VersionedComparator};

use crate::hummock::iterator::{Forward, HummockIterator, HummockIteratorDirection};
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchInner, SharedBufferBatchIterator,
};
use crate::hummock::utils::range_overlap;
use crate::hummock::value::HummockValue;
use crate::hummock::HummockResult;

const MIN_MERGE_BATCH_LIMIT: usize = 8 * 1024 * 1024;
const MAX_MERGE_WRITE_AMPLIFICATION: usize = 8;

#[derive(Clone, Debug, PartialEq)]
pub struct ImmutableMemtable {
    shared_buffer: SharedBufferBatch,
}

pub fn build_shared_batch(
    mut batches: Vec<SharedBufferBatch>,
    compaction_group_id: u64,
) -> SharedBufferBatch {
    let mut key_count = 0;
    let mut heap = BinaryHeap::new();
    let mut min_epoch = HummockEpoch::MAX;
    let mut table_ids = vec![];
    for batch in batches {
        key_count += batch.count();
        min_epoch = std::cmp::min(min_epoch, batch.epoch());
        table_ids.extend_from_slice(batch.get_table_ids());
        let mut iter = batch.into_forward_iter();
        iter.blocking_rewind();
        heap.push(Node { iter });
    }
    let mut data = Vec::with_capacity(key_count);

    while !heap.is_empty() {
        let mut node = heap.peek_mut().expect("no inner iter");
        // WARNING: within scope of BinaryHeap::PeekMut, we must carefully handle all places of
        // return. Once the iterator enters an invalid state, we should remove it from heap
        // before returning.
        data.push(node.iter.current_item().clone());

        node.iter.blocking_next();
        if !node.iter.is_valid() {
            // Put back to `unused_iters`
            PeekMut::pop(node);
        } else {
            // This will update the heap top.
            drop(node);
        }
    }

    SharedBufferBatch::for_immutable_memtable(data, min_epoch, compaction_group_id, table_ids)
}

struct Node {
    iter: SharedBufferBatchIterator<Forward>,
}

impl Ord for Node
where
    Self: PartialOrd,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Implement `PartialOrd` for unordered iter node. Only compare the key.
impl PartialOrd<Node> for Node {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Note: to implement min-heap by using max-heap internally, the comparing
        // order should be reversed.

        Some(VersionedComparator::compare_key(
            other.iter.key(),
            self.iter.key(),
        ))
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.iter.key() == other.iter.key()
    }
}

impl Eq for Node where Self: PartialEq {}
