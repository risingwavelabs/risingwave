use std::collections::binary_heap::PeekMut;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch, VersionedComparator};

use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchIterator,
};
use crate::hummock::shared_buffer::SharedBuffer;
use crate::hummock::MemoryLimiter;

pub fn build_shared_batch(
    batches: Arc<BTreeMap<HummockEpoch, SharedBuffer>>,
    limiter: &MemoryLimiter,
) -> Vec<SharedBufferBatch> {
    let mut payload: HashMap<CompactionGroupId, Vec<SharedBufferBatch>> = HashMap::default();
    for buffer in batches.values() {
        if let Some((data, _)) = buffer.to_uncommitted_data() {
            for batch in data {
                let group = payload
                    .entry(batch.compaction_group_id())
                    .or_insert_with(Vec::new);
                group.push(batch);
            }
        }
    }
    payload
        .into_iter()
        .map(|(group_id, data)| build_shared_batch_for_group(data, group_id, limiter))
        .collect_vec()
}

fn build_shared_batch_for_group(
    batches: Vec<SharedBufferBatch>,
    compaction_group_id: u64,
    limiter: &MemoryLimiter,
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
    let batch_size = SharedBufferBatch::measure_batch_size(&data);

    // Because all memory of this batch are copy from Bytes, which means that they only use a few
    // memory for pointer. And after a moment, the memory of shared-buffer would be release.
    let tracker = limiter.must_require_memory(batch_size as u64).unwrap();

    SharedBufferBatch::for_immutable_memtable(
        data,
        min_epoch,
        compaction_group_id,
        table_ids,
        tracker,
        batch_size,
    )
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
