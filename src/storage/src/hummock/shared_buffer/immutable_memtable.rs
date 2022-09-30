use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use risingwave_hummock_sdk::{key, CompactionGroupId, VersionedComparator};

use crate::hummock::iterator::{Forward, HummockIterator, HummockIteratorDirection};
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatchInner, SharedBufferBatchIterator,
};
use crate::hummock::value::HummockValue;
use crate::hummock::HummockResult;

const MIN_MERGE_BATCH_LIMIT: usize = 8 * 1024 * 1024;
const MAX_MERGE_WRITE_AMPLIFICATION: usize = 8;

pub struct ImmutableMemtable {
    data: Arc<SharedBufferBatchInner>,
    compaction_group_id: CompactionGroupId,
}

impl ImmutableMemtable {
    fn new(data: Arc<SharedBufferBatchInner>, compaction_group_id: CompactionGroupId) -> Self {
        Self {
            data,
            compaction_group_id,
        }
    }

    pub fn iter<D: HummockIteratorDirection>(&self) -> SharedBufferBatchIterator<D> {
        SharedBufferBatchIterator::<D>::new(self.data.clone())
    }

    pub fn contains_sst(&self, sst_id: u64) -> bool {
        self.ssts.contains(&sst_id)
    }

    pub fn get_ssts(&self) -> HashSet<u64> {
        self.ssts.clone()
    }

    pub fn compaction_group_id(&self) -> u64 {
        self.compaction_group_id
    }

    pub fn can_merge(&self, new_batch_size: usize) -> bool {
        if self.data.size() > MIN_MERGE_BATCH_LIMIT
            && self.data.size() > new_batch_size * MAX_MERGE_WRITE_AMPLIFICATION
        {
            return false;
        }
        true
    }

    pub fn start_key(&self) -> &[u8] {
        &self.data.first().unwrap().0
    }

    pub fn end_key(&self) -> &[u8] {
        &self.data.last().unwrap().0
    }

    pub fn start_user_key(&self) -> &[u8] {
        key::user_key(&self.data.first().unwrap().0)
    }

    pub fn end_user_key(&self) -> &[u8] {
        key::user_key(&self.data.last().unwrap().0)
    }

    pub fn build(
        mut iters: Vec<SharedBufferBatchIterator<Forward>>,
        key_count: usize,
        compaction_group_id: u64,
    ) -> Self {
        let mut data = Vec::with_capacity(key_count);
        let mut data_size = 0;
        let mut heap = BinaryHeap::new();
        for mut iter in iters {
            iter.blocking_rewind();
            heap.push(Node { iter });
        }

        while !heap.is_empty() {
            let mut node = heap.peek_mut().expect("no inner iter");
            // WARNING: within scope of BinaryHeap::PeekMut, we must carefully handle all places of
            // return. Once the iterator enters an invalid state, we should remove it from heap
            // before returning.
            data.push(node.iter.current_item().clone());

            node.iter.blocking_next();
            if !node.iter.is_valid() {
                // Put back to `unused_iters`
                heap.pop();
            } else {
                // This will update the heap top.
                drop(node);
            }
        }

        ImmutableMemtable::new(
            Arc::new(SharedBufferBatchInner::new(data, data_size, None)),
            compaction_group_id,
        )
    }

    pub fn get(&self, internal_key: &[u8]) -> Option<HummockValue<Bytes>> {
        // Perform binary search on user key because the items in SharedBufferBatch is ordered by
        // user key.
        let idx = match self
            .data
            .binary_search_by(|m| VersionedComparator::compare_key(&m.0, internal_key))
        {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        if idx < self.data.len() && key::user_key(&self.data[idx].0).eq(key::user_key(internal_key))
        {
            return Some(self.data[idx].1.clone());
        }
        None
    }

    pub fn merge(batches: Vec<ImmutableMemtable>) -> Self {
        let mut key_count = 0;
        let compaction_group_id = batches[0].compaction_group_id;
        let mut iters = vec![];
        for batch in batches {
            key_count += batch.data.key_count();
            iters.push(batch.iter::<Forward>());
        }
        return Self::build(iters, key_count, compaction_group_id);
    }
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
