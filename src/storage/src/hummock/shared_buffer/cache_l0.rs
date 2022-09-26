use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;
use bytes::Bytes;
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch, key, VersionedComparator};
use risingwave_pb::hummock::SstableInfo;
use crate::hummock::HummockResult;
use crate::hummock::iterator::{Forward, HummockIterator, HummockIteratorDirection};
use crate::hummock::shared_buffer::shared_buffer_batch::{SharedBufferBatch, SharedBufferBatchInner, SharedBufferBatchIterator};
use crate::hummock::value::HummockValue;

const MIN_MERGE_BATCH_LIMIT: usize = 8 * 1024 * 1024;
const MAX_MERGE_WRITE_AMPLIFICATION: usize = 8;

pub struct InMemorySstableData {
    data: Arc<SharedBufferBatchInner>,
    compaction_group_id: CompactionGroupId,
    ssts: HashSet<u64>,
}

impl InMemorySstableData {
    pub fn new(data: Arc<SharedBufferBatchInner>, compaction_group_id: CompactionGroupId, mut sst_ids: Vec<u64>) -> Self {
        sst_ids.sort();
        sst_ids.dedup();
       Self {
           data,
           compaction_group_id,
           ssts: sst_ids.into_iter().collect(),
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
        if self.data.size() > MIN_MERGE_BATCH_LIMIT && self.data.size() > new_batch_size * MAX_MERGE_WRITE_AMPLIFICATION {
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

    pub fn to_batch(&self) -> SharedBufferBatch {
        SharedBufferBatch::from_inner(self.data.clone(), 0,
            self.compaction_group_id)
    }

    pub async fn build<I: HummockIterator<Direction=Forward>>(mut iter: I, key_count: usize, compaction_group_id: u64, sst_ids: Vec<u64>) -> HummockResult<Self> {
        let mut data = Vec::with_capacity(key_count);
        let mut data_size = 0;
        iter.rewind().await?;
        while iter.is_valid() {
            let value = iter.value().to_bytes();
            data_size += iter.key().len();
            if let HummockValue::Put(t) = &value {
                data_size += t.len();
            }
            data.push((Bytes::copy_from_slice(iter.key()), value));
            iter.next().await?;
        }
        Ok(InMemorySstableData::new(
                Arc::new(SharedBufferBatchInner::new(data, data_size, None)),
                compaction_group_id,
                sst_ids
            ))
    }

    pub fn merge(batches: Vec<InMemorySstableData>) -> Self {
        let mut heap = BinaryHeap::with_capacity(batches.len());
        let mut key_count = 0;
        let mut data_size = 0;
        let mut ssts = vec![];
        let compaction_group_id = batches[0].compaction_group_id;
        for batch in batches {
            key_count += batch.data.key_count();
            data_size += batch.data.size();
            ssts.extend(batch.ssts.iter());
            let mut iter = batch.iter::<Forward>();
            iter.rewind_inner();
            if iter.is_valid() {
                heap.push(Node { iter });
            }
        }

        let mut data = Vec::with_capacity(key_count);
        while heap.peek().map_or(false, |n| n.iter.is_valid()) {
            let mut node = heap.peek_mut().expect("no inner iter");
            data.push(node.iter.current_item().clone());
            node.iter.next_inner();
            if !node.iter.is_valid() {
                // Put back to `unused_iters`
                PeekMut::pop(node);
            } else {
                // This will update the heap top.
                drop(node);
            }
        }
        InMemorySstableData::new(
            Arc::new(SharedBufferBatchInner::new(data, data_size, None)),
            compaction_group_id,
            ssts
        )
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
impl PartialOrd<Node> for Node  {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Note: to implement min-heap by using max-heap internally, the comparing
        // order should be reversed.

        Some(VersionedComparator::compare_key(other.iter.key(), self.iter.key()))
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.iter.key() == other.iter.key()
    }
}

impl Eq for Node where Self: PartialEq {}
