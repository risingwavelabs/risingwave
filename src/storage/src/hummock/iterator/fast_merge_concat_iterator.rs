use std::cmp::Ordering;
use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, LinkedList};
use std::future::Future;
use std::sync::Arc;

use risingwave_hummock_sdk::VersionedComparator;
use risingwave_pb::hummock::SstableInfo;

use crate::hummock::iterator::merge_inner::MergeIteratorNext;
use crate::hummock::iterator::{HummockIterator, ReadOptions};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockResult, SSTableIterator};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};

struct ConcatSstableIterator {
    /// The iterator of the current table.
    sstable_iter: Option<SSTableIterator>,

    /// Current table index.
    cur_idx: usize,

    /// All non-overlapping tables.
    tables: Vec<SstableInfo>,

    sstable_store: SstableStoreRef,

    stats: StoreLocalStatistic,
    read_options: Arc<ReadOptions>,
}

impl ConcatSstableIterator {
    /// Caller should make sure that `tables` are non-overlapping,
    /// arranged in ascending order when it serves as a forward iterator,
    /// and arranged in descending order when it serves as a backward iterator.
    fn new(
        tables: Vec<SstableInfo>,
        sstable_store: SstableStoreRef,
        read_options: Arc<ReadOptions>,
    ) -> Self {
        Self {
            sstable_iter: None,
            cur_idx: 0,
            tables,
            sstable_store,
            stats: StoreLocalStatistic::default(),
            read_options,
        }
    }

    /// Seeks to a table, and then seeks to the key if `seek_key` is given.
    async fn seek_idx(&mut self, idx: usize, seek_key: Option<&[u8]>) -> HummockResult<()> {
        if idx >= self.tables.len() {
            if let Some(old_iter) = self.sstable_iter.take() {
                old_iter.collect_local_statistic(&mut self.stats);
            }
        } else {
            let table = if self.read_options.prefetch {
                self.sstable_store
                    .load_table(self.tables[idx].id, true, &mut self.stats)
                    .await?
            } else {
                self.sstable_store
                    .sstable(self.tables[idx].id, &mut self.stats)
                    .await?
            };
            let mut sstable_iter =
                SSTableIterator::new(table, self.sstable_store.clone(), self.read_options.clone());

            if let Some(key) = seek_key {
                sstable_iter.seek_inner(key).await?;
            } else {
                sstable_iter.rewind().await?;
            }

            if let Some(old_iter) = self.sstable_iter.take() {
                old_iter.collect_local_statistic(&mut self.stats);
            }

            self.sstable_iter = Some(sstable_iter);
            self.cur_idx = idx;
        }
        Ok(())
    }

    async fn next(&mut self) -> HummockResult<()> {
        let sstable_iter = self.sstable_iter.as_mut().expect("no table iter");
        sstable_iter.next_inner().await?;

        if sstable_iter.is_valid() {
            Ok(())
        } else {
            // seek to next table
            self.seek_idx(self.cur_idx + 1, None).await
        }
    }

    fn key(&self) -> &[u8] {
        self.sstable_iter.as_ref().expect("no table iter").key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.sstable_iter.as_ref().expect("no table iter").value()
    }

    fn is_valid(&self) -> bool {
        self.sstable_iter.as_ref().map_or(false, |i| i.is_valid())
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.seek_idx(0, None).await
    }

    async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        let table_idx = self
            .tables
            .partition_point(|table| {
                let ord =
                    VersionedComparator::compare_key(&table.key_range.as_ref().unwrap().left, key);
                ord == Ordering::Less || ord == Ordering::Equal
            })
            .saturating_sub(1); // considering the boundary of 0

        self.seek_idx(table_idx, Some(key)).await?;
        if !self.is_valid() {
            // Seek to next table
            self.seek_idx(table_idx + 1, None).await?;
        }
        Ok(())
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        stats.add(&self.stats)
    }
}

impl Eq for Box<ConcatSstableIterator> where Self: PartialEq {}
impl Ord for Box<ConcatSstableIterator>
where
    Self: PartialOrd,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Implement `PartialOrd` for unordered iter node. Only compare the key.
impl PartialOrd for Box<ConcatSstableIterator> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Note: to implement min-heap by using max-heap internally, the comparing
        // order should be reversed.
        Some(VersionedComparator::compare_key(other.key(), self.key()))
    }
}

impl PartialEq for Box<ConcatSstableIterator> {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

pub struct FastMergeConcatIterator {
    /// Invalid or non-initialized iterators.
    unused_iters: LinkedList<Box<ConcatSstableIterator>>,

    /// The heap for merge sort.
    heap: BinaryHeap<Box<ConcatSstableIterator>>,

    /// Statistics.
    stats: Arc<StateStoreMetrics>,
}

impl FastMergeConcatIterator {
    pub fn new(
        tables: Vec<Vec<SstableInfo>>,
        sstable_store: SstableStoreRef,
        read_options: Arc<ReadOptions>,
        stats: Arc<StateStoreMetrics>,
    ) -> Self {
        let mut unused_iters = LinkedList::default();
        for ssts in tables {
            let iter = Box::new(ConcatSstableIterator::new(
                ssts,
                sstable_store.clone(),
                read_options.clone(),
            ));
            unused_iters.push_back(iter);
        }
        Self {
            unused_iters,
            heap: BinaryHeap::new(),
            stats,
        }
    }
}

impl FastMergeConcatIterator {
    /// Moves all iterators from the `heap` to the linked list.
    fn reset_heap(&mut self) {
        self.unused_iters.extend(self.heap.drain());
    }

    /// After some iterators in `unused_iterators` are sought or rewound, calls this function
    /// to construct a new heap using the valid ones.
    fn build_heap(&mut self) {
        assert!(self.heap.is_empty());

        self.heap = self.unused_iters.drain_filter(|i| i.is_valid()).collect();
    }

    pub fn key(&self) -> &[u8] {
        self.heap.peek().expect("no inner iter").key()
    }

    pub fn value(&self) -> HummockValue<&[u8]> {
        self.heap.peek().expect("no inner iter").value()
    }

    pub fn is_valid(&self) -> bool {
        self.heap.peek().map_or(false, |n| n.is_valid())
    }

    pub async fn rewind(&mut self) -> HummockResult<()> {
        self.reset_heap();
        futures::future::try_join_all(self.unused_iters.iter_mut().map(|x| x.rewind())).await?;
        self.build_heap();
        Ok(())
    }

    pub async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        self.reset_heap();
        futures::future::try_join_all(self.unused_iters.iter_mut().map(|x| x.seek(key))).await?;
        self.build_heap();
        Ok(())
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        for node in &self.heap {
            node.collect_local_statistic(stats);
        }
        for node in &self.unused_iters {
            node.collect_local_statistic(stats);
        }
    }
}

impl Drop for FastMergeConcatIterator {
    fn drop(&mut self) {
        let mut stats = StoreLocalStatistic::default();
        self.collect_local_statistic(&mut stats);
        stats.report(self.stats.as_ref());
    }
}

impl MergeIteratorNext for FastMergeConcatIterator {
    type HummockResultFuture<'a> = impl Future<Output = HummockResult<()>>;

    fn next_inner(&mut self) -> Self::HummockResultFuture<'_> {
        async {
            let mut node = self.heap.peek_mut().expect("no inner iter");

            // WARNING: within scope of BinaryHeap::PeekMut, we must carefully handle all places of
            // return. Once the iterator enters an invalid state, we should remove it from heap
            // before returning.

            match node.next().await {
                Ok(_) => {}
                Err(e) => {
                    // If the iterator returns error, we should clear the heap, so that this
                    // iterator becomes invalid.
                    PeekMut::pop(node);
                    self.heap.clear();
                    return Err(e);
                }
            }

            if !node.is_valid() {
                // Put back to `unused_iters`
                let node = PeekMut::pop(node);
                self.unused_iters.push_back(node);
            } else {
                // This will update the heap top.
                drop(node);
            }

            Ok(())
        }
    }

    fn key_inner(&self) -> &[u8] {
        self.heap.peek().expect("no inner iter").key()
    }

    fn value_inner(&self) -> HummockValue<&[u8]> {
        self.heap.peek().expect("no inner iter").value()
    }

    fn is_valid_inner(&self) -> bool {
        self.heap.peek().map_or(false, |n| n.is_valid())
    }
}
