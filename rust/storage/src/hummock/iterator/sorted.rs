use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, LinkedList};

use async_trait::async_trait;

use super::{BoxedHummockIterator, HummockIterator};
use crate::hummock::value::HummockValue;
use crate::hummock::version_cmp::VersionedComparator;
use crate::hummock::HummockResult;

/// Used as node of the min-heap for merge-sorting the key-value pairs from iterators.
///
/// Since the iterators are ordered by their first keys by calling [`HummockIterator::key`], all of
/// them must be ensured valid or panic occurs.
struct Node(BoxedHummockIterator);

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.0.key() == other.0.key()
    }
}
impl Eq for Node {}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Node {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Note: to implement min-heap by using max-heap internally, the comparing
        // order should be reversed.
        VersionedComparator::compare_key(other.0.key(), self.0.key())
    }
}

/// Iterates on multiple iterators, a.k.a. `MergeIterator`.
pub struct SortedIterator {
    /// Invalid or non-initialized iterators.
    unused_iters: LinkedList<BoxedHummockIterator>,

    /// The heap for merge sort.
    heap: BinaryHeap<Node>,
}

impl SortedIterator {
    pub fn new(iterators: impl IntoIterator<Item = BoxedHummockIterator>) -> Self {
        Self {
            unused_iters: iterators.into_iter().collect(),
            heap: BinaryHeap::new(),
        }
    }

    /// Move all iterators from the `heap` to the linked list.
    fn reset_heap(&mut self) {
        self.unused_iters.extend(self.heap.drain().map(|n| n.0));
    }

    /// After some of the iterators in `unused_iterators` are seeked or rewound, call this function
    /// to construct a new heap using the valid ones.
    fn build_heap(&mut self) {
        assert!(self.heap.is_empty());

        self.heap = self
            .unused_iters
            .drain_filter(|i| i.is_valid())
            .map(Node)
            .collect();
    }
}

#[async_trait]
impl HummockIterator for SortedIterator {
    async fn next(&mut self) -> HummockResult<()> {
        let mut node = self.heap.peek_mut().expect("no inner iter");

        node.0.next().await?;
        if !node.0.is_valid() {
            // put back to `unused_iters`
            let node = PeekMut::pop(node);
            self.unused_iters.push_back(node.0);
        } else {
            // this will update the heap top
            drop(node);
        }

        Ok(())
    }

    fn key(&self) -> &[u8] {
        self.heap.peek().expect("no inner iter").0.key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.heap.peek().expect("no inner iter").0.value()
    }

    fn is_valid(&self) -> bool {
        self.heap.peek().map_or(false, |n| n.0.is_valid())
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.reset_heap();
        futures::future::try_join_all(self.unused_iters.iter_mut().map(|x| x.rewind())).await?;
        self.build_heap();
        Ok(())
    }

    async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        self.reset_heap();
        futures::future::try_join_all(self.unused_iters.iter_mut().map(|x| x.seek(key))).await?;
        self.build_heap();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use itertools::Itertools;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, gen_test_table, iterator_test_key_of, test_key,
        test_value_of, TestIteratorBuilder, TEST_KEYS_COUNT,
    };
    use crate::hummock::iterator::HummockIterator;
    use crate::hummock::table::TableIterator;

    #[tokio::test]
    async fn test_basic() {
        let (iters, validators): (Vec<_>, Vec<_>) = (0..3)
            .map(|iter_id| {
                TestIteratorBuilder::default()
                    .id(0)
                    .map_key(Box::new(move |id, x| {
                        iterator_test_key_of(id, x * 3 + (iter_id as usize) + 1)
                    }))
                    .map_value(Box::new(move |id, x| {
                        test_value_of(id, x * 3 + (iter_id as usize) + 1)
                    }))
                    .finish()
            })
            .unzip();

        let iters: Vec<BoxedHummockIterator> = iters
            .into_iter()
            .map(|x| Box::new(x) as BoxedHummockIterator)
            .collect_vec();

        let mut mi = SortedIterator::new(iters);
        let mut i = 0;
        mi.rewind().await.unwrap();
        while mi.is_valid() {
            let key = mi.key();
            let val = mi.value();
            validators[i % 3].assert_key(i / 3, key);
            validators[i % 3].assert_hummock_value(i / 3, val);
            i += 1;
            mi.next().await.unwrap();
            if i == TEST_KEYS_COUNT * 3 {
                assert!(!mi.is_valid());
                break;
            }
        }
        assert!(i >= TEST_KEYS_COUNT);
    }

    #[tokio::test]
    async fn test_seek() {
        let (iters, validators): (Vec<_>, Vec<_>) = (0..3)
            .map(|iter_id| {
                TestIteratorBuilder::default()
                    .id(0)
                    .total(20)
                    .map_key(Box::new(move |id, x| {
                        iterator_test_key_of(id, x * 3 + (iter_id as usize))
                    }))
                    .finish()
            })
            .unzip();
        let iters: Vec<BoxedHummockIterator> = iters
            .into_iter()
            .map(|x| Box::new(x) as BoxedHummockIterator)
            .collect_vec();

        let mut mi = SortedIterator::new(iters);
        let test_validator = &validators[2];

        // right edge case
        mi.seek(test_key!(test_validator, 3 * TEST_KEYS_COUNT))
            .await
            .unwrap();
        assert!(!mi.is_valid());

        // normal case
        mi.seek(test_key!(test_validator, 4)).await.unwrap();
        let k = mi.key();
        let v = mi.value();
        test_validator.assert_hummock_value(4, v);
        test_validator.assert_key(4, k);

        mi.seek(test_key!(test_validator, 17)).await.unwrap();
        let k = mi.key();
        let v = mi.value();
        test_validator.assert_hummock_value(17, v);
        test_validator.assert_key(17, k);

        // left edge case
        mi.seek(test_key!(test_validator, 0)).await.unwrap();
        let k = mi.key();
        let v = mi.value();
        test_validator.assert_hummock_value(0, v);
        test_validator.assert_key(0, k);
    }

    #[tokio::test]
    async fn test_invalidate_reset() {
        let table0 = gen_test_table(0, default_builder_opt_for_test()).await;
        let table1 = gen_test_table(1, default_builder_opt_for_test()).await;
        let iters: Vec<BoxedHummockIterator> = vec![
            Box::new(TableIterator::new(Arc::new(table0))),
            Box::new(TableIterator::new(Arc::new(table1))),
        ];

        let mut si = SortedIterator::new(iters);

        si.rewind().await.unwrap();
        let mut count = 0;
        while si.is_valid() {
            count += 1;
            si.next().await.unwrap();
        }
        assert_eq!(count, TEST_KEYS_COUNT * 2);

        si.rewind().await.unwrap();
        let mut count = 0;
        while si.is_valid() {
            count += 1;
            si.next().await.unwrap();
        }
        assert_eq!(count, TEST_KEYS_COUNT * 2);
    }
}
