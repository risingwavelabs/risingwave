// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, LinkedList};
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_hummock_sdk::VersionedComparator;

use crate::hummock::iterator::{
    BoxedHummockIterator, DirectionEnum, HummockIterator, HummockIteratorDirection,
};
use crate::hummock::value::HummockValue;
use crate::hummock::HummockResult;
use crate::monitor::StateStoreMetrics;

pub struct Node<'a, D: HummockIteratorDirection, T: Send + Sync> {
    iter: BoxedHummockIterator<'a, D>,
    extra_info: T,
}

impl<T: Eq + Send + Sync, D: HummockIteratorDirection> PartialEq for Node<'_, D, T> {
    fn eq(&self, other: &Self) -> bool {
        self.iter.key() == other.iter.key() && self.extra_info.eq(&other.extra_info)
    }
}

impl<T: Eq + Send + Sync, D: HummockIteratorDirection> Eq for Node<'_, D, T> {}

impl<T: Ord + Send + Sync, D: HummockIteratorDirection> PartialOrd for Node<'_, D, T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T: Ord + Send + Sync, D: HummockIteratorDirection> Ord for Node<'_, D, T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Note: to implement min-heap by using max-heap internally, the comparing
        // order should be reversed.
        //
        // The `extra_info` is used as a tie-breaker when the keys are equal.

        match D::direction() {
            DirectionEnum::Forward => {
                VersionedComparator::compare_key(other.iter.key(), self.iter.key())
                    .then_with(|| other.extra_info.cmp(&self.extra_info))
            }
            DirectionEnum::Backward => {
                VersionedComparator::compare_key(self.iter.key(), other.iter.key())
                    .then_with(|| self.extra_info.cmp(&other.extra_info))
            }
        }
    }
}

/// Iterates on multiple iterators, a.k.a. `MergeIterator`.
pub struct MergeIteratorInner<'a, D: HummockIteratorDirection, NE: Send + Sync> {
    /// Invalid or non-initialized iterators.
    unused_iters: LinkedList<Node<'a, D, NE>>,

    /// The heap for merge sort.
    heap: BinaryHeap<Node<'a, D, NE>>,

    /// Statistics.
    stats: Arc<StateStoreMetrics>,
}

impl<'a, D: HummockIteratorDirection, NE: Send + Sync + Ord> MergeIteratorInner<'a, D, NE> {
    /// Moves all iterators from the `heap` to the linked list.
    fn reset_heap(&mut self) {
        self.unused_iters.extend(self.heap.drain());
    }

    /// After some iterators in `unused_iterators` are sought or rewound, calls this function
    /// to construct a new heap using the valid ones.
    fn build_heap(&mut self) {
        assert!(self.heap.is_empty());

        self.heap = self
            .unused_iters
            .drain_filter(|i| i.iter.is_valid())
            .collect();
    }
}

#[async_trait]
/// The behaviour of `next` of order aware merge iterator is different from the normal one, so we
/// extract this trait.
trait MergeIteratorNext<'a> {
    async fn next_inner(&mut self) -> HummockResult<()>;
}

/// An order aware merge iterator. `usize` as the `extra_info` is used to define the order of
/// iterators.
pub type OrderedMergeIteratorInner<'a, D> = MergeIteratorInner<'a, D, usize>;

impl<'a, D: HummockIteratorDirection> OrderedMergeIteratorInner<'a, D> {
    #[allow(dead_code)]
    pub fn new(
        iterators: impl IntoIterator<Item = BoxedHummockIterator<'a, D>>,
        stats: Arc<StateStoreMetrics>,
    ) -> Self {
        Self {
            unused_iters: iterators
                .into_iter()
                .enumerate()
                .map(|(i, iter)| Node {
                    iter,
                    extra_info: i,
                })
                .collect(),
            heap: BinaryHeap::new(),
            stats,
        }
    }
}

#[async_trait]
impl<'a, D: HummockIteratorDirection> MergeIteratorNext<'a> for OrderedMergeIteratorInner<'a, D> {
    async fn next_inner(&mut self) -> HummockResult<()> {
        let top_node = self.heap.pop().expect("no inner iter");
        let mut popped_nodes = vec![];

        // Take all nodes with the same current key as the top_node out of the heap.
        while !self.heap.is_empty() {
            // unwrap is safe because we have checked the heap is not empty.
            let next_node = self.heap.peek_mut().unwrap();

            match VersionedComparator::compare_key(top_node.iter.key(), next_node.iter.key()) {
                Ordering::Equal => {
                    popped_nodes.push(PeekMut::pop(next_node));
                }
                _ => break,
            }
        }

        popped_nodes.push(top_node);

        // WARNING: within scope of BinaryHeap::PeekMut, we must carefully handle all places of
        // return. Once the iterator enters an invalid state, we should remove it from heap
        // before returning.

        // Put the popped nodes back to the heap if valid or unused_iters if invalid.
        for mut node in popped_nodes {
            match node.iter.next().await {
                Ok(_) => {}
                Err(e) => {
                    // If the iterator returns error, we should clear the heap, so that this
                    // iterator becomes invalid.
                    self.heap.clear();
                    return Err(e);
                }
            }

            if !node.iter.is_valid() {
                self.unused_iters.push_back(node);
            } else {
                self.heap.push(node);
            }
        }

        Ok(())
    }
}

pub type UnorderedMergeIteratorInner<'a, D> = MergeIteratorInner<'a, D, ()>;

impl<'a, D: HummockIteratorDirection> UnorderedMergeIteratorInner<'a, D> {
    pub fn new(
        iterators: impl IntoIterator<Item = BoxedHummockIterator<'a, D>>,
        stats: Arc<StateStoreMetrics>,
    ) -> Self {
        Self {
            unused_iters: iterators
                .into_iter()
                .map(|iter| Node {
                    iter,
                    extra_info: (),
                })
                .collect(),
            heap: BinaryHeap::new(),
            stats,
        }
    }
}

#[async_trait]
impl<'a, D: HummockIteratorDirection> MergeIteratorNext<'a> for UnorderedMergeIteratorInner<'a, D> {
    async fn next_inner(&mut self) -> HummockResult<()> {
        let mut node = self.heap.peek_mut().expect("no inner iter");

        // WARNING: within scope of BinaryHeap::PeekMut, we must carefully handle all places of
        // return. Once the iterator enters an invalid state, we should remove it from heap
        // before returning.

        match node.iter.next().await {
            Ok(_) => {}
            Err(e) => {
                // If the iterator returns error, we should clear the heap, so that this iterator
                // becomes invalid.
                PeekMut::pop(node);
                self.heap.clear();
                return Err(e);
            }
        }

        if !node.iter.is_valid() {
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

#[async_trait]
impl<'a, D: HummockIteratorDirection, NE: Send + Sync + Ord> HummockIterator
    for MergeIteratorInner<'a, D, NE>
where
    Self: MergeIteratorNext<'a>,
{
    type Direction = D;

    async fn next(&mut self) -> HummockResult<()> {
        self.next_inner().await
    }

    fn key(&self) -> &[u8] {
        self.heap.peek().expect("no inner iter").iter.key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.heap.peek().expect("no inner iter").iter.value()
    }

    fn is_valid(&self) -> bool {
        self.heap.peek().map_or(false, |n| n.iter.is_valid())
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.reset_heap();
        futures::future::try_join_all(self.unused_iters.iter_mut().map(|x| x.iter.rewind()))
            .await?;
        self.build_heap();
        Ok(())
    }

    async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        let timer = self.stats.iter_merge_seek_duration.start_timer();

        self.reset_heap();
        futures::future::try_join_all(self.unused_iters.iter_mut().map(|x| x.iter.seek(key)))
            .await?;
        self.build_heap();

        timer.observe_duration();
        Ok(())
    }
}
