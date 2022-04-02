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

use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, LinkedList};
use std::sync::Arc;

use async_trait::async_trait;

use super::variants::*;
use crate::hummock::iterator::{BoxedHummockIterator, HummockIterator};
use crate::hummock::value::HummockValue;
use risingwave_common::storage::VersionedComparator;
use crate::hummock::HummockResult;
use crate::monitor::StateStoreMetrics;

pub struct Node<'a, const DIRECTION: usize>(BoxedHummockIterator<'a>);

impl<const DIRECTION: usize> PartialEq for Node<'_, DIRECTION> {
    fn eq(&self, other: &Self) -> bool {
        self.0.key() == other.0.key()
    }
}
impl<const DIRECTION: usize> Eq for Node<'_, DIRECTION> {}

impl<const DIRECTION: usize> PartialOrd for Node<'_, DIRECTION> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<const DIRECTION: usize> Ord for Node<'_, DIRECTION> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Note: to implement min-heap by using max-heap internally, the comparing
        // order should be reversed.
        match DIRECTION {
            FORWARD => VersionedComparator::compare_key(other.0.key(), self.0.key()),
            BACKWARD => VersionedComparator::compare_key(self.0.key(), other.0.key()),
            _ => unreachable!(),
        }
    }
}

/// Iterates on multiple iterators, a.k.a. `MergeIterator`.
pub struct MergeIteratorInner<'a, const DIRECTION: usize> {
    /// Invalid or non-initialized iterators.
    unused_iters: LinkedList<BoxedHummockIterator<'a>>,

    /// The heap for merge sort.
    heap: BinaryHeap<Node<'a, DIRECTION>>,

    /// Statistics.
    stats: Arc<StateStoreMetrics>,
}

impl<'a, const DIRECTION: usize> MergeIteratorInner<'a, DIRECTION> {
    /// Caller should make sure that `iterators`'s direction is the same as `DIRECTION`.
    pub fn new(
        iterators: impl IntoIterator<Item = BoxedHummockIterator<'a>>,
        stats: Arc<StateStoreMetrics>,
    ) -> Self {
        Self {
            unused_iters: iterators.into_iter().collect(),
            heap: BinaryHeap::new(),
            stats,
        }
    }

    /// Moves all iterators from the `heap` to the linked list.
    fn reset_heap(&mut self) {
        self.unused_iters.extend(self.heap.drain().map(|n| n.0));
    }

    /// After some iterators in `unused_iterators` are sought or rewound, calls this function
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
impl<const DIRECTION: usize> HummockIterator for MergeIteratorInner<'_, DIRECTION> {
    async fn next(&mut self) -> HummockResult<()> {
        let mut node = self.heap.peek_mut().expect("no inner iter");

        // WARNING: within scope of BinaryHeap::PeekMut, we must carefully handle all places of
        // return. Once the iterator enters an invalid state, we should remove it from heap
        // before returning.

        match node.0.next().await {
            Ok(_) => {}
            Err(e) => {
                // If the iterator returns error, we should clear the heap, so that this iterator
                // becomes invalid.
                PeekMut::pop(node);
                self.heap.clear();
                return Err(e);
            }
        }

        if !node.0.is_valid() {
            // Put back to `unused_iters`
            let node = PeekMut::pop(node);
            self.unused_iters.push_back(node.0);
        } else {
            // This will update the heap top.
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
        let timer = self.stats.iter_merge_seek_duration.start_timer();

        self.reset_heap();
        futures::future::try_join_all(self.unused_iters.iter_mut().map(|x| x.seek(key))).await?;
        self.build_heap();

        timer.observe_duration();
        Ok(())
    }
}
