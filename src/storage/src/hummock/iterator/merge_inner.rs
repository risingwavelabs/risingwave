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

use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, LinkedList};
use std::future::Future;
use std::ops::{Deref, DerefMut};

use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey};

use crate::hummock::iterator::{DirectionEnum, HummockIterator, HummockIteratorDirection};
use crate::hummock::value::HummockValue;
use crate::hummock::HummockResult;
use crate::monitor::StoreLocalStatistic;

pub trait NodeExtraOrderInfo: Eq + Ord + Send + Sync {}

/// For unordered merge iterator, no extra order info is needed.
type UnorderedNodeExtra = ();
/// Store the order index for the order aware merge iterator
type OrderedNodeExtra = usize;
impl NodeExtraOrderInfo for UnorderedNodeExtra {}
impl NodeExtraOrderInfo for OrderedNodeExtra {}

pub struct Node<I: HummockIterator, T: NodeExtraOrderInfo> {
    iter: I,
    extra_order_info: T,
}

impl<I: HummockIterator, T: NodeExtraOrderInfo> Eq for Node<I, T> where Self: PartialEq {}
impl<I: HummockIterator, T: NodeExtraOrderInfo> Ord for Node<I, T>
where
    Self: PartialOrd,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Implement `PartialOrd` for unordered iter node. Only compare the key.
impl<I: HummockIterator> PartialOrd for Node<I, UnorderedNodeExtra> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Note: to implement min-heap by using max-heap internally, the comparing
        // order should be reversed.

        Some(match I::Direction::direction() {
            DirectionEnum::Forward => other.iter.key().cmp(&self.iter.key()),
            DirectionEnum::Backward => self.iter.key().cmp(&other.iter.key()),
        })
    }
}

/// Implement `PartialOrd` for ordered iter node. Compare key and use order index as tie breaker.
impl<I: HummockIterator> PartialOrd for Node<I, OrderedNodeExtra> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // The `extra_info` is used as a tie-breaker when the keys are equal.
        Some(match I::Direction::direction() {
            DirectionEnum::Forward => other
                .iter
                .key()
                .cmp(&self.iter.key())
                .then_with(|| other.extra_order_info.cmp(&self.extra_order_info)),
            DirectionEnum::Backward => self
                .iter
                .key()
                .cmp(&other.iter.key())
                .then_with(|| self.extra_order_info.cmp(&other.extra_order_info)),
        })
    }
}

impl<I: HummockIterator> PartialEq for Node<I, UnorderedNodeExtra> {
    fn eq(&self, other: &Self) -> bool {
        self.iter.key() == other.iter.key()
    }
}

impl<I: HummockIterator> PartialEq for Node<I, OrderedNodeExtra> {
    fn eq(&self, other: &Self) -> bool {
        self.iter.key() == other.iter.key() && self.extra_order_info.eq(&other.extra_order_info)
    }
}

/// Iterates on multiple iterators, a.k.a. `MergeIterator`.
pub struct MergeIteratorInner<I: HummockIterator, NE: NodeExtraOrderInfo> {
    /// Invalid or non-initialized iterators.
    unused_iters: Vec<Node<I, NE>>,

    /// The heap for merge sort.
    heap: BinaryHeap<Node<I, NE>>,

    last_table_key: Vec<u8>,
}

/// An order aware merge iterator.
#[allow(type_alias_bounds)]
pub type OrderedMergeIteratorInner<I: HummockIterator> = MergeIteratorInner<I, OrderedNodeExtra>;

impl<I: HummockIterator> OrderedMergeIteratorInner<I> {
    pub fn new(iterators: impl IntoIterator<Item = I>) -> Self {
        Self::create(iterators)
    }

    pub fn for_compactor(iterators: impl IntoIterator<Item = I>) -> Self {
        Self::create(iterators)
    }

    fn create(iterators: impl IntoIterator<Item = I>) -> Self {
        Self {
            unused_iters: iterators
                .into_iter()
                .enumerate()
                .map(|(i, iter)| Node {
                    iter,
                    extra_order_info: i,
                })
                .collect(),
            heap: BinaryHeap::new(),
            last_table_key: Vec::new(),
        }
    }
}

impl<I: HummockIterator, NE: NodeExtraOrderInfo> MergeIteratorInner<I, NE> {
    fn collect_local_statistic_impl(&self, stats: &mut StoreLocalStatistic) {
        for node in &self.heap {
            node.iter.collect_local_statistic(stats);
        }
        for node in &self.unused_iters {
            node.iter.collect_local_statistic(stats);
        }
    }
}

#[allow(type_alias_bounds)]
pub type UnorderedMergeIteratorInner<I: HummockIterator> =
    MergeIteratorInner<I, UnorderedNodeExtra>;

impl<I: HummockIterator> UnorderedMergeIteratorInner<I> {
    pub fn new(iterators: impl IntoIterator<Item = I>) -> Self {
        Self::create(iterators)
    }

    pub fn for_compactor(iterators: impl IntoIterator<Item = I>) -> Self {
        Self::create(iterators)
    }

    fn create(iterators: impl IntoIterator<Item = I>) -> Self {
        Self {
            unused_iters: iterators
                .into_iter()
                .map(|iter| Node {
                    iter,
                    extra_order_info: (),
                })
                .collect(),
            heap: BinaryHeap::new(),
            last_table_key: Vec::new(),
        }
    }
}

impl<I: HummockIterator, NE: NodeExtraOrderInfo> MergeIteratorInner<I, NE>
where
    Node<I, NE>: Ord,
{
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

/// The behaviour of `next` of order aware merge iterator is different from the normal one, so we
/// extract this trait.
trait MergeIteratorNext {
    type HummockResultFuture<'a>: Future<Output = HummockResult<()>> + Send + 'a
    where
        Self: 'a;
    fn next_inner(&mut self) -> Self::HummockResultFuture<'_>;
}

/// This is a wrapper for the `PeekMut` of heap.
///
/// Several panics due to future cancellation are caused by calling `drop` on the `PeekMut` when
/// futures holding the `PeekMut` are cancelled and dropped. Dropping a `PeekMut` will accidentally
/// cause a comparison between the top node and the node below, and may call `key()` for top node
/// iterators that are in some intermediate inconsistent states.
///
/// When a `PeekMut` is wrapped by this guard, when the guard is dropped, `PeekMut::pop` will be
/// called on the `PeekMut`, and the popped node will be added to the linked list that collects the
/// unused nodes. In this way, when the future holding the guard is dropped, the `PeekMut` will not
/// be called `drop`, and there will not be unexpected `key()` called for heap comparison.
///
/// In normal usage, when we finished using the `PeekMut`, we should explicitly call `guard.used()`
/// in every branch carefully. When we want to pop the `PeekMut`, we can simply call `guard.pop()`.
struct PeekMutGuard<'a, T: Ord> {
    peek: Option<PeekMut<'a, T>>,
    unused: &'a mut Vec<T>,
}

impl<'a, T: Ord> PeekMutGuard<'a, T> {
    /// Call `peek_mut` on the top of heap and return a guard over the `PeekMut` if the heap is not
    /// empty.
    fn peek_mut(heap: &'a mut BinaryHeap<T>, unused: &'a mut Vec<T>) -> Option<Self> {
        heap.peek_mut().map(|peek| Self {
            peek: Some(peek),
            unused,
        })
    }

    /// Call `pop` on the `PeekMut`.
    fn pop(mut self) -> T {
        PeekMut::pop(self.peek.take().expect("should not be None"))
    }

    /// Mark finish using the `PeekMut`. `drop` will be called on the `PeekMut` directly.
    fn used(mut self) {
        self.peek.take().expect("should not be None");
    }
}

impl<'a, T: Ord> Deref for PeekMutGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.peek.as_ref().expect("should not be None")
    }
}

impl<'a, T: Ord> DerefMut for PeekMutGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.peek.as_mut().expect("should not be None")
    }
}

impl<'a, T: Ord> Drop for PeekMutGuard<'a, T> {
    /// When the guard is dropped, if `pop` or `used` is not called before it is dropped, we will
    /// call `PeekMut::pop` on the `PeekMut` and recycle the node to the unused list.
    fn drop(&mut self) {
        if let Some(peek) = self.peek.take() {
            tracing::debug!(
                "PeekMut are dropped without used. May be caused by future cancellation"
            );
            let top = PeekMut::pop(peek);
            self.unused.push(top);
        }
    }
}

impl<I: HummockIterator> MergeIteratorNext for OrderedMergeIteratorInner<I> {
    type HummockResultFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next_inner(&mut self) -> Self::HummockResultFuture<'_> {
        async {
            let top_key = {
                let top_key = self.heap.peek().expect("no inner iter").iter.key();
                self.last_table_key.clear();
                self.last_table_key
                    .extend_from_slice(top_key.user_key.table_key.0);
                FullKey {
                    user_key: UserKey {
                        table_id: top_key.user_key.table_id,
                        table_key: TableKey(self.last_table_key.as_slice()),
                    },
                    epoch: top_key.epoch,
                }
            };
            loop {
                let Some(mut node) = PeekMutGuard::peek_mut(&mut self.heap, &mut self.unused_iters) else {
                    break;
                };
                // WARNING: within scope of BinaryHeap::PeekMut, we must carefully handle all places
                // of return. Once the iterator enters an invalid state, we should
                // remove it from heap before returning.

                if node.iter.key() == top_key {
                    if let Err(e) = node.iter.next().await {
                        node.pop();
                        self.heap.clear();
                        return Err(e);
                    };
                    if !node.iter.is_valid() {
                        let node = node.pop();
                        self.unused_iters.push(node);
                    } else {
                        node.used();
                    }
                } else {
                    node.used();
                    break;
                }
            }

            Ok(())
        }
    }
}

impl<I: HummockIterator> MergeIteratorNext for UnorderedMergeIteratorInner<I> {
    type HummockResultFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next_inner(&mut self) -> Self::HummockResultFuture<'_> {
        async {
            let mut node = PeekMutGuard::peek_mut(&mut self.heap, &mut self.unused_iters)
                .expect("no inner iter");

            // WARNING: within scope of BinaryHeap::PeekMut, we must carefully handle all places of
            // return. Once the iterator enters an invalid state, we should remove it from heap
            // before returning.

            match node.iter.next().await {
                Ok(_) => {}
                Err(e) => {
                    // If the iterator returns error, we should clear the heap, so that this
                    // iterator becomes invalid.
                    node.pop();
                    self.heap.clear();
                    return Err(e);
                }
            }

            if !node.iter.is_valid() {
                // Put back to `unused_iters`
                let node = node.pop();
                self.unused_iters.push(node);
            } else {
                // This will update the heap top.
                node.used();
            }

            Ok(())
        }
    }
}

impl<I: HummockIterator, NE: NodeExtraOrderInfo> HummockIterator for MergeIteratorInner<I, NE>
where
    Self: MergeIteratorNext + 'static,
    Node<I, NE>: Ord,
{
    type Direction = I::Direction;

    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        self.next_inner()
    }

    fn key(&self) -> FullKey<&[u8]> {
        self.heap.peek().expect("no inner iter").iter.key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.heap.peek().expect("no inner iter").iter.value()
    }

    fn is_valid(&self) -> bool {
        self.heap.peek().map_or(false, |n| n.iter.is_valid())
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async move {
            self.reset_heap();
            futures::future::try_join_all(self.unused_iters.iter_mut().map(|x| x.iter.rewind()))
                .await?;
            self.build_heap();
            Ok(())
        }
    }

    fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> Self::SeekFuture<'a> {
        async move {
            self.reset_heap();
            const MAX_CONCURRENCY: usize = 5;
            if self.unused_iters.len() < MAX_CONCURRENCY {
                futures::future::try_join_all(
                    self.unused_iters.iter_mut().map(|x| x.iter.seek(key)),
                )
                .await?;
            } else {
                let mut start_idx = 0;
                while start_idx < self.unused_iters.len() {
                    let end_idx =
                        std::cmp::min(start_idx + MAX_CONCURRENCY, self.unused_iters.len());
                    futures::future::try_join_all(
                        self.unused_iters[start_idx..end_idx]
                            .iter_mut()
                            .map(|x| x.iter.seek(key)),
                    )
                    .await?;
                    start_idx += MAX_CONCURRENCY;
                }
            }
            self.build_heap();
            Ok(())
        }
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        self.collect_local_statistic_impl(stats);
    }
}
