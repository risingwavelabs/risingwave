// Copyright 2025 RisingWave Labs
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

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::mem::replace;

use crate::vector::VectorDistance;

pub(super) fn compare_distance(first: VectorDistance, second: VectorDistance) -> Ordering {
    first
        .partial_cmp(&second)
        .unwrap_or_else(|| panic!("failed to compare distance {} and {}.", first, second))
}

fn compare_distance_on_heap<const MAX_HEAP: bool>(
    first: VectorDistance,
    second: VectorDistance,
) -> Ordering {
    let (first, second) = if MAX_HEAP {
        (first, second)
    } else {
        (second, first)
    };
    compare_distance(first, second)
}

pub(super) struct HeapNode<I, const MAX_HEAP: bool> {
    distance: VectorDistance,
    pub(super) item: I,
}

impl<I, const MAX_HEAP: bool> PartialEq for HeapNode<I, MAX_HEAP> {
    fn eq(&self, other: &Self) -> bool {
        self.distance.eq(&other.distance)
    }
}

impl<I, const MAX_HEAP: bool> Eq for HeapNode<I, MAX_HEAP> {}

impl<I, const MAX_HEAP: bool> PartialOrd for HeapNode<I, MAX_HEAP> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I, const MAX_HEAP: bool> Ord for HeapNode<I, MAX_HEAP> {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_distance_on_heap::<MAX_HEAP>(self.distance, other.distance)
    }
}

pub struct DistanceHeap<I, const MAX_HEAP: bool>(BinaryHeap<HeapNode<I, MAX_HEAP>>);

pub type MaxDistanceHeap<I> = DistanceHeap<I, true>;
pub type MinDistanceHeap<I> = DistanceHeap<I, false>;

impl<I, const MAX_HEAP: bool> DistanceHeap<I, MAX_HEAP> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(BinaryHeap::with_capacity(capacity))
    }

    pub fn push(&mut self, distance: VectorDistance, item: I) {
        self.0.push(HeapNode { distance, item });
    }

    pub fn top(&self) -> Option<(VectorDistance, &I)> {
        self.0.peek().map(|node| (node.distance, &node.item))
    }

    pub fn pop(&mut self) -> Option<(VectorDistance, I)> {
        self.0.pop().map(|node| (node.distance, node.item))
    }
}

pub struct BoundedNearest<I> {
    heap: MaxDistanceHeap<I>,
    capacity: usize,
}

impl<I> BoundedNearest<I> {
    pub fn new(capacity: usize) -> Self {
        Self {
            heap: DistanceHeap(BinaryHeap::with_capacity(capacity)),
            capacity,
        }
    }

    pub fn furthest(&self) -> Option<(VectorDistance, &I)> {
        self.heap.top()
    }

    pub fn insert(
        &mut self,
        distance: VectorDistance,
        get_item: impl FnOnce() -> I,
    ) -> Option<(VectorDistance, I)> {
        if self.heap.0.len() >= self.capacity {
            let mut top = self.heap.0.peek_mut().expect("non-empty");
            if top.distance > distance {
                let prev_node = replace(
                    &mut *top,
                    HeapNode {
                        distance,
                        item: get_item(),
                    },
                );
                Some((prev_node.distance, prev_node.item))
            } else {
                None
            }
        } else {
            self.heap.0.push(HeapNode {
                distance,
                item: get_item(),
            });
            None
        }
    }

    pub fn collect(mut self) -> Vec<I> {
        let size = self.heap.0.len();
        let mut vec = Vec::with_capacity(size);
        let uninit_slice = vec.spare_capacity_mut();
        let mut i = size;
        // elements are popped from max to min, so we write elements from back to front to ensure that the output is sorted ascendingly.
        while let Some(node) = self.heap.0.pop() {
            i -= 1;
            // safety: `i` is initialized as the size of `self.heap`. It must have decremented for once, and can
            // decrement for at most `size` time, so it must be that 0 <= i < size
            unsafe {
                uninit_slice.get_unchecked_mut(i).write(node.item);
            }
        }
        assert_eq!(i, 0);
        // safety: should have write `size` elements to the vector.
        unsafe { vec.set_len(size) }
        vec
    }

    pub fn resize(&mut self, new_capacity: usize) {
        self.capacity = new_capacity;
        while self.heap.0.len() > new_capacity {
            self.heap.pop();
        }
    }
}

impl<'a, I> IntoIterator for &'a BoundedNearest<I> {
    type Item = (VectorDistance, &'a I);

    type IntoIter = impl Iterator<Item = Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.heap.0.iter().map(|node| (node.distance, &node.item))
    }
}
