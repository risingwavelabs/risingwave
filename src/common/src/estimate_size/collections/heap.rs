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

use std::collections::BinaryHeap;
use std::mem::size_of;

use crate::estimate_size::EstimateSize;
use crate::memory::{MemoryContext, MonitoredGlobalAlloc};

pub struct MemMonitoredHeap<T> {
    inner: BinaryHeap<T>,
    mem_ctx: MemoryContext,
}

impl<T: Ord + EstimateSize> MemMonitoredHeap<T> {
    pub fn new_with(mem_ctx: MemoryContext) -> Self {
        Self {
            inner: BinaryHeap::new(),
            mem_ctx,
        }
    }

    pub fn with_capacity(capacity: usize, mem_ctx: MemoryContext) -> Self {
        let inner = BinaryHeap::with_capacity(capacity);
        mem_ctx.add((capacity * size_of::<T>()) as i64);
        Self { inner, mem_ctx }
    }

    pub fn push(&mut self, item: T) {
        let prev_cap = self.inner.capacity();
        let item_heap = item.estimated_heap_size();
        self.inner.push(item);
        let new_cap = self.inner.capacity();
        self.mem_ctx
            .add(((new_cap - prev_cap) * size_of::<T>() + item_heap) as i64);
    }

    pub fn pop(&mut self) -> Option<T> {
        let prev_cap = self.inner.capacity();
        let item = self.inner.pop();
        let item_heap = item.as_ref().map(|i| i.estimated_heap_size()).unwrap_or(0);
        let new_cap = self.inner.capacity();
        self.mem_ctx
            .add(-(((prev_cap - new_cap) * size_of::<T>() + item_heap) as i64));

        item
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn peek(&self) -> Option<&T> {
        self.inner.peek()
    }

    pub fn into_sorted_vec(self) -> Vec<T, MonitoredGlobalAlloc> {
        let old_cap = self.inner.capacity();
        let alloc = MonitoredGlobalAlloc::with_memory_context(self.mem_ctx.clone());
        let vec = self.inner.into_iter_sorted();

        let mut ret = Vec::with_capacity_in(vec.len(), alloc);
        ret.extend(vec);

        self.mem_ctx.add(-((old_cap * size_of::<T>()) as i64));
        ret
    }
}

impl<T> Extend<T> for MemMonitoredHeap<T>
where
    T: Ord + EstimateSize,
{
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        let old_cap = self.inner.capacity();
        let mut items_heap_size = 0usize;
        let items = iter.into_iter();
        self.inner.reserve_exact(items.size_hint().0);
        for item in items {
            items_heap_size += item.estimated_heap_size();
            self.inner.push(item);
        }

        let new_cap = self.inner.capacity();

        let diff = (new_cap - old_cap) * size_of::<T>() + items_heap_size;
        self.mem_ctx.add(diff as i64);
    }
}

#[cfg(test)]
mod tests {
    use prometheus::IntGauge;

    use crate::estimate_size::collections::MemMonitoredHeap;
    use crate::memory::MemoryContext;

    #[test]
    fn test_heap() {
        let gauge = IntGauge::new("test", "test").unwrap();
        let mem_ctx = MemoryContext::root(gauge.clone());

        let mut heap = MemMonitoredHeap::<u8>::new_with(mem_ctx);
        assert_eq!(0, gauge.get());

        heap.push(9u8);
        heap.push(1u8);
        assert_eq!(heap.inner.capacity() as i64, gauge.get());

        heap.pop().unwrap();
        assert_eq!(heap.inner.capacity() as i64, gauge.get());

        assert!(!heap.is_empty());
    }

    #[test]
    fn test_heap_drop() {
        let gauge = IntGauge::new("test", "test").unwrap();
        let mem_ctx = MemoryContext::root(gauge.clone());

        let vec = {
            let mut heap = MemMonitoredHeap::<u8>::new_with(mem_ctx);
            assert_eq!(0, gauge.get());

            heap.push(9u8);
            heap.push(1u8);
            assert_eq!(heap.inner.capacity() as i64, gauge.get());

            heap.into_sorted_vec()
        };

        assert_eq!(2, gauge.get());

        drop(vec);

        assert_eq!(0, gauge.get());
    }
}
