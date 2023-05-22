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
use crate::memory::MemoryContext;

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
}
