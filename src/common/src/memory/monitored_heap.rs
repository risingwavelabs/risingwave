// Copyright 2024 RisingWave Labs
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

use risingwave_common_estimate_size::EstimateSize;

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
        // The allocation already succeeded; a budget overrun must not discard its charge.
        mem_ctx.add_unchecked((capacity * size_of::<T>()) as i64);
        Self { inner, mem_ctx }
    }

    pub fn push(&mut self, item: T) {
        let prev_cap = self.inner.capacity();
        let item_heap = item.estimated_heap_size();
        self.inner.push(item);
        let new_cap = self.inner.capacity();
        self.mem_ctx
            .add_unchecked(((new_cap - prev_cap) * size_of::<T>() + item_heap) as i64);
    }

    pub fn pop(&mut self) -> Option<T> {
        let prev_cap = self.inner.capacity();
        let item = self.inner.pop();
        let item_heap = item.as_ref().map(|i| i.estimated_heap_size()).unwrap_or(0);
        let new_cap = self.inner.capacity();
        self.mem_ctx
            .add_unchecked(-(((prev_cap - new_cap) * size_of::<T>() + item_heap) as i64));

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

    /// Moves the elements into a sorted vector and releases the old heap's backing-storage charge.
    ///
    /// # Warning
    ///
    /// `deallocate` only subtracts the backing-storage size. The elements may already have been
    /// dropped, so it cannot determine the size of their separately allocated memory. Those charges
    /// need separate cleanup, either explicitly or through a private memory context. A future
    /// redesign should make this handling automatic.
    ///
    /// In this example, the heap uses a new private `mem_ctx`. Values are dropped as they are
    /// consumed, but their payload charges remain until the context is dropped on normal return,
    /// errors, or cancellation.
    ///
    /// ```rust
    /// # #![feature(allocator_api)]
    /// # use prometheus::core::Atomic;
    /// # use risingwave_common::memory::{MemMonitoredHeap, MemoryContext};
    /// # use risingwave_common::metrics::TrAdderAtomic;
    /// # let parent = MemoryContext::none();
    /// let mem_ctx = MemoryContext::new(Some(parent), TrAdderAtomic::new(0));
    /// let mut heap = MemMonitoredHeap::new_with(mem_ctx.clone());
    /// heap.push(String::from("value"));
    /// for value in heap.into_sorted_vec() {
    ///     drop(value);
    /// }
    /// ```
    pub fn into_sorted_vec(self) -> Vec<T, MonitoredGlobalAlloc> {
        let old_cap = self.inner.capacity();
        let alloc = MonitoredGlobalAlloc::with_memory_context(self.mem_ctx.clone());
        let vec = self.inner.into_iter_sorted();

        let mut ret = Vec::with_capacity_in(vec.len(), alloc);
        ret.extend(vec);

        self.mem_ctx
            .add_unchecked(-((old_cap * size_of::<T>()) as i64));
        ret
    }

    pub fn mem_context(&self) -> &MemoryContext {
        &self.mem_ctx
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
        self.mem_ctx.add_unchecked(diff as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::MemMonitoredHeap;
    use crate::memory::MemoryContext;
    use crate::metrics::LabelGuardedIntGauge;

    /// Verifies that a heap's allocated space is counted even above budget, and freeing its sorted
    /// output removes those charges without changing other users' memory counts.
    #[test]
    fn test_over_budget_capacity_accounting() {
        let parent = MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), 64);
        assert!(parent.add(16));
        let child = MemoryContext::new(
            Some(parent.clone()),
            LabelGuardedIntGauge::test_int_gauge::<4>(),
        );
        let heap = MemMonitoredHeap::<u8>::with_capacity(128, child.clone());
        assert_eq!(child.get_bytes_used(), 128);
        assert_eq!(parent.get_bytes_used(), 144);
        drop(heap.into_sorted_vec());
        // `into_sorted_vec()` releases the old heap's backing-storage charge. Only the new vector's
        // backing storage and any separately allocated element payloads remain charged.
        // This heap is empty, so the returned vector has no backing allocation or element payloads.
        // The child context therefore reports zero usage.
        assert_eq!(child.get_bytes_used(), 0);
        assert_eq!(parent.get_bytes_used(), 16);
        assert!(parent.add(1));
        assert_eq!(parent.get_bytes_used(), 17);
        assert!(parent.add(-17));
    }

    /// Verifies that adding strings to a heap counts their memory even above budget, and removing
    /// them subtracts the right amount each time. After converting a nonempty heap, the caller
    /// must release consumed strings' payload charges; the vector releases its backing storage.
    #[test]
    fn test_over_budget_push_extend_and_pop_accounting() {
        let parent = MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), 64);
        assert!(parent.add(16));
        let child = MemoryContext::new(
            Some(parent.clone()),
            LabelGuardedIntGauge::test_int_gauge::<4>(),
        );
        let expected_usage = |heap: &MemMonitoredHeap<String>| {
            (heap.inner.capacity() * std::mem::size_of::<String>()
                + heap.inner.iter().map(String::capacity).sum::<usize>()) as i64
        };
        let mut heap = MemMonitoredHeap::<String>::new_with(child.clone());
        heap.push("a".repeat(128));
        assert_eq!(child.get_bytes_used(), expected_usage(&heap));
        heap.extend(["b".repeat(128), "c".repeat(128)]);
        assert_eq!(child.get_bytes_used(), expected_usage(&heap));
        assert_eq!(parent.get_bytes_used(), child.get_bytes_used() + 16);
        assert!(!child.check_memory_usage());
        while let Some(item) = heap.pop() {
            drop(item);
            assert_eq!(child.get_bytes_used(), expected_usage(&heap));
            assert_eq!(parent.get_bytes_used(), child.get_bytes_used() + 16);
        }
        // Preserve the pop coverage above, then convert a heap with actual string payloads.
        heap.extend(["d".repeat(128), "e".repeat(128)]);
        let values = heap.into_sorted_vec();
        let backing_bytes = (values.capacity() * std::mem::size_of::<String>()) as i64;
        let mut payload_bytes = values
            .iter()
            .map(|value| value.capacity() as i64)
            .sum::<i64>();
        assert_eq!(child.get_bytes_used(), backing_bytes + payload_bytes);
        for value in values {
            let bytes = value.capacity() as i64;
            drop(value);
            child.add_unchecked(-bytes);
            payload_bytes -= bytes;
            assert_eq!(child.get_bytes_used(), backing_bytes + payload_bytes);
            assert_eq!(parent.get_bytes_used(), child.get_bytes_used() + 16);
        }
        assert_eq!(child.get_bytes_used(), 0);
        assert_eq!(parent.get_bytes_used(), 16);
        assert!(parent.add(-16));
    }

    #[test]
    fn test_heap() {
        let gauge = LabelGuardedIntGauge::test_int_gauge::<4>();
        let mem_ctx = MemoryContext::root(gauge.clone(), u64::MAX);

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
        let gauge = LabelGuardedIntGauge::test_int_gauge::<4>();
        let mem_ctx = MemoryContext::root(gauge.clone(), u64::MAX);

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
