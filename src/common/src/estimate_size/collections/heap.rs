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
