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

use std::alloc::{Allocator, Global};
use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use risingwave_common::lru::{LruCache, RandomState};
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_common::sequence::AtomicSequence;
use risingwave_common_estimate_size::EstimateSize;

use crate::common::metrics::MetricsInfo;

const REPORT_SIZE_EVERY_N_KB_CHANGE: usize = 4096;

/// The managed cache is a lru cache that bounds the memory usage by epoch.
/// Should be used with `MemoryManager`.
pub struct ManagedLruCache<K, V, S = RandomState, A = Global>
where
    K: Hash + Eq,
    S: BuildHasher + Send + Sync + 'static,
    A: Clone + Allocator,
{
    inner: LruCache<K, V, S, A>,

    /// The entry with sequence less than `watermark_sequence` should be evicted.
    /// `watermark_sequence` should only be updatd by `MemoryManager`.
    watermark_sequence: Arc<AtomicSequence>,

    // Metrics info
    _metrics_info: MetricsInfo,

    reporter: HeapSizeReporter,
}

impl<K, V, S, A> ManagedLruCache<K, V, S, A>
where
    K: Hash + Eq + EstimateSize,
    V: EstimateSize,
    S: BuildHasher + Send + Sync + 'static,
    A: Clone + Allocator,
{
    pub fn unbounded_with_hasher_in(
        watermark_sequence: Arc<AtomicSequence>,
        metrics_info: MetricsInfo,
        hash_builder: S,
        alloc: A,
    ) -> Self {
        let inner = LruCache::unbounded_with_hasher_in(hash_builder, alloc);

        let memory_usage_metrics = metrics_info
            .metrics
            .stream_memory_usage
            .with_guarded_label_values(&[
                &metrics_info.actor_id,
                &metrics_info.table_id,
                &metrics_info.desc,
            ]);
        memory_usage_metrics.set(0.into());

        let reporter = HeapSizeReporter::new(memory_usage_metrics, 0, 0);

        Self {
            inner,
            watermark_sequence,
            _metrics_info: metrics_info,
            reporter,
        }
    }

    /// Evict epochs lower than the watermark
    pub fn evict(&mut self) {
        let sequence = self.watermark_sequence.load(Ordering::Relaxed);
        while let Some((key, value, _)) = self.inner.pop_with_sequence(sequence) {
            let charge = key.estimated_size() + value.estimated_size();
            self.reporter.dec(charge);
        }
    }

    pub fn put(&mut self, k: K, v: V) -> Option<V> {
        let key_size = k.estimated_size();
        self.reporter.inc(key_size + v.estimated_size());
        let old_val = self.inner.put(k, v);
        if let Some(old_val) = &old_val {
            self.reporter.dec(key_size + old_val.estimated_size());
        }
        old_val
    }

    // TODO(MrCroxx): REMOVE ME!!!
    pub fn push(&mut self, k: K, v: V) -> Option<V> {
        self.put(k, v)
    }

    pub fn remove(&mut self, k: &K) -> Option<V> {
        let key_size = k.estimated_size();
        let old_val = self.inner.remove(k);
        if let Some(old_val) = &old_val {
            self.reporter.dec(key_size + old_val.estimated_size());
        }
        old_val
    }

    pub fn get_mut(&mut self, k: &K) -> Option<MutGuard<'_, V>> {
        let v = self.inner.get_mut(k);
        v.map(|inner| MutGuard::new(inner, &mut self.reporter))
    }

    pub fn get<Q>(&mut self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get(k)
    }

    pub fn peek<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.peek(k)
    }

    pub fn peek_mut(&mut self, k: &K) -> Option<MutGuard<'_, V>> {
        let v = self.inner.peek_mut(k);
        v.map(|inner| MutGuard::new(inner, &mut self.reporter))
    }

    pub fn contains<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.contains(k)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<K, V> ManagedLruCache<K, V>
where
    K: Hash + Eq + EstimateSize,
    V: EstimateSize,
{
    pub fn unbounded(watermark_sequence: Arc<AtomicSequence>, metrics_info: MetricsInfo) -> Self {
        Self::unbounded_with_hasher(watermark_sequence, metrics_info, RandomState::default())
    }
}

impl<K, V, S> ManagedLruCache<K, V, S>
where
    K: Hash + Eq + EstimateSize,
    V: EstimateSize,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn unbounded_with_hasher(
        watermark_sequence: Arc<AtomicSequence>,
        metrics_info: MetricsInfo,
        hash_builder: S,
    ) -> Self {
        Self::unbounded_with_hasher_in(watermark_sequence, metrics_info, hash_builder, Global)
    }
}

pub struct MutGuard<'a, V: EstimateSize> {
    inner: &'a mut V,
    reporter: &'a mut HeapSizeReporter,
    old_value_size: usize,
}

impl<'a, V: EstimateSize> MutGuard<'a, V> {
    fn new(inner: &'a mut V, reporter: &'a mut HeapSizeReporter) -> Self {
        let old_value_size = inner.estimated_size();
        Self {
            inner,
            reporter,
            old_value_size,
        }
    }
}

impl<V: EstimateSize> Drop for MutGuard<'_, V> {
    fn drop(&mut self) {
        let new_value_size = self.inner.estimated_size();
        if new_value_size != self.old_value_size {
            self.reporter.apply(|size| {
                *size = size
                    .saturating_sub(self.old_value_size)
                    .saturating_add(new_value_size)
            })
        }
    }
}

impl<V: EstimateSize> Deref for MutGuard<'_, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<V: EstimateSize> DerefMut for MutGuard<'_, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
    }
}

struct HeapSizeReporter {
    metrics: LabelGuardedIntGauge,
    heap_size: usize,
    last_reported: usize,
}

impl HeapSizeReporter {
    fn new(
        heap_size_metrics: LabelGuardedIntGauge,
        heap_size: usize,
        last_reported: usize,
    ) -> Self {
        Self {
            metrics: heap_size_metrics,
            heap_size,
            last_reported,
        }
    }

    fn inc(&mut self, size: usize) {
        self.heap_size = self.heap_size.saturating_add(size);
        self.try_report();
    }

    fn dec(&mut self, size: usize) {
        self.heap_size = self.heap_size.saturating_sub(size);
        self.try_report();
    }

    fn apply<F>(&mut self, f: F)
    where
        F: FnOnce(&mut usize),
    {
        f(&mut self.heap_size);
        self.try_report();
    }

    fn try_report(&mut self) -> bool {
        if self.heap_size.abs_diff(self.last_reported) >= REPORT_SIZE_EVERY_N_KB_CHANGE << 10 {
            self.metrics.set(self.heap_size as _);
            self.last_reported = self.heap_size;
            true
        } else {
            false
        }
    }
}

impl Drop for HeapSizeReporter {
    fn drop(&mut self) {
        self.metrics.set(0);
    }
}
