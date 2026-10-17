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

#![feature(btree_cursors)]

pub mod collections;

use std::cmp::Reverse;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use fixedbitset::FixedBitSet;
pub use risingwave_common_proc_macro::EstimateSize;

/// The trait for estimating the actual memory usage of a struct.
///
/// Used for cache eviction now.
pub trait EstimateSize {
    /// The estimated heap size of the current struct in bytes.
    fn estimated_heap_size(&self) -> usize;

    /// The estimated total size of the current struct in bytes, including the `estimated_heap_size`
    /// and the size of `Self`.
    fn estimated_size(&self) -> usize
    where
        Self: Sized,
    {
        self.estimated_heap_size() + std::mem::size_of::<Self>()
    }
}

impl EstimateSize for FixedBitSet {
    fn estimated_heap_size(&self) -> usize {
        std::mem::size_of_val(self.as_slice())
    }
}

impl EstimateSize for String {
    fn estimated_heap_size(&self) -> usize {
        self.capacity()
    }
}

impl<T: EstimateSize> EstimateSize for Option<T> {
    fn estimated_heap_size(&self) -> usize {
        if let Some(inner) = self {
            inner.estimated_heap_size()
        } else {
            0
        }
    }
}

/// SAFETY: `Bytes` can store a pointer in some cases, that may cause the size
/// of a `Bytes` be calculated more than one and when memory stats is larger than the real value.
impl EstimateSize for Bytes {
    fn estimated_heap_size(&self) -> usize {
        self.len()
    }
}

impl EstimateSize for Box<str> {
    fn estimated_heap_size(&self) -> usize {
        self.len()
    }
}

impl EstimateSize for serde_json::Value {
    fn estimated_heap_size(&self) -> usize {
        // FIXME: implement correct size
        // https://github.com/risingwavelabs/risingwave/issues/9377
        match self {
            Self::Null => 0,
            Self::Bool(_) => 0,
            Self::Number(_) => 0,
            Self::String(s) => s.estimated_heap_size(),
            Self::Array(v) => std::mem::size_of::<Self>() * v.capacity(),
            Self::Object(map) => std::mem::size_of::<Self>() * map.len(),
        }
    }
}

impl EstimateSize for jsonbb::Value {
    fn estimated_heap_size(&self) -> usize {
        self.capacity()
    }
}

impl EstimateSize for jsonbb::Builder {
    fn estimated_heap_size(&self) -> usize {
        self.capacity()
    }
}

impl<T1: EstimateSize, T2: EstimateSize> EstimateSize for (T1, T2) {
    fn estimated_heap_size(&self) -> usize {
        self.0.estimated_heap_size() + self.1.estimated_heap_size()
    }
}

macro_rules! primitive_estimate_size_impl {
    ($($t:ty)*) => ($(
        impl ZeroHeapSize for $t {}
    )*)
}

primitive_estimate_size_impl! { () usize u8 u16 u32 u64 u128 isize i8 i16 i32 i64 i128 f32 f64 bool }

pub trait ZeroHeapSize {}

impl<T: ZeroHeapSize> EstimateSize for T {
    fn estimated_heap_size(&self) -> usize {
        0
    }
}

impl<T: ZeroHeapSize> EstimateSize for Vec<T> {
    fn estimated_heap_size(&self) -> usize {
        std::mem::size_of::<T>() * self.capacity()
    }
}

impl<T: ZeroHeapSize> EstimateSize for Box<[T]> {
    fn estimated_heap_size(&self) -> usize {
        std::mem::size_of::<T>() * self.len()
    }
}

impl<T: EstimateSize> EstimateSize for Reverse<T> {
    fn estimated_heap_size(&self) -> usize {
        self.0.estimated_heap_size()
    }
}

impl<T: ZeroHeapSize, const LEN: usize> EstimateSize for [T; LEN] {
    fn estimated_heap_size(&self) -> usize {
        0
    }
}

impl ZeroHeapSize for rust_decimal::Decimal {}

impl ZeroHeapSize for ethnum::I256 {}

impl<T> ZeroHeapSize for PhantomData<T> {}

/// Estimates the size of the heap allocation backing a hash table (such as
/// [`std::collections::HashMap`] or [`std::collections::HashSet`]) whose `capacity()` is
/// `capacity` and whose slots hold values of type `T` (`(K, V)` for a map).
///
/// The standard library's hash tables use the `SwissTable` layout: a power-of-two number of
/// buckets, each holding one `T` inline plus one control byte. A table with 8 or more buckets
/// reports a capacity of 7/8 of its buckets, and smaller tables (4 or 8 buckets) keep one
/// bucket empty, so the bucket count can be recovered from `capacity()`. The small fixed tail of
/// control bytes and alignment padding are ignored. An empty table with no capacity does not
/// allocate.
pub fn hash_table_heap_size<T>(capacity: usize) -> usize {
    let buckets = if capacity == 0 {
        0
    } else if capacity < 8 {
        capacity + 1
    } else {
        capacity / 7 * 8
    };
    buckets.saturating_mul(std::mem::size_of::<T>() + 1)
}

/// The size of the collection.
///
/// We use an atomic value here to enable operating the size without a mutable reference.
/// See [`collections::AtomicMutGuard`] for more details.
///
/// In the most cases, we have the mutable reference of this struct, so we can directly
/// operate the underlying value.
#[derive(Default)]
pub struct KvSize(AtomicUsize);

/// Clone the [`KvSize`] will duplicate the underlying value.
impl Clone for KvSize {
    fn clone(&self) -> Self {
        Self(self.size().into())
    }
}

impl KvSize {
    pub fn new() -> Self {
        Self(0.into())
    }

    pub fn with_size(size: usize) -> Self {
        Self(size.into())
    }

    pub fn add<K: EstimateSize, V: EstimateSize>(&mut self, key: &K, val: &V) {
        self.add_size(key.estimated_size());
        self.add_size(val.estimated_size());
    }

    pub fn sub<K: EstimateSize, V: EstimateSize>(&mut self, key: &K, val: &V) {
        self.sub_size(key.estimated_size());
        self.sub_size(val.estimated_size());
    }

    /// Add the size of `val` and return it.
    pub fn add_val<V: EstimateSize>(&mut self, val: &V) -> usize {
        let size = val.estimated_size();
        self.add_size(size);
        size
    }

    pub fn sub_val<V: EstimateSize>(&mut self, val: &V) {
        self.sub_size(val.estimated_size());
    }

    pub fn add_size(&mut self, size: usize) {
        let this = self.0.get_mut(); // get the underlying value since we have a mutable reference
        *this = this.saturating_add(size);
    }

    pub fn sub_size(&mut self, size: usize) {
        let this = self.0.get_mut(); // get the underlying value since we have a mutable reference
        *this = this.saturating_sub(size);
    }

    /// Update the size of the collection by `to - from` atomically, i.e., without a mutable reference.
    pub fn update_size_atomic(&self, from: usize, to: usize) {
        let _ = (self.0).fetch_update(Ordering::Relaxed, Ordering::Relaxed, |this| {
            Some(this.saturating_add(to).saturating_sub(from))
        });
    }

    pub fn set(&mut self, size: usize) {
        self.0 = size.into();
    }

    pub fn size(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// Recovers the bucket count the same way `hash_table_heap_size` does.
    fn buckets(capacity: usize) -> usize {
        hash_table_heap_size::<u8>(capacity) / (std::mem::size_of::<u8>() + 1)
    }

    #[test]
    fn test_hash_table_heap_size_empty() {
        assert_eq!(hash_table_heap_size::<(u64, u64)>(0), 0);
        let map = HashMap::<u64, u64>::new();
        assert_eq!(hash_table_heap_size::<(u64, u64)>(map.capacity()), 0);
    }

    /// The bucket count recovered from `capacity()` must be a power of two that yields back the
    /// same capacity, for every capacity the standard library actually produces. This fails if
    /// the standard library changes its hash table growth policy.
    #[test]
    fn test_hash_table_heap_size_matches_std_capacities() {
        let mut map = HashMap::<u64, u64>::new();
        for i in 0..100_000u64 {
            map.insert(i, i);
            let capacity = map.capacity();
            let buckets = buckets(capacity);
            assert!(buckets.is_power_of_two(), "capacity {capacity}");
            let expected_capacity = if buckets < 8 {
                buckets - 1
            } else {
                buckets / 8 * 7
            };
            assert_eq!(expected_capacity, capacity);
            assert!(capacity >= map.len());
        }
    }

    #[test]
    fn test_hash_table_heap_size_scales_with_element_size() {
        let map = HashMap::<u64, u64>::with_capacity(1000);
        let capacity = map.capacity();
        let buckets = buckets(capacity);
        assert_eq!(hash_table_heap_size::<(u64, u64)>(capacity), buckets * 17);
        assert_eq!(hash_table_heap_size::<u64>(capacity), buckets * 9);
    }
}
