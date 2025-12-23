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

#![feature(allocator_api)]
#![feature(btree_cursors)]
#![feature(btree_extract_if)]

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

impl ZeroHeapSize for ethnum::U256 {}

impl<T> ZeroHeapSize for PhantomData<T> {}

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
