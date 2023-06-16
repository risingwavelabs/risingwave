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

pub mod collections;

use std::marker::PhantomData;

use bytes::Bytes;
use fixedbitset::FixedBitSet;
pub use risingwave_common_proc_macro::EstimateSize;
use rust_decimal::Decimal as RustDecimal;

use crate::types::DataType;

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

impl<T: ZeroHeapSize, const LEN: usize> EstimateSize for [T; LEN] {
    fn estimated_heap_size(&self) -> usize {
        0
    }
}

impl ZeroHeapSize for RustDecimal {}

impl<T> ZeroHeapSize for PhantomData<T> {}

impl ZeroHeapSize for DataType {}

#[derive(Clone)]
pub struct VecWithKvSize<T: EstimateSize> {
    inner: Vec<T>,
    kv_heap_size: usize,
}

impl<T: EstimateSize> Default for VecWithKvSize<T> {
    fn default() -> Self {
        Self {
            inner: vec![],
            kv_heap_size: 0,
        }
    }
}

impl<T: EstimateSize> VecWithKvSize<T> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn get_kv_size(&self) -> usize {
        self.kv_heap_size
    }

    pub fn push(&mut self, value: T) {
        self.kv_heap_size = self
            .kv_heap_size
            .saturating_add(value.estimated_heap_size());
        self.inner.push(value);
    }

    pub fn into_inner(self) -> Vec<T> {
        self.inner
    }

    pub fn inner(&self) -> &Vec<T> {
        &self.inner
    }
}

impl<T: EstimateSize> IntoIterator for VecWithKvSize<T> {
    type IntoIter = std::vec::IntoIter<Self::Item>;
    type Item = T;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

#[derive(Default)]
pub struct KvSize(usize);

impl KvSize {
    pub fn new() -> Self {
        Self(0)
    }

    pub fn with_size(size: usize) -> Self {
        Self(size)
    }

    pub fn add<K: EstimateSize, V: EstimateSize>(&mut self, key: &K, val: &V) -> usize {
        self.0 = self
            .0
            .saturating_add(key.estimated_size() + val.estimated_size());
        self.0
    }

    pub fn sub<K: EstimateSize, V: EstimateSize>(&mut self, key: &K, val: &V) -> usize {
        self.0 = self
            .0
            .saturating_sub(key.estimated_size() + val.estimated_size());
        self.0
    }

    pub fn add_val<V: EstimateSize>(&mut self, val: &V) -> usize {
        self.0 = self.0.saturating_add(val.estimated_size());
        self.0
    }

    pub fn sub_val<V: EstimateSize>(&mut self, val: &V) -> usize {
        self.0 = self.0.saturating_sub(val.estimated_size());
        self.0
    }

    pub fn add_size(&mut self, size: usize) {
        self.0 += size;
    }

    pub fn sub_size(&mut self, size: usize) {
        self.0 -= size;
    }

    pub fn set(&mut self, size: usize) {
        self.0 = size;
    }

    pub fn size(&self) -> usize {
        self.0
    }
}
