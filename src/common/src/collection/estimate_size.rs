// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use fixedbitset::FixedBitSet;

use crate::array::Row;

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
        self.as_slice().len() * std::mem::size_of::<u32>()
    }
}

impl EstimateSize for std::collections::BTreeSet<Row> {
    // FIXME(yuhao): impl the correct size.
    fn estimated_heap_size(&self) -> usize {
        0
    }
}

impl<T: EstimateSize> EstimateSize for Box<T> {
    fn estimated_heap_size(&self) -> usize {
        self.as_ref().estimated_size()
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

impl EstimateSize for String {
    fn estimated_heap_size(&self) -> usize {
        self.capacity()
    }
}

impl EstimateSize for () {
    fn estimated_heap_size(&self) -> usize {
        0
    }
}

impl EstimateSize for &str {
    fn estimated_heap_size(&self) -> usize {
        self.len()
    }
}

macro_rules! estimate_size_impl {
    ($($t:ty)*) => ($(
        impl EstimateSize for $t {
            fn estimated_heap_size(&self) -> usize { 0 }
        }

        impl EstimateSize for Vec<$t> {
            fn estimated_heap_size(&self) -> usize { std::mem::size_of::<$t>() * self.len() }
        }
    )*)
}

estimate_size_impl! { usize u8 u16 u32 u64 u128 isize i8 i16 i32 i64 i128 f32 f64 }
