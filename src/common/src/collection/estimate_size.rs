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

impl EstimateSize for Vec<u8> {
    fn estimated_heap_size(&self) -> usize {
        self.capacity()
    }
}

impl<T> EstimateSize for Vec<T>
where
    T: EstimateSize,
{
    fn estimated_heap_size(&self) -> usize {
        self.capacity() * std::mem::size_of::<T>()
            + self
                .iter()
                .map(EstimateSize::estimated_heap_size)
                .sum::<usize>()
    }
}

impl EstimateSize for String {
    fn estimated_heap_size(&self) -> usize {
        self.capacity()
    }
}
