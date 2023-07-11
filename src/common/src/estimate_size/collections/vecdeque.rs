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

use std::collections::VecDeque;

use educe::Educe;

use crate::estimate_size::{EstimateSize, KvSize};

#[derive(Educe)]
#[educe(Default)]
pub struct EstimatedVecDeque<T: EstimateSize> {
    inner: VecDeque<T>,
    heap_size: KvSize,
}

impl<T: EstimateSize> EstimatedVecDeque<T> {
    pub fn pop_back(&mut self) -> Option<T> {
        self.inner.pop_back().inspect(|v| {
            self.heap_size.sub_val(v);
        })
    }

    pub fn pop_front(&mut self) -> Option<T> {
        self.inner.pop_front().inspect(|v| {
            self.heap_size.sub_val(v);
        })
    }

    pub fn push_back(&mut self, value: T) {
        self.heap_size.add_val(&value);
        self.inner.push_back(value)
    }

    pub fn push_front(&mut self, value: T) {
        self.heap_size.add_val(&value);
        self.inner.push_front(value)
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.inner.get(index)
    }

    pub fn front(&self) -> Option<&T> {
        self.inner.front()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<T: EstimateSize> std::ops::Index<usize> for EstimatedVecDeque<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.inner[index]
    }
}

impl<T: EstimateSize> EstimateSize for EstimatedVecDeque<T> {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Add `VecDeque` internal size.
        // https://github.com/risingwavelabs/risingwave/issues/9713
        self.heap_size.size()
    }
}
