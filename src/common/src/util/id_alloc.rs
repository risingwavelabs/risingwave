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

use std::ops::Add;

use num_traits::{CheckedAdd, One};

#[derive(Debug, Clone, Copy)]
pub struct IdAllocator<T> {
    next: T,
}

impl<T> IdAllocator<T>
where
    T: One,
{
    pub fn new() -> Self {
        Self { next: T::one() }
    }
}

impl<T> Default for IdAllocator<T>
where
    T: One,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> IdAllocator<T>
where
    T: Copy + Add<Output = T> + One,
{
    pub fn next(&mut self) -> T {
        let next_next = self.next + T::one();
        std::mem::replace(&mut self.next, next_next)
    }
}

impl<T> IdAllocator<T>
where
    T: CheckedAdd + One,
{
    pub fn checked_next(&mut self) -> Option<T> {
        let next_next = self.next.checked_add(&T::one())?;
        Some(std::mem::replace(&mut self.next, next_next))
    }
}
