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

use std::alloc::{Allocator, Global};

use derivative::Derivative;

use super::Row2;

/// `CompactedRow` is used in streaming executors' cache, which takes less memory than `Vec<Datum>`.
/// Executors need to serialize Row into `CompactedRow` before writing into cache.
#[derive(Clone, Derivative)]
#[derivative(
    Debug(bound = ""),
    Hash(bound = ""),
    PartialEq(bound = ""),
    Eq(bound = "")
)]
pub struct CompactedRow<A: Allocator = Global> {
    // TODO: make this private, so that we don't rely on the internal representation is the same as
    // a value-serialized row.
    pub row: Vec<u8, A>,
}

impl<A: Allocator> CompactedRow<A> {
    /// Create a new compacted row from a value-serialized row.
    pub fn from_value_serialized(serialized: Vec<u8, A>) -> Self {
        Self { row: serialized }
    }

    /// Create a new compacted row from a row, with the given allocator.
    pub fn new_in(row: impl Row2, alloc: A) -> Self {
        Self {
            row: row.value_serialize_in(alloc),
        }
    }
}

impl<A: Allocator + Default> CompactedRow<A> {
    /// Create a new compacted row from a row.
    pub fn new(row: impl Row2) -> Self {
        Self::new_in(row, A::default())
    }
}

impl<A: Allocator + Default, R: Row2> From<R> for CompactedRow<A> {
    // TODO: remove this and use explicit `new` or `new_in`.
    fn from(row: R) -> Self {
        Self::new(row)
    }
}
