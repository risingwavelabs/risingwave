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

use super::Row;
use crate::types::DatumRef;

/// A row that is backed by a byte buffer with contiguous memory layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BRow {
    data: Box<[u8]>,
}

impl BRow {
    /// Converts from `&BRow` to `BRowRef<'_>`.
    pub fn as_ref(&self) -> BRowRef<'_> {
        BRowRef { data: &self.data }
    }
}

/// Forwards to `BRowRef`.
impl Row for BRow {
    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        self.as_ref().datum_at(index)
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        self.as_ref().datum_at_unchecked(index)
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn iter(&self) -> impl Iterator<Item = DatumRef<'_>> {
        self.as_ref().iter()
    }
}

/// Reference to a row that is backed by a byte buffer with contiguous memory layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BRowRef<'a> {
    data: &'a [u8],
}

/// Methods implemented on `self`.
impl<'a> BRowRef<'a> {
    // TODO: obtain length from the byte buffer.
    pub fn len(self) -> usize {
        todo!()
    }

    // TODO: deserialize the byte buffer with zero-copy.
    pub fn datum_at(self, _index: usize) -> DatumRef<'a> {
        todo!()
    }

    pub unsafe fn datum_at_unchecked(self, _index: usize) -> DatumRef<'a> {
        todo!()
    }

    pub fn iter(self) -> impl DoubleEndedIterator + ExactSizeIterator<Item = DatumRef<'a>> {
        (0..self.len()).map(move |i| self.datum_at(i))
    }
}

impl<'a> Row for BRowRef<'a> {
    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        (*self).datum_at(index)
    }

    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        (*self).datum_at_unchecked(index)
    }

    fn len(&self) -> usize {
        (*self).len()
    }

    fn iter(&self) -> impl DoubleEndedIterator + ExactSizeIterator<Item = DatumRef<'_>> {
        (*self).iter()
    }
}
