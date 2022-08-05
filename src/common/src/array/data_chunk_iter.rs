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

use crate::array::DataChunk;
use crate::row::RowRef;

impl DataChunk {
    /// Get an iterator for visible rows.
    pub fn rows(&self) -> impl Iterator<Item = RowRef> {
        DataChunkRefIter {
            chunk: self,
            idx: Some(0),
        }
    }

    /// Get an iterator for all rows in the chunk, and a `None` represents an invisible row.
    pub fn rows_with_holes(&self) -> impl Iterator<Item = Option<RowRef>> {
        DataChunkRefIterWithHoles {
            chunk: self,
            idx: 0,
        }
    }
}

struct DataChunkRefIter<'a> {
    chunk: &'a DataChunk,
    /// `None` means finished
    idx: Option<usize>,
}

impl<'a> Iterator for DataChunkRefIter<'a> {
    type Item = RowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.idx {
            None => None,
            Some(idx) => {
                self.idx = self.chunk.next_visible_row_idx(idx);
                match self.idx {
                    None => None,
                    Some(idx) => {
                        self.idx = Some(idx + 1);
                        Some(RowRef::new(self.chunk, idx))
                    }
                }
            }
        }
    }
}

struct DataChunkRefIterWithHoles<'a> {
    chunk: &'a DataChunk,
    idx: usize,
}

impl<'a> Iterator for DataChunkRefIterWithHoles<'a> {
    type Item = Option<RowRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let len = self.chunk.capacity();
        let vis = self.chunk.vis();
        if self.idx == len {
            None
        } else {
            let ret = Some(if !vis.is_set(self.idx) {
                None
            } else {
                Some(RowRef::new(self.chunk, self.idx))
            });
            self.idx += 1;
            ret
        }
    }
}
