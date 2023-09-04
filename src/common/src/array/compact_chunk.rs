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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::BuildHasherDefault;

use itertools::Itertools;
use prehash::{new_prehashed_map, new_prehashed_map_with_capacity, Passthru, Prehashed};

use super::stream_chunk::{OpRowMutRef, StreamChunkMut};
use super::DataChunk;
use crate::array::{Op, RowRef, StreamChunk};
use crate::buffer::BitmapBuilder;
use crate::row::{Project, RowExt};
use crate::util::chunk_coalesce::DataChunkBuilder;
use crate::util::hash_util::Crc32FastBuilder;

struct StreamChunkCompactorOwner {
    chunks: Vec<StreamChunk>,
    stream_key: Vec<usize>,
}

pub struct StreamChunkCompactor {
    chunks: Vec<StreamChunk>,
    stream_key: Vec<usize>,
}

struct OpRowMutRefTuple<'a> {
    previous: Option<OpRowMutRef<'a>>,
    latest: OpRowMutRef<'a>,
}

impl<'a> OpRowMutRefTuple<'a> {
    fn push(&mut self, mut op_row: OpRowMutRef<'a>) -> bool {
        match (self.latest.op(), op_row.op()) {
            (Op::Insert, Op::Insert) => panic!(
                "receive duplicated insert on the stream. old valie {:?}, new value {:?}",
                self.latest, op_row
            ),
            (Op::Delete, Op::Delete) => panic!(
                "receive duplicated delete on the stream. old valie {:?}, new value {:?}",
                self.latest, op_row
            ),
            (Op::Insert, Op::Delete) => {
                self.latest.set_vis(false);
                op_row.set_vis(false);
                self.latest = if let Some(prev) = self.previous.take() {
                    prev
                } else {
                    return true;
                }
            }
            (Op::Delete, Op::Insert) => {
                // The operation for the key must be (+, -, +) or (-, +). And the (+, -) must has
                // been filtered.
                debug_assert!(self.previous.is_none());

                todo!();
            }
            // `all the updateDelete` and `updateInsert` should be normalized to `delete`
            // and`insert`
            _ => unreachable!(),
        };
        false
    }
}

type OpRowMap<'a, 'b> =
    HashMap<Prehashed<Project<'b, RowRef<'a>>>, OpRowMutRefTuple<'a>, BuildHasherDefault<Passthru>>;

impl StreamChunkCompactor {
    pub fn new(stream_key: Vec<usize>) -> Self {
        Self {
            stream_key,
            chunks: vec![],
        }
    }

    pub fn into_inner(self) -> (Vec<StreamChunk>, Vec<usize>) {
        (self.chunks, self.stream_key)
    }

    pub fn push_chunk(&mut self, c: StreamChunk) {
        self.chunks.push(c);
    }

    /// Compact a chunk by modifying the ops and the visibility of a stream chunk. All UPDATE INSERT
    /// and UPDATE DELETE will be converted to INSERT and DELETE, and dropped according to
    /// certain rules (see `merge_insert` and `merge_delete` for more details).

    pub fn into_compacted_chunks(self) -> Vec<StreamChunk> {
        let (chunks, key_indices) = self.into_inner();

        let estimate_size = chunks.iter().map(|c| c.cardinality()).sum();
        let mut chunks: Vec<(Vec<u64>, StreamChunkMut)> = chunks
            .into_iter()
            .map(|c| {
                let hash_values = c
                    .data_chunk()
                    .get_hash_values(&key_indices, Crc32FastBuilder)
                    .into_iter()
                    .map(|hash| hash.value())
                    .collect_vec();
                (hash_values, StreamChunkMut::from(c))
            })
            .collect_vec();
        {
            let mut op_row_map: OpRowMap = new_prehashed_map_with_capacity(estimate_size);
            for (hash_values, c) in &mut chunks {
                for mut r in c.to_mut_rows() {
                    if !r.vis() {
                        continue;
                    }
                    r.set_op(r.op().normalize_update());
                    let hash = hash_values[r.index()];
                    let stream_key = r.row_ref().project(&key_indices);
                    match op_row_map.entry(Prehashed::new(stream_key, hash)) {
                        Entry::Vacant(v) => {
                            v.insert(OpRowMutRefTuple {
                                previous: None,
                                latest: r,
                            });
                        }
                        Entry::Occupied(mut o) => {
                            if o.get_mut().push(r) {
                                o.remove_entry();
                            }
                        }
                    }
                }
            }
        }
        chunks.into_iter().map(|(_, c)| c.into()).collect_vec()
    }
}

pub fn merge_chunk_row(stream_chunk: StreamChunk, pk_indices: &[usize]) -> StreamChunk {
    let mut compactor = StreamChunkCompactor::new(pk_indices.to_vec());
    compactor.push_chunk(stream_chunk);
    compactor
        .into_compacted_chunks()
        .into_iter()
        .next()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::StreamChunk;
    use crate::test_prelude::StreamChunkTestExt;

    #[test]
    fn test_merge_chunk_row() {
        let pk_indices = [0, 1];

        let chunk = StreamChunk::from_pretty(
            " I I I
            + 2 5 7
            + 4 9 2
            - 2 5 7
            - 6 6 9
            + 6 6 6",
        );
        let compacted_chunk = merge_chunk_row(chunk, &pk_indices);
        assert_eq!(
            compacted_chunk.compact(),
            StreamChunk::from_pretty(
                " I I I
                + 4 9 2
                - 6 6 9
                + 6 6 6",
            )
        );

        let chunk = StreamChunk::from_pretty(
            " I I I
            + 2 5 7
            + 2 5 1
            - 9 7 3",
        );
        let compacted_chunk = merge_chunk_row(chunk, &pk_indices);
        assert_eq!(
            compacted_chunk.compact(),
            StreamChunk::from_pretty(
                " I I I
                + 2 5 1
                - 9 7 3",
            )
        );

        let chunk = StreamChunk::from_pretty(
            "  I I I
            +  4 9 2
            U- 2 5 1
            U+ 2 5 1
            U- 4 9 2
            U+ 4 9 9",
        );
        let compacted_chunk = merge_chunk_row(chunk, &pk_indices);
        assert_eq!(
            compacted_chunk.compact(),
            StreamChunk::from_pretty(
                " I I I
                + 4 9 9",
            )
        );
    }
}
