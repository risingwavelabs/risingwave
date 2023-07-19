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
use std::collections::HashMap;

use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::buffer::BitmapBuilder;

/// Compact a chunk by modifying the ops and the visibility of a stream chunk. All UPDATE INSERT and
/// UPDATE DELETE will be converted to INSERT and DELETE, and dropped according to certain rules
/// (see `compact_insert` and `compact_delete` for more details).
pub fn compact_chunk(stream_chunk: StreamChunk) -> StreamChunk {
    let mut chunk_cache = HashMap::new();
    let mut bitmap_builder = None;
    let mut ops = None;

    for (op, row) in stream_chunk.rows() {
        match op {
            Op::Insert => {
                compact_insert(&stream_chunk, row, &mut chunk_cache, &mut bitmap_builder);
            }
            Op::Delete => {
                compact_delete(&stream_chunk, row, &mut chunk_cache, &mut bitmap_builder);
            }
            Op::UpdateDelete => {
                if ops.is_none() {
                    // Lazily initialize ops to save one clone if the chunk needn't be modified.
                    ops = Some(stream_chunk.ops().to_vec());
                };
                ops.as_mut().unwrap()[row.index()] = Op::Delete;
                compact_delete(&stream_chunk, row, &mut chunk_cache, &mut bitmap_builder);
            }
            Op::UpdateInsert => {
                if ops.is_none() {
                    ops = Some(stream_chunk.ops().to_vec());
                };
                ops.as_mut().unwrap()[row.index()] = Op::Insert;
                compact_insert(&stream_chunk, row, &mut chunk_cache, &mut bitmap_builder);
            }
        }
    }

    match (bitmap_builder, ops) {
        (Some(bitmap_builder), Some(ops)) => {
            let (_, columns, _) = stream_chunk.into_inner();
            StreamChunk::new(ops, columns, Some(bitmap_builder.finish()))
        }
        (Some(bitmap_builder), None) => {
            let (ops, columns, _) = stream_chunk.into_inner();
            StreamChunk::new(ops, columns, Some(bitmap_builder.finish()))
        }
        (None, Some(ops)) => {
            let (_, columns, vis) = stream_chunk.into_inner();
            StreamChunk::new(ops, columns, vis)
        }
        (None, None) => stream_chunk,
    }
}

fn compact_insert<'a>(
    chunk: &StreamChunk,
    row: RowRef<'a>,
    chunk_cache: &mut HashMap<RowRef<'a>, Op>,
    bitmap_builder: &mut Option<BitmapBuilder>,
) {
    match chunk_cache.entry(row) {
        Entry::Vacant(v) => {
            v.insert(Op::Insert);
        }
        Entry::Occupied(o) => match o.get() {
            Op::Insert => {
                // INSERT K, INSERT K => INSERT K, INSERT K (invis)
                if bitmap_builder.is_none() {
                    // Lazily initialize the bitmap builder to save one clone if the chunk needn't
                    // be modified.
                    *bitmap_builder = Some(BitmapBuilder::from_bitmap(
                        &chunk.data_chunk().vis().to_bitmap(),
                    ))
                }
                bitmap_builder.as_mut().unwrap().set(row.index(), false);
            }
            Op::Delete => {
                // DELETE K, INSERT K => DELETE K (invis), INSERT K (invis)
                if bitmap_builder.is_none() {
                    *bitmap_builder = Some(BitmapBuilder::from_bitmap(
                        &chunk.data_chunk().vis().to_bitmap(),
                    ))
                }
                let (deleted_row, _) = o.remove_entry();
                bitmap_builder
                    .as_mut()
                    .unwrap()
                    .set(deleted_row.index(), false);
                bitmap_builder.as_mut().unwrap().set(row.index(), false);
            }
            _ => {
                unreachable!();
            }
        },
    }
}

fn compact_delete<'a>(
    chunk: &StreamChunk,
    row: RowRef<'a>,
    chunk_cache: &mut HashMap<RowRef<'a>, Op>,
    bitmap_builder: &mut Option<BitmapBuilder>,
) {
    match chunk_cache.entry(row) {
        Entry::Vacant(v) => {
            v.insert(Op::Delete);
        }
        Entry::Occupied(o) => match o.get() {
            Op::Insert => {
                // INSERT K, DELETE K => INSERT K (invis), DELETE K (invis)
                if bitmap_builder.is_none() {
                    *bitmap_builder = Some(BitmapBuilder::from_bitmap(
                        &chunk.data_chunk().vis().to_bitmap(),
                    ))
                }
                let (inserted_row, _) = o.remove_entry();
                bitmap_builder
                    .as_mut()
                    .unwrap()
                    .set(inserted_row.index(), false);
                bitmap_builder.as_mut().unwrap().set(row.index(), false);
            }
            Op::Delete => {
                // DELETE K, DELETE K => DELETE K, DELETE K (invis)
                if bitmap_builder.is_none() {
                    *bitmap_builder = Some(BitmapBuilder::from_bitmap(
                        &chunk.data_chunk().vis().to_bitmap(),
                    ))
                }
                bitmap_builder.as_mut().unwrap().set(row.index(), false);
            }
            _ => {
                unreachable!();
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::StreamChunk;
    use risingwave_common::test_prelude::StreamChunkTestExt;

    use super::compact_chunk;

    #[test]
    fn test_compact_chunk() {
        let chunk = StreamChunk::from_pretty(
            " I I I
            + 2 5 1
            + 4 9 2
            - 2 5 1
            - 6 6 6
            + 6 6 6",
        );
        let compacted_chunk = compact_chunk(chunk);
        assert_eq!(
            compacted_chunk.compact(),
            StreamChunk::from_pretty(
                " I I I
                + 4 9 2",
            )
        );

        let chunk = StreamChunk::from_pretty(
            " I I I
            + 2 5 1
            + 2 5 1
            - 9 7 3
            - 9 7 3",
        );
        let compacted_chunk = compact_chunk(chunk);
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
            U+ 5 5 5",
        );
        let compacted_chunk = compact_chunk(chunk);
        assert_eq!(
            compacted_chunk.compact(),
            StreamChunk::from_pretty(
                " I I I
                + 5 5 5",
            )
        );

        let chunk = StreamChunk::from_pretty(
            "  I I I
            U- 8 7 3
            U+ 5 5 5
            +  8 7 3
            U- 5 5 5
            U+ 8 7 3",
        );
        let compacted_chunk = compact_chunk(chunk);
        assert_eq!(
            compacted_chunk.compact(),
            StreamChunk::from_pretty(
                " I I I
                + 8 7 3",
            )
        );
    }
}
