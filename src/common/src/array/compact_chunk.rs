use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use crate::array::data_chunk_iter::PkRef;
use crate::array::{Op, RowRef, StreamChunk};
use crate::buffer::BitmapBuilder;
use crate::util::chunk_coalesce::DataChunkBuilder;

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

/// Convert DELETE and INSERT on the same key to UPDATE messages.
///
/// This function must be called on a chunk with INSERT and DELETE ops only.
pub fn gen_update_from_pk(pk_indices: &[usize], chunk: StreamChunk) -> StreamChunk {
    let mut delete_cache = HashSet::new();
    let mut insert_cache = HashSet::new();
    for (op, row) in chunk.rows() {
        let key = PkRef::new(row, pk_indices);
        match op {
            Op::Insert => {
                insert_cache.insert(key);
            }
            Op::Delete => {
                if let Some(inserted_pk) = insert_cache.take(&key) {
                    if inserted_pk.row() != key.row() {
                        // It's invalid that `key` has already had an inserted value and this value
                        // is different from the value to be deleted here.
                        panic!("deleting a non-existing record {:?}", key.row());
                    }
                } else {
                    if !delete_cache.contains(&key) {
                        delete_cache.insert(key);
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    let mut chunk_builder =
        DataChunkBuilder::new(chunk.data_chunk().data_types(), chunk.cardinality() + 1);
    let mut ops = Vec::with_capacity(chunk.cardinality());

    for deleted_pk in delete_cache.into_iter() {
        if let Some(inserted_pk) = insert_cache.take(&deleted_pk) {
            //
            ops.push(Op::UpdateDelete);
            let returned_chunk = chunk_builder.append_one_row(deleted_pk.row());
            debug_assert_eq!(returned_chunk, None);

            ops.push(Op::UpdateInsert);
            let returned_chunk = chunk_builder.append_one_row(inserted_pk.row());
            debug_assert_eq!(returned_chunk, None);
        } else {
            ops.push(Op::Delete);
            let returned_chunk = chunk_builder.append_one_row(deleted_pk.row());
            debug_assert_eq!(returned_chunk, None);
        }
    }

    for inserted_pk in insert_cache.into_iter() {
        ops.push(Op::Insert);
        let returned_chunk = chunk_builder.append_one_row(inserted_pk.row());
        debug_assert_eq!(returned_chunk, None);
    }

    StreamChunk::from_data_chunk(ops, chunk_builder.consume_all().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::StreamChunk;
    use crate::test_prelude::StreamChunkTestExt;

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

    #[test]
    fn test_gen_update_from_pk() {
        let pk_indices = [0, 1];

        let chunk = StreamChunk::from_pretty(
            " I I I
            + 2 5 1
            + 4 9 2
            - 2 5 1",
        );
        assert_eq!(
            gen_update_from_pk(&pk_indices, chunk),
            StreamChunk::from_pretty(
                " I I I
                + 4 9 2",
            )
        );

        let chunk = StreamChunk::from_pretty(
            " I I I
            - 2 5 1
            + 6 6 6
            + 2 5 3",
        );
        assert_eq!(
            gen_update_from_pk(&pk_indices, chunk),
            StreamChunk::from_pretty(
                "  I I I
                U- 2 5 1
                U+ 2 5 3
                 + 6 6 6",
            )
        );

        let chunk = StreamChunk::from_pretty(
            " I I I
            - 2 5 1
            + 4 9 2
            + 2 5 3
            - 2 5 3",
        );
        assert_eq!(
            gen_update_from_pk(&pk_indices, chunk),
            StreamChunk::from_pretty(
                " I I I
                - 2 5 1
                + 4 9 2",
            )
        );
    }
}
