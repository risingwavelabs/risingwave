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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Bound;

use await_tree::InstrumentAwait;
use futures::Stream;
use futures_async_stream::try_stream;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::Datum;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::sort_util::{cmp_datum_iter, OrderType};
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_storage::table::collect_data_chunk;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTableInner;
use crate::executor::{
    Message, PkIndicesRef, StreamExecutorError, StreamExecutorResult, Watermark,
};

pub type CurrentPosMap = HashMap<VirtualNode, OwnedRow>;

pub struct BackfillState {
    /// Used to track backfill progress.
    backfill_progress: HashMap<VirtualNode, BackfillProgressPerVnode>,

    /// We need this to process state updates.
    committed_progress: HashMap<VirtualNode, Option<OwnedRow>>,
}

/// Used for tracking backfill state per vnode
#[derive(Eq, PartialEq, Debug)]
pub enum BackfillProgressPerVnode {
    NotStarted,
    InProgress(OwnedRow),
    Completed,
}

pub(crate) fn mark_chunk(
    chunk: StreamChunk,
    current_pos: &OwnedRow,
    pk_in_output_indices: PkIndicesRef<'_>,
    pk_order: &[OrderType],
) -> StreamChunk {
    let chunk = chunk.compact();
    mark_chunk_inner(chunk, current_pos, pk_in_output_indices, pk_order)
}

/// Mark chunk:
/// For each row of the chunk, forward it to downstream if its pk <= `current_pos` for the
/// corresponding `vnode`, otherwise ignore it.
/// We implement it by changing the visibility bitmap.
pub(crate) fn mark_chunk_ref_by_vnode(
    chunk: &StreamChunk,
    current_pos_map: &HashMap<VirtualNode, OwnedRow>,
    pk_in_output_indices: PkIndicesRef<'_>,
    pk_order: &[OrderType],
) -> StreamChunk {
    let chunk = chunk.clone();
    let (data, ops) = chunk.into_parts();
    let mut new_visibility = BitmapBuilder::with_capacity(ops.len());
    // Use project to avoid allocation.
    for v in data.rows().map(|row| {
        // TODO(kwannoel): Is this logic correct for compute vnode?
        // I will revisit it again when arrangement_backfill is implemented e2e.
        let vnode = VirtualNode::compute_row(row, pk_in_output_indices);
        let current_pos = current_pos_map.get(&vnode).unwrap();
        let lhs = row.project(pk_in_output_indices);
        let rhs = current_pos.project(pk_in_output_indices);
        let order = cmp_datum_iter(lhs.iter(), rhs.iter(), pk_order.iter().copied());
        match order {
            Ordering::Less | Ordering::Equal => true,
            Ordering::Greater => false,
        }
    }) {
        new_visibility.append(v);
    }
    let (columns, _) = data.into_parts();
    StreamChunk::new(ops, columns, Some(new_visibility.finish()))
}

/// Mark chunk:
/// For each row of the chunk, forward it to downstream if its pk <= `current_pos`, otherwise
/// ignore it. We implement it by changing the visibility bitmap.
fn mark_chunk_inner(
    chunk: StreamChunk,
    current_pos: &OwnedRow,
    pk_in_output_indices: PkIndicesRef<'_>,
    pk_order: &[OrderType],
) -> StreamChunk {
    let (data, ops) = chunk.into_parts();
    let mut new_visibility = BitmapBuilder::with_capacity(ops.len());
    // Use project to avoid allocation.
    for v in data.rows().map(|row| {
        let lhs = row.project(pk_in_output_indices);
        let rhs = current_pos.project(pk_in_output_indices);
        let order = cmp_datum_iter(lhs.iter(), rhs.iter(), pk_order.iter().copied());
        match order {
            Ordering::Less | Ordering::Equal => true,
            Ordering::Greater => false,
        }
    }) {
        new_visibility.append(v);
    }
    let (columns, _) = data.into_parts();
    StreamChunk::new(ops, columns, Some(new_visibility.finish()))
}

/// Builds a new stream chunk with `output_indices`.
pub(crate) fn mapping_chunk(chunk: StreamChunk, output_indices: &[usize]) -> StreamChunk {
    let (ops, columns, visibility) = chunk.into_inner();
    let mapped_columns = output_indices.iter().map(|&i| columns[i].clone()).collect();
    StreamChunk::new(ops, mapped_columns, visibility)
}

fn mapping_watermark(watermark: Watermark, upstream_indices: &[usize]) -> Option<Watermark> {
    watermark.transform_with_indices(upstream_indices)
}

pub(crate) fn mapping_message(msg: Message, upstream_indices: &[usize]) -> Option<Message> {
    match msg {
        Message::Barrier(_) => Some(msg),
        Message::Watermark(watermark) => {
            mapping_watermark(watermark, upstream_indices).map(Message::Watermark)
        }
        Message::Chunk(chunk) => Some(Message::Chunk(mapping_chunk(chunk, upstream_indices))),
    }
}

/// Gets progress per vnode, so we know which to backfill.
pub(crate) async fn get_progress_per_vnode<S: StateStore, const IS_REPLICATED: bool>(
    state_table: &StateTableInner<S, BasicSerde, IS_REPLICATED>,
    state_len: usize,
) -> StreamExecutorResult<Vec<BackfillProgressPerVnode>> {
    debug_assert!(!state_table.vnode_bitmap().is_empty());
    let vnodes = state_table.vnodes().iter_vnodes_scalar();
    let mut result = Vec::with_capacity(state_table.vnodes().len());
    for vnode in vnodes {
        let vnode_key: &[Datum] = &[Some(vnode.into())];
        let state_for_vnode_key = state_table.get_row(vnode_key).await?;

        // original_backfill_datum_pos = (state_len - 1)
        // value indices are set, so we can -1 for the pk (a single vnode).
        let backfill_datum_pos = state_len - 2;
        let backfill_progress = match state_for_vnode_key {
            Some(row) => {
                let vnode_is_finished = row.datum_at(backfill_datum_pos).unwrap();
                if vnode_is_finished.into_bool() {
                    BackfillProgressPerVnode::Completed
                } else {
                    BackfillProgressPerVnode::InProgress(row)
                }
            }
            None => BackfillProgressPerVnode::NotStarted,
        };
        result.push(backfill_progress);
    }
    Ok(result)
}

/// All vnodes should be persisted with status finished.
pub(crate) async fn check_all_vnode_finished<S: StateStore, const IS_REPLICATED: bool>(
    state_table: &StateTableInner<S, BasicSerde, IS_REPLICATED>,
    state_len: usize,
) -> StreamExecutorResult<bool> {
    debug_assert!(!state_table.vnode_bitmap().is_empty());
    let vnodes = state_table.vnodes().iter_vnodes_scalar();
    let mut is_finished = true;
    for vnode in vnodes {
        let key: &[Datum] = &[Some(vnode.into())];
        let row = state_table.get_row(key).await?;

        // original_backfill_datum_pos = (state_len - 1)
        // value indices are set, so we can -1 for the pk (a single vnode).
        let backfill_datum_pos = state_len - 2;
        let vnode_is_finished = if let Some(row) = row
            && let Some(vnode_is_finished) = row.datum_at(backfill_datum_pos)
        {
            vnode_is_finished.into_bool()
        } else {
            false
        };
        if !vnode_is_finished {
            is_finished = false;
            break;
        }
    }
    Ok(is_finished)
}

/// Flush the data
pub(crate) async fn flush_data<S: StateStore, const IS_REPLICATED: bool>(
    table: &mut StateTableInner<S, BasicSerde, IS_REPLICATED>,
    epoch: EpochPair,
    old_state: &mut Option<Vec<Datum>>,
    current_partial_state: &mut [Datum],
) -> StreamExecutorResult<()> {
    let vnodes = table.vnodes().clone();
    if let Some(old_state) = old_state {
        if old_state[1..] == current_partial_state[1..] {
            table.commit_no_data_expected(epoch);
            return Ok(());
        } else {
            vnodes.iter_vnodes_scalar().for_each(|vnode| {
                let datum = Some(vnode.into());
                current_partial_state[0] = datum.clone();
                old_state[0] = datum;
                table.write_record(Record::Update {
                    old_row: &old_state[..],
                    new_row: &(*current_partial_state),
                })
            });
        }
    } else {
        // No existing state, create a new entry.
        vnodes.iter_vnodes_scalar().for_each(|vnode| {
            let datum = Some(vnode.into());
            // fill the state
            current_partial_state[0] = datum;
            table.write_record(Record::Insert {
                new_row: &(*current_partial_state),
            })
        });
    }
    table.commit(epoch).await
}

// We want to avoid building a row for every vnode.
// Instead we can just modify a single row, and dispatch it to state table to write.
pub(crate) fn build_temporary_state(
    row_state: &mut [Datum],
    is_finished: bool,
    current_pos: &OwnedRow,
) {
    row_state[1..current_pos.len() + 1].clone_from_slice(current_pos.as_inner());
    row_state[current_pos.len() + 1] = Some(is_finished.into());
}

pub(crate) fn update_pos_per_vnode(
    chunk: &StreamChunk,
    pk_in_output_indices: &[usize],
) -> Option<OwnedRow> {
    Some(
        chunk
            .rows()
            .last()
            .unwrap()
            .1
            .project(pk_in_output_indices)
            .into_owned_row(),
    )
}

/// Update backfill pos by vnode.
pub(crate) fn update_pos_by_vnode(
    vnode: VirtualNode,
    chunk: &StreamChunk,
    pk_in_output_indices: &[usize],
    current_pos_map: &mut CurrentPosMap,
) {
    let new_pos = get_new_pos(chunk, pk_in_output_indices);
    current_pos_map.insert(vnode, new_pos);
}

/// Get new backfill pos from the chunk. Since chunk should have ordered rows, we can just take the
/// last row.
pub(crate) fn get_new_pos(chunk: &StreamChunk, pk_in_output_indices: &[usize]) -> OwnedRow {
    chunk
        .rows()
        .last()
        .unwrap()
        .1
        .project(pk_in_output_indices)
        .into_owned_row()
}

// NOTE(kwannoel): ["None" ..] encoding should be appropriate to mark
// the case where upstream snapshot is empty.
// This is so we can persist backfill state as "finished".
// It won't be confused with another case where pk position comprised of nulls,
// because they both record that backfill is finished.
pub(crate) fn construct_initial_finished_state(pos_len: usize) -> Option<OwnedRow> {
    Some(OwnedRow::new(vec![None; pos_len]))
}

pub(crate) fn compute_bounds(
    pk_indices: &[usize],
    current_pos: Option<OwnedRow>,
) -> Option<(Bound<OwnedRow>, Bound<OwnedRow>)> {
    // `current_pos` is None means it needs to scan from the beginning, so we use Unbounded to
    // scan. Otherwise, use Excluded.
    if let Some(current_pos) = current_pos {
        // If `current_pos` is an empty row which means upstream mv contains only one row and it
        // has been consumed. The iter interface doesn't support
        // `Excluded(empty_row)` range bound, so we can simply return `None`.
        if current_pos.is_empty() {
            assert!(pk_indices.is_empty());
            return None;
        }

        Some((Bound::Excluded(current_pos), Bound::Unbounded))
    } else {
        Some((Bound::Unbounded, Bound::Unbounded))
    }
}

#[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
pub(crate) async fn iter_chunks<'a, S, E>(
    mut iter: S,
    upstream_table_schema: &'a Schema,
    chunk_size: usize,
) where
    StreamExecutorError: From<E>,
    S: Stream<Item = Result<OwnedRow, E>> + Unpin + 'a,
{
    while let Some(data_chunk) =
        collect_data_chunk(&mut iter, upstream_table_schema, Some(chunk_size))
            .instrument_await("backfill_snapshot_read")
            .await?
    {
        debug_assert!(data_chunk.cardinality() > 0);
        let ops = vec![Op::Insert; data_chunk.capacity()];
        let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
        yield Some(stream_chunk);
    }

    yield None;
}

/// Schema
/// | vnode | pk | `backfill_finished` |
///
/// `current_pos_map` is the map from vnode to the current backfilled pk position.
pub(crate) async fn persist_state_per_vnode<S: StateStore, const IS_REPLICATED: bool>(
    epoch: EpochPair,
    table: &mut StateTableInner<S, BasicSerde, IS_REPLICATED>,
    is_finished: bool,
    current_pos_map: &CurrentPosMap,
    old_state: &mut Option<Vec<Datum>>,
    current_state: &mut [Datum],
) -> StreamExecutorResult<()> {
    if current_pos_map.is_empty() {
        table.commit_no_data_expected(epoch);
    }
    for current_pos in current_pos_map.values() {
        // state w/o vnodes.
        build_temporary_state(current_state, is_finished, current_pos);
        flush_data(table, epoch, old_state, current_state).await?;
        // FIXME
        *old_state = Some(current_state.into());
    }
    Ok(())
}

/// Schema
/// | vnode | pk | `backfill_finished` |
///
/// For `current_pos` and `old_pos` are just pk of upstream.
/// They should be strictly increasing.
pub(crate) async fn persist_state<S: StateStore, const IS_REPLICATED: bool>(
    epoch: EpochPair,
    table: &mut StateTableInner<S, BasicSerde, IS_REPLICATED>,
    is_finished: bool,
    current_pos: &Option<OwnedRow>,
    old_state: &mut Option<Vec<Datum>>,
    current_state: &mut [Datum],
) -> StreamExecutorResult<()> {
    if let Some(current_pos_inner) = current_pos {
        // state w/o vnodes.
        build_temporary_state(current_state, is_finished, current_pos_inner);
        flush_data(table, epoch, old_state, current_state).await?;
        *old_state = Some(current_state.into());
    } else {
        table.commit_no_data_expected(epoch);
    }
    Ok(())
}
