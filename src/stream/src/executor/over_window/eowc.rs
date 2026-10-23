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

use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Bound;

use anyhow::Context;
use futures::{TryStreamExt, stream};
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::array::stream_record::Record;
use risingwave_common::row::RowExt;
use risingwave_common::types::ToOwnedDatum;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::memcmp_encoding::{self, MemcmpEncoded};
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::{must_match, row};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_common_estimate_size::collections::EstimatedVecDeque;
use risingwave_expr::window_function::{
    Readiness, StateEvictHint, StateKey, WatermarkBound, WindowFuncCall, WindowStateSnapshot,
    WindowStates, create_window_state,
};
use risingwave_pb::stream_plan::PbEowcOverWindowNodeVersion;
use risingwave_pb::window_function::{
    EowcPartitionProgress as PbEowcPartitionProgress, StateKey as PbStateKey,
    WatermarkBound as PbWatermarkBound, WindowStateSnapshot as PbWindowStateSnapshot,
};
use risingwave_storage::store::PrefetchOptions;
use tracing::debug;

use super::watermark_trigger::{TriggerEntry, WatermarkTriggers};
use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::consistency::consistency_panic;
use crate::executor::prelude::*;

struct Partition {
    states: WindowStates,
    curr_row_buffer: EstimatedVecDeque<OwnedRow>,
    /// Cached intermediate state row for this partition, used for upsert operations.
    /// `None` means no prior row exists in the intermediate state table for this partition.
    intermediate_state_row: Option<OwnedRow>,
    /// Only maintained when windows may be closed by watermark.
    progress: PartitionProgress,
    /// Whether the intermediate states or the progress changed since persisted.
    dirty: bool,
}

impl EstimateSize for Partition {
    fn estimated_heap_size(&self) -> usize {
        let mut total_size = self.curr_row_buffer.estimated_heap_size();
        for state in self.states.iter() {
            total_size += state.estimated_heap_size();
        }
        if let Some(row) = &self.intermediate_state_row {
            total_size += row.estimated_heap_size();
        }
        total_size + self.progress.estimated_heap_size()
    }
}

/// Output progress of a partition, persisted in the `progress` column of the intermediate state
/// table.
#[derive(Default, EstimateSize)]
struct PartitionProgress {
    /// The key of the last output row. Rows up to it may be retained for the windows of later rows,
    /// and are skipped by it when reloading the partition, as whether a window was closed by
    /// watermark can't be told from the rows.
    last_output_key: Option<StateKey>,
    /// The bound of the trigger registered in [`WatermarkTriggers`]. It's never later than the
    /// watermark that closes the window of the next row to output, which only grows, so the trigger
    /// is kept as new rows arrive, and checked again when it fires.
    trigger: Option<WatermarkBound>,
}

type PartitionCache = ManagedLruCache<MemcmpEncoded, Partition>; // TODO(rc): use `K: HashKey` as key like in hash agg?

/// Number of partitions loaded concurrently.
const LOAD_CONCURRENCY: usize = 10;

fn encode_state_key(key: &StateKey, pk_ser: &OrderedRowSerde) -> PbStateKey {
    PbStateKey {
        order_key: key.order_key.to_vec(),
        pk: key.pk.as_inner().memcmp_serialize(pk_ser),
    }
}

fn decode_state_key(key: PbStateKey, pk_deser: &OrderedRowSerde) -> StreamExecutorResult<StateKey> {
    let pk = pk_deser
        .deserialize(&key.pk)
        .context("failed to deserialize pk")?;
    Ok(StateKey {
        order_key: key.order_key.into(),
        pk: pk.into(),
    })
}

/// Encode a [`WindowStateSnapshot`] to bytes for persistence using protobuf.
fn encode_snapshot(snapshot: &WindowStateSnapshot, pk_ser: &OrderedRowSerde) -> Vec<u8> {
    use prost::Message;
    let pb = PbWindowStateSnapshot {
        last_output_key: snapshot
            .last_output_key
            .as_ref()
            .map(|key| encode_state_key(key, pk_ser)),
        function_state: Some(snapshot.function_state.clone()),
    };
    pb.encode_to_vec()
}

/// Decode a [`WindowStateSnapshot`] from bytes during recovery using protobuf.
fn decode_snapshot(
    bytes: &[u8],
    pk_deser: &OrderedRowSerde,
) -> StreamExecutorResult<WindowStateSnapshot> {
    use prost::Message;
    let pb = PbWindowStateSnapshot::decode(bytes).context("failed to decode snapshot")?;
    let last_output_key = pb
        .last_output_key
        .map(|key| decode_state_key(key, pk_deser))
        .transpose()?;
    let function_state = pb
        .function_state
        .context("snapshot missing function_state")?;
    Ok(WindowStateSnapshot {
        last_output_key,
        function_state,
    })
}

/// Encode a [`PartitionProgress`] for persistence, or `None` if it's empty.
fn encode_progress(progress: &PartitionProgress, pk_ser: &OrderedRowSerde) -> Option<Vec<u8>> {
    use prost::Message;
    if progress.last_output_key.is_none() && progress.trigger.is_none() {
        return None;
    }
    let pb = PbEowcPartitionProgress {
        last_output_key: progress
            .last_output_key
            .as_ref()
            .map(|key| encode_state_key(key, pk_ser)),
        trigger: progress.trigger.as_ref().map(|bound| PbWatermarkBound {
            value: bound.value.to_vec(),
            exclusive: bound.exclusive,
        }),
    };
    Some(pb.encode_to_vec())
}

fn decode_progress(
    bytes: &[u8],
    pk_deser: &OrderedRowSerde,
) -> StreamExecutorResult<PartitionProgress> {
    use prost::Message;
    let pb = PbEowcPartitionProgress::decode(bytes).context("failed to decode progress")?;
    Ok(PartitionProgress {
        last_output_key: pb
            .last_output_key
            .map(|key| decode_state_key(key, pk_deser))
            .transpose()?,
        trigger: pb.trigger.map(|bound| WatermarkBound {
            value: bound.value.into(),
            exclusive: bound.exclusive,
        }),
    })
}

/// [`EowcOverWindowExecutor`] consumes ordered input (on order key column with watermark in
/// ascending order) and outputs window function results. One [`EowcOverWindowExecutor`] can handle
/// one combination of partition key and order key.
///
/// The reason not to use [`SortBuffer`] is that the table schemas of [`EowcOverWindowExecutor`] and
/// [`SortBuffer`] are different, since we don't have something like a _grouped_ sort buffer.
///
/// [`SortBuffer`]: crate::executor::eowc::SortBuffer
///
/// Basic idea:
///
/// ```text
/// ──────────────┬────────────────────────────────────────────────────── curr evict row
///               │ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
///        (1)    │ ─┬─
///               │  │RANGE BETWEEN '1hr' PRECEDING AND '1hr' FOLLOWING
///        ─┬─    │  │ ─┬─
///   LAG(1)│        │  │
/// ────────┴──┬─────┼──┼──────────────────────────────────────────────── curr output row
///     LEAD(1)│     │  │GROUPS 1 PRECEDING AND 1 FOLLOWING
///                  │
///                  │ (2)
/// ─────────────────┴─────────────────────────────────────────────────── curr input row
/// (1): additional buffered input (unneeded) for some window
/// (2): additional delay (already able to output) for some window
/// ```
///
/// - State table schema = input schema, state table pk = `partition key | order key | input pk`.
/// - Output schema = input schema + window function results.
/// - Rows in range (`curr evict row`, `curr input row`] are in state table.
/// - `curr evict row` <= min(last evict rows of all `WindowState`s).
/// - `WindowState` should output agg result for `curr output row`.
/// - Recover: iterate through state table, push rows to `WindowState`, and skip the rows that were
///   output, see [`Self::skip_output_rows`].
/// - Windows that may be closed by the watermark of the order key are closed by
///   [`WatermarkTriggers`].
pub struct EowcOverWindowExecutor<S: StateStore> {
    input: Executor,
    inner: ExecutorInner<S>,
}

struct ExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,

    schema: Schema,
    calls: Vec<WindowFuncCall>,
    input_stream_key: Vec<usize>,
    partition_key_indices: Vec<usize>,
    order_key_index: usize, // no `OrderType` here, cuz we expect the input is ascending
    state_table: StateTable<S>,
    /// For each column of the state table, its index in
    /// `partition key | order key | input stream key | NULL`, to rebuild the row of an evicted key.
    evict_row_indices: Vec<usize>,
    watermark_sequence: AtomicU64Ref,
    /// Optional state table for persisting window function intermediate states.
    /// See `StreamEowcOverWindow::infer_intermediate_state_table` for schema definition.
    intermediate_state_table: Option<StateTable<S>>,
    /// Serde for input stream key (pk), used for encoding/decoding `StateKey` in the intermediate
    /// state table. Only initialized when `intermediate_state_table` is present.
    pk_serde: Option<OrderedRowSerde>,
    /// Whether windows are judged exactly by [`WindowStates::readiness`], rather than the
    /// conservative [`WindowStates::are_ready`], see `EowcOverWindowNodeVersion`.
    exact_readiness: bool,
    /// Present iff windows may be closed by watermark, in which case `intermediate_state_table` has a
    /// trailing `progress` column.
    triggers: Option<WatermarkTriggers<S>>,
    chunk_size: usize,
}

impl<S: StateStore> ExecutorInner<S> {
    fn encode_partition_key(&self, partition_key: impl Row) -> StreamExecutorResult<MemcmpEncoded> {
        Ok(memcmp_encoding::encode_row(
            partition_key,
            &vec![OrderType::ascending(); self.partition_key_indices.len()],
        )?)
    }
}

struct ExecutionVars<S: StateStore> {
    partitions: PartitionCache,
    _phantom: PhantomData<S>,
}

impl<S: StateStore> Execute for EowcOverWindowExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.executor_inner().boxed()
    }
}

pub struct EowcOverWindowExecutorArgs<S: StateStore> {
    pub actor_ctx: ActorContextRef,

    pub input: Executor,

    pub schema: Schema,
    pub calls: Vec<WindowFuncCall>,
    pub partition_key_indices: Vec<usize>,
    pub order_key_index: usize,
    pub state_table: StateTable<S>,
    pub watermark_epoch: AtomicU64Ref,
    /// Optional state table for persisting window function intermediate states.
    /// See `StreamEowcOverWindow::infer_intermediate_state_table` for schema definition.
    pub intermediate_state_table: Option<StateTable<S>>,
    /// See `StreamEowcOverWindow::infer_watermark_trigger_table` for schema definition.
    pub watermark_trigger_table: Option<StateTable<S>>,
    pub version: PbEowcOverWindowNodeVersion,
    pub chunk_size: usize,
}

impl<S: StateStore> EowcOverWindowExecutor<S> {
    pub fn new(args: EowcOverWindowExecutorArgs<S>) -> Self {
        let input_info = args.input.info().clone();

        // Evictions rebuild state table rows from these columns only.
        debug_assert!(
            args.state_table.pk_indices().iter().all(|i| {
                args.partition_key_indices.contains(i)
                    || *i == args.order_key_index
                    || input_info.stream_key.contains(i)
            }),
            "state table pk must consist of partition key, order key and input stream key columns"
        );
        let num_partition_key_cols = args.partition_key_indices.len();
        let evict_row_indices = (0..input_info.schema.len())
            .map(|i| {
                if let Some(j) = args.partition_key_indices.iter().position(|&k| k == i) {
                    j
                } else if i == args.order_key_index {
                    num_partition_key_cols
                } else if let Some(j) = input_info.stream_key.iter().position(|&k| k == i) {
                    num_partition_key_cols + 1 + j
                } else {
                    // NULL
                    num_partition_key_cols + 1 + input_info.stream_key.len()
                }
            })
            .collect_vec();

        // Build pk_serde if intermediate_state_table is present
        let pk_serde = args.intermediate_state_table.as_ref().map(|_| {
            let pk_data_types: Vec<_> = input_info
                .stream_key
                .iter()
                .map(|&i| args.schema[i].data_type())
                .collect();
            let pk_order_types: Vec<_> = input_info
                .stream_key
                .iter()
                .map(|_| OrderType::ascending())
                .collect();
            OrderedRowSerde::new(pk_data_types, pk_order_types)
        });

        let exact_readiness = args.version >= PbEowcOverWindowNodeVersion::Issue17119;
        let triggers = args.watermark_trigger_table.map(|table| {
            assert!(
                exact_readiness && args.intermediate_state_table.is_some(),
                "watermark trigger table is planned with the exact readiness and the progress column"
            );
            WatermarkTriggers::new(table, args.schema[args.order_key_index].data_type())
        });

        Self {
            input: args.input,
            inner: ExecutorInner {
                actor_ctx: args.actor_ctx,
                schema: args.schema,
                calls: args.calls,
                input_stream_key: input_info.stream_key,
                partition_key_indices: args.partition_key_indices,
                order_key_index: args.order_key_index,
                state_table: args.state_table,
                evict_row_indices,
                watermark_sequence: args.watermark_epoch,
                intermediate_state_table: args.intermediate_state_table,
                pk_serde,
                exact_readiness,
                triggers,
                chunk_size: args.chunk_size,
            },
        }
    }

    /// Restore intermediate state snapshots and the output progress of the partition from its row
    /// in the intermediate state table.
    fn restore_intermediate_state(
        this: &ExecutorInner<S>,
        partition: &mut Partition,
        partition_key: &OwnedRow,
        row: Option<OwnedRow>,
    ) -> StreamExecutorResult<()> {
        if this.intermediate_state_table.is_none() {
            return Ok(());
        }
        let pk_serde = this
            .pk_serde
            .as_ref()
            .expect("pk_serde must be set when intermediate_state_table is present");

        for state in partition.states.iter_mut() {
            state.enable_persistence();
        }

        if let Some(row) = row {
            let num_partition_key_cols = this.partition_key_indices.len();
            let num_calls = this.calls.len();

            for call_index in 0..num_calls {
                let state_col = num_partition_key_cols + call_index;
                if state_col < row.len() {
                    if let Some(state_bytes) = row.datum_at(state_col) {
                        let snapshot = decode_snapshot(state_bytes.into_bytea(), pk_serde)?;
                        debug!(
                            "Restoring intermediate state for partition {:?}, call_index {}, has_last_key: {}",
                            partition_key,
                            call_index,
                            snapshot.last_output_key.is_some()
                        );
                        partition
                            .states
                            .get_mut(call_index)
                            .unwrap()
                            .restore(snapshot)?;
                    }
                } else {
                    return Err(anyhow::anyhow!(
                        "intermediate state row has fewer columns ({}) than expected ({}) \
                        at call_index {}, state may be corrupted",
                        row.len(),
                        num_partition_key_cols + num_calls,
                        call_index
                    )
                    .into());
                }
            }
            if this.triggers.is_some()
                && let Some(progress_bytes) = row.datum_at(num_partition_key_cols + num_calls)
            {
                partition.progress = decode_progress(progress_bytes.into_bytea(), pk_serde)?;
            }
            partition.intermediate_state_row = Some(row);
        }
        Ok(())
    }

    /// Persist intermediate state snapshots and the output progress to the state table, if changed.
    fn persist_partition(
        this: &mut ExecutorInner<S>,
        partition: &mut Partition,
        partition_key: &OwnedRow,
    ) {
        if !std::mem::take(&mut partition.dirty) {
            return;
        }
        let Some(intermediate_state_table) = &mut this.intermediate_state_table else {
            return;
        };
        let pk_serde = this
            .pk_serde
            .as_ref()
            .expect("pk_serde must be set when intermediate_state_table is present");

        let num_calls = partition.states.len();
        let num_partition_key_cols = partition_key.len();

        // Build the new row: partition_key columns + state_0..state_{n-1} [+ progress]
        let mut new_row_values = Vec::with_capacity(num_partition_key_cols + num_calls + 1);
        for datum in partition_key.iter() {
            new_row_values.push(datum.to_owned_datum());
        }

        // For each call, encode snapshot or preserve previous value
        for (call_index, state) in partition.states.iter().enumerate() {
            if let Some(snapshot) = state.snapshot() {
                let snapshot_bytes = encode_snapshot(&snapshot, pk_serde);
                new_row_values.push(Some(snapshot_bytes.into_boxed_slice().into()));
            } else if let Some(ref old_row) = partition.intermediate_state_row {
                let state_col = num_partition_key_cols + call_index;
                if state_col < old_row.len() {
                    new_row_values.push(old_row.datum_at(state_col).to_owned_datum());
                } else {
                    new_row_values.push(None);
                }
            } else {
                new_row_values.push(None);
            }
        }
        if this.triggers.is_some() {
            if partition.states.is_empty() {
                // No retained row to skip when reloading the partition.
                partition.progress.last_output_key = None;
            }
            new_row_values.push(
                encode_progress(&partition.progress, pk_serde)
                    .map(|bytes| bytes.into_boxed_slice().into()),
            );
        }
        let new_row = OwnedRow::new(new_row_values);
        let is_empty = new_row
            .iter()
            .skip(num_partition_key_cols)
            .all(|datum| datum.is_none());

        partition.intermediate_state_row = match partition.intermediate_state_row.take() {
            Some(old_row) if is_empty => {
                intermediate_state_table.delete(old_row);
                None
            }
            Some(old_row) => {
                intermediate_state_table.update(old_row, new_row.clone());
                Some(new_row)
            }
            None if is_empty => None,
            None => {
                intermediate_state_table.insert(new_row.clone());
                Some(new_row)
            }
        };
    }

    /// Load the partition from the state tables. The rows that were output are skipped by
    /// [`Self::skip_output_rows`] afterwards.
    async fn load_partition(
        this: &ExecutorInner<S>,
        partition_key: &OwnedRow,
    ) -> StreamExecutorResult<Partition> {
        let mut partition = Partition {
            states: WindowStates::new(this.calls.iter().map(create_window_state).try_collect()?),
            curr_row_buffer: Default::default(),
            intermediate_state_row: None,
            progress: Default::default(),
            dirty: false,
        };

        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
        let (intermediate_state_row, table_iter) = futures::try_join!(
            async {
                match &this.intermediate_state_table {
                    Some(table) => table.get_row(partition_key).await,
                    None => Ok(None),
                }
            },
            this.state_table
                .iter_with_prefix(partition_key, sub_range, PrefetchOptions::default()),
        )?;

        // If intermediate state table exists, restore intermediate state snapshots
        Self::restore_intermediate_state(
            this,
            &mut partition,
            partition_key,
            intermediate_state_row,
        )?;

        // Recover states from state table.
        #[for_await]
        for keyed_row in table_iter {
            let row = keyed_row?.into_owned_row();
            let order_key_enc = memcmp_encoding::encode_row(
                row::once(Some(
                    row.datum_at(this.order_key_index)
                        .expect("order key column must be non-NULL")
                        .into_scalar_impl(),
                )),
                &[OrderType::ascending()],
            )?;
            let pk = (&row).project(&this.input_stream_key).into_owned_row();
            let key = StateKey {
                order_key: order_key_enc,
                pk: pk.into(),
            };
            for (call, state) in this.calls.iter().zip_eq_fast(partition.states.iter_mut()) {
                state.append(
                    key.clone(),
                    (&row)
                        .project(call.args.val_indices())
                        .into_owned_row()
                        .as_inner()
                        .into(),
                );
            }
            partition.curr_row_buffer.push_back(row);
        }

        // Ensure states correctness.
        assert!(partition.states.are_aligned());

        Ok(partition)
    }

    /// Skip the rows of a loaded partition that were output. The rows not needed any more are
    /// evicted, as they were retained when output without the later rows.
    fn skip_output_rows(
        this: &mut ExecutorInner<S>,
        partition: &mut Partition,
        partition_key: &OwnedRow,
    ) -> StreamExecutorResult<()> {
        if !this.exact_readiness {
            // Ignore ready windows (all ready windows were outputted before).
            // Use just_slide which calls slide_no_output and respects recovery skip logic.
            while partition.states.are_ready() {
                partition.states.just_slide()?;
                partition.curr_row_buffer.pop_front();
            }
            return Ok(());
        }

        while let Some(key) = partition.states.curr_key() {
            let output = if this.triggers.is_some() {
                // Whether a window was closed by watermark can't be told from the rows.
                partition
                    .progress
                    .last_output_key
                    .as_ref()
                    .is_some_and(|last_output_key| key <= last_output_key)
            } else {
                // Rows are retained in order, so a window closed by the rows is still closed by the
                // retained rows.
                partition.states.readiness().is_ready(None)
            };
            if !output {
                break;
            }
            let evict_hint = partition.states.slide_no_output()?;
            partition.curr_row_buffer.pop_front();
            Self::evict(this, partition_key, evict_hint)?;
        }
        Ok(())
    }

    async fn ensure_partition_in_cache(
        this: &mut ExecutorInner<S>,
        cache: &mut PartitionCache,
        encoded_partition_key: &MemcmpEncoded,
        partition_key: &OwnedRow,
    ) -> StreamExecutorResult<()> {
        if cache.contains(encoded_partition_key) {
            return Ok(());
        }
        Self::ensure_partitions_in_cache(
            this,
            cache,
            &[(encoded_partition_key.clone(), partition_key)],
        )
        .await
    }

    /// Load the partitions not in the cache concurrently, and put them into the cache.
    async fn ensure_partitions_in_cache(
        this: &mut ExecutorInner<S>,
        cache: &mut PartitionCache,
        partitions: &[(MemcmpEncoded, &OwnedRow)],
    ) -> StreamExecutorResult<()> {
        let loaded: Vec<_> = {
            let this = &*this;
            stream::iter(
                partitions
                    .iter()
                    .filter(|(encoded_partition_key, _)| !cache.contains(encoded_partition_key))
                    .unique_by(|(encoded_partition_key, _)| encoded_partition_key.clone())
                    .map(|(encoded_partition_key, partition_key)| async move {
                        let partition = Self::load_partition(this, partition_key).await?;
                        Ok::<_, StreamExecutorError>((
                            encoded_partition_key,
                            partition_key,
                            partition,
                        ))
                    }),
            )
            .buffer_unordered(LOAD_CONCURRENCY)
            .try_collect()
            .await?
        };
        for (encoded_partition_key, partition_key, mut partition) in loaded {
            Self::skip_output_rows(this, &mut partition, partition_key)?;
            cache.put(encoded_partition_key.clone(), partition);
        }
        Ok(())
    }

    /// Rebuild a partition whose states are empty, as if it's reloaded. The window states may hold
    /// memory for the closed windows that their estimated sizes don't count.
    fn reset_partition(
        this: &ExecutorInner<S>,
        partition: &mut Partition,
    ) -> StreamExecutorResult<()> {
        let mut states =
            WindowStates::new(this.calls.iter().map(create_window_state).try_collect()?);
        for (state, old_state) in states.iter_mut().zip_eq_fast(partition.states.iter()) {
            if this.intermediate_state_table.is_some() {
                state.enable_persistence();
            }
            if let Some(snapshot) = old_state.snapshot() {
                state.restore(snapshot)?;
            }
        }
        partition.states = states;
        partition.curr_row_buffer = Default::default();
        Ok(())
    }

    /// Delete the evicted rows from the state table.
    fn evict(
        this: &mut ExecutorInner<S>,
        partition_key: &OwnedRow,
        evict_hint: StateEvictHint,
    ) -> StreamExecutorResult<()> {
        let StateEvictHint::CanEvict(keys_to_evict) = evict_hint else {
            return Ok(());
        };
        for key in keys_to_evict {
            let order_key = memcmp_encoding::decode_row(
                &key.order_key,
                &[this.schema[this.order_key_index].data_type()],
                &[OrderType::ascending()],
            )?;
            // FIXME(rc): quite hacky here, we may need `state_table.delete_by_pk`
            let state_row = partition_key
                .chain(&order_key)
                .chain(key.pk.as_inner())
                .chain(row::once(Datum::None))
                .project(&this.evict_row_indices);
            // NOTE: We don't know the value of the row here, so the table must allow
            // inconsistent ops.
            this.state_table.delete(state_row);
        }
        Ok(())
    }

    /// Output the rows of the partition whose windows are closed, under the current watermark of the
    /// order key if windows may be closed by watermark. Returns the readiness of the next row.
    fn output_ready_rows(
        this: &mut ExecutorInner<S>,
        partition: &mut Partition,
        partition_key: &OwnedRow,
        chunk_builder: &mut StreamChunkBuilder,
        chunks: &mut Vec<StreamChunk>,
    ) -> StreamExecutorResult<Readiness> {
        loop {
            let readiness = if this.exact_readiness {
                partition.states.readiness()
            } else if partition.states.are_ready() {
                Readiness::Ready
            } else {
                Readiness::WaitForInput
            };
            let watermark = this
                .triggers
                .as_ref()
                .and_then(|triggers| triggers.watermark());
            if !readiness.is_ready(watermark) {
                return Ok(readiness);
            }
            if this.triggers.is_some() {
                partition.progress.last_output_key = partition.states.curr_key().cloned();
            }
            partition.dirty = true;

            // Get all outputs.
            let (ret_values, evict_hint) = partition.states.slide()?;
            let curr_row = partition
                .curr_row_buffer
                .pop_front()
                .expect("ready window must have corresponding current row");
            if let Some(chunk) =
                chunk_builder.append_row(Op::Insert, (&curr_row).chain(OwnedRow::new(ret_values)))
            {
                chunks.push(chunk);
            }

            Self::evict(this, partition_key, evict_hint)?;
        }
    }

    /// Register a trigger for the partition if the window of its next row to output, whose
    /// `readiness` is given, may be closed by watermark, unless one is registered.
    fn register_trigger(
        this: &mut ExecutorInner<S>,
        partition: &mut Partition,
        partition_key: &OwnedRow,
        readiness: Readiness,
    ) {
        let Some(triggers) = &mut this.triggers else {
            return;
        };
        if let Some(trigger) = &partition.progress.trigger {
            // The triggers satisfied by the watermark have all fired, unless the trigger table lost
            // this one.
            if !triggers
                .watermark()
                .is_some_and(|watermark| trigger.is_satisfied_by(watermark))
            {
                return;
            }
            consistency_panic!(
                ?partition_key,
                ?trigger,
                "watermark trigger of the partition is lost"
            );
            partition.progress.trigger = None;
            partition.dirty = true;
        }
        let Readiness::OnWatermark(bound) = readiness else {
            return;
        };
        // Bounds are not less than the order key of the last row either, so they're never below
        // the persisted watermark of the trigger table, under which rows are hidden from reads.
        assert!(
            !triggers
                .watermark()
                .is_some_and(|watermark| bound.is_satisfied_by(watermark)),
            "rows closed by the current watermark must have been output"
        );
        triggers.register(TriggerEntry {
            bound: bound.clone(),
            partition_key: partition_key.clone(),
        });
        partition.progress.trigger = Some(bound);
        partition.dirty = true;
    }

    async fn apply_chunk(
        this: &mut ExecutorInner<S>,
        vars: &mut ExecutionVars<S>,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<Vec<StreamChunk>> {
        let mut chunk_builder = StreamChunkBuilder::new(this.chunk_size, this.schema.data_types());
        let mut chunks = vec![];
        // Track partitions to persist, so we persist only once per partition at the end.
        let mut dirty_partitions: HashMap<MemcmpEncoded, OwnedRow> = HashMap::new();

        // We assume that the input is sorted by order key.
        for record in chunk.records() {
            let input_row = must_match!(record, Record::Insert { new_row } => new_row);

            let partition_key = input_row
                .project(&this.partition_key_indices)
                .into_owned_row();
            let encoded_partition_key = this.encode_partition_key(&partition_key)?;

            // Get the partition.
            Self::ensure_partition_in_cache(
                this,
                &mut vars.partitions,
                &encoded_partition_key,
                &partition_key,
            )
            .await?;
            let partition: &mut Partition =
                &mut vars.partitions.get_mut(&encoded_partition_key).unwrap();

            // Materialize input to state table.
            this.state_table.insert(input_row);

            // Feed the row to all window states.
            let order_key_enc = memcmp_encoding::encode_row(
                row::once(Some(
                    input_row
                        .datum_at(this.order_key_index)
                        .expect("order key column must be non-NULL")
                        .into_scalar_impl(),
                )),
                &[OrderType::ascending()],
            )?;
            let pk = input_row.project(&this.input_stream_key).into_owned_row();
            let key = StateKey {
                order_key: order_key_enc,
                pk: pk.into(),
            };
            for (call, state) in this.calls.iter().zip_eq_fast(partition.states.iter_mut()) {
                state.append(
                    key.clone(),
                    input_row
                        .project(call.args.val_indices())
                        .into_owned_row()
                        .as_inner()
                        .into(),
                );
            }
            partition
                .curr_row_buffer
                .push_back(input_row.into_owned_row());

            let readiness = Self::output_ready_rows(
                this,
                partition,
                &partition_key,
                &mut chunk_builder,
                &mut chunks,
            )?;
            Self::register_trigger(this, partition, &partition_key, readiness);

            if this.intermediate_state_table.is_some() && partition.dirty {
                dirty_partitions
                    .entry(encoded_partition_key)
                    .or_insert(partition_key);
            }
        }

        for (encoded_partition_key, partition_key) in &dirty_partitions {
            let partition = &mut *vars.partitions.get_mut(encoded_partition_key).unwrap();
            Self::persist_partition(this, partition, partition_key);
        }

        chunks.extend(chunk_builder.take());
        Ok(chunks)
    }

    /// Output the rows whose windows are closed by the new watermark of the order key.
    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn apply_watermark<'a>(
        this: &'a mut ExecutorInner<S>,
        vars: &'a mut ExecutionVars<S>,
        watermark: ScalarImpl,
    ) {
        let Some(triggers) = &mut this.triggers else {
            return Ok(());
        };
        if !triggers.advance_watermark(watermark)? {
            return Ok(());
        }

        let mut chunk_builder = StreamChunkBuilder::new(this.chunk_size, this.schema.data_types());
        // Handle the fired triggers a chunk at a time, after which the partitions can be evicted and
        // the tables flushed, so the memory doesn't grow with the number of partitions closed.
        loop {
            let entries = (this.triggers.as_mut().unwrap())
                .pop_due(this.chunk_size)
                .await?;
            if entries.is_empty() {
                break;
            }
            let partitions: Vec<_> = entries
                .iter()
                .map(|entry| {
                    Ok::<_, StreamExecutorError>((
                        this.encode_partition_key(&entry.partition_key)?,
                        &entry.partition_key,
                    ))
                })
                .try_collect()?;
            Self::ensure_partitions_in_cache(this, &mut vars.partitions, &partitions).await?;

            for (entry, (encoded_partition_key, _)) in entries.iter().zip_eq_fast(&partitions) {
                let mut chunks = vec![];
                {
                    let partition = &mut *vars
                        .partitions
                        .get_mut(encoded_partition_key)
                        .expect("partitions of the chunk must be in the cache");
                    Self::fire_trigger(this, partition, entry, &mut chunk_builder, &mut chunks)?;
                }
                for chunk in chunks {
                    yield chunk;
                }
            }
            Self::try_flush(this).await?;
            vars.partitions.evict();
        }

        if let Some(chunk) = chunk_builder.take() {
            yield chunk;
        }
        (this.triggers.as_mut().unwrap()).clean();
    }

    /// Handle the partition whose trigger `entry` is satisfied by the watermark: output the rows
    /// whose windows are closed, and register a trigger again if needed.
    fn fire_trigger(
        this: &mut ExecutorInner<S>,
        partition: &mut Partition,
        entry: &TriggerEntry,
        chunk_builder: &mut StreamChunkBuilder,
        chunks: &mut Vec<StreamChunk>,
    ) -> StreamExecutorResult<()> {
        let trigger = partition.progress.trigger.take();
        if trigger.as_ref() != Some(&entry.bound) {
            consistency_panic!(
                partition_key = ?entry.partition_key,
                bound = ?entry.bound,
                progress_trigger = ?trigger,
                "watermark trigger table diverges from the partition progress"
            );
            // A later trigger is still in the table, which fires then.
            if let Some(trigger) = trigger
                && trigger > entry.bound
            {
                partition.progress.trigger = Some(trigger);
            }
        }
        partition.dirty = true;

        let readiness =
            Self::output_ready_rows(this, partition, &entry.partition_key, chunk_builder, chunks)?;
        Self::register_trigger(this, partition, &entry.partition_key, readiness);
        Self::persist_partition(this, partition, &entry.partition_key);
        if partition.states.is_empty() {
            Self::reset_partition(this, partition)?;
        }
        Ok(())
    }

    async fn try_flush(this: &mut ExecutorInner<S>) -> StreamExecutorResult<()> {
        this.state_table.try_flush().await?;
        if let Some(intermediate_state_table) = &mut this.intermediate_state_table {
            intermediate_state_table.try_flush().await?;
        }
        if let Some(triggers) = &mut this.triggers {
            triggers.try_flush().await?;
        }
        Ok(())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn executor_inner(self) {
        let EowcOverWindowExecutor {
            input,
            inner: mut this,
        } = self;

        let metrics_info = MetricsInfo::new(
            this.actor_ctx.streaming_metrics.clone(),
            this.state_table.table_id(),
            this.actor_ctx.id,
            "EowcOverWindow",
        );

        let mut vars = ExecutionVars {
            partitions: ManagedLruCache::unbounded(this.watermark_sequence.clone(), metrics_info),
            _phantom: PhantomData::<S>,
        };

        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        this.state_table.init_epoch(first_epoch).await?;
        if let Some(intermediate_state_table) = &mut this.intermediate_state_table {
            intermediate_state_table.init_epoch(first_epoch).await?;
        }
        if let Some(triggers) = &mut this.triggers {
            triggers.init_epoch(first_epoch).await?;
        }

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                // Watermarks are not forwarded, see `StreamEowcOverWindow`.
                Message::Watermark(watermark) => {
                    if watermark.col_idx == this.order_key_index {
                        #[for_await]
                        for chunk in Self::apply_watermark(&mut this, &mut vars, watermark.val) {
                            yield Message::Chunk(chunk?);
                        }
                    }
                }
                Message::Chunk(chunk) => {
                    for chunk in Self::apply_chunk(&mut this, &mut vars, chunk).await? {
                        yield Message::Chunk(chunk);
                    }
                    Self::try_flush(&mut this).await?;
                }
                Message::Barrier(barrier) => {
                    let post_commit = this.state_table.commit(barrier.epoch).await?;
                    let intermediate_post_commit = if let Some(intermediate_state_table) =
                        &mut this.intermediate_state_table
                    {
                        Some(intermediate_state_table.commit(barrier.epoch).await?)
                    } else {
                        None
                    };
                    let triggers_post_commit = if let Some(triggers) = &mut this.triggers {
                        Some(triggers.commit(barrier.epoch).await?)
                    } else {
                        None
                    };

                    vars.partitions.evict();

                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(this.actor_ctx.id);
                    yield Message::Barrier(barrier);

                    let mut cache_may_stale = false;
                    if let Some((_, stale)) = post_commit
                        .post_yield_barrier(update_vnode_bitmap.clone())
                        .await?
                    {
                        cache_may_stale = cache_may_stale || stale;
                    }
                    if let Some(intermediate_post_commit) = intermediate_post_commit
                        && let Some((_, stale)) = intermediate_post_commit
                            .post_yield_barrier(update_vnode_bitmap.clone())
                            .await?
                    {
                        cache_may_stale = cache_may_stale || stale;
                    }
                    if let Some(triggers_post_commit) = triggers_post_commit {
                        triggers_post_commit
                            .post_yield_barrier(update_vnode_bitmap)
                            .await?;
                    }
                    if cache_may_stale {
                        vars.partitions.clear();
                    }
                }
            }
        }
    }
}
