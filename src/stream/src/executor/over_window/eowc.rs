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

use std::marker::PhantomData;
use std::ops::Bound;

use itertools::Itertools;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{ArrayRef, Op};
use risingwave_common::row::RowExt;
use risingwave_common::types::{ToDatumRef, ToOwnedDatum};
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_common::util::memcmp_encoding::{self, MemcmpEncoded};
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::{must_match, row};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_common_estimate_size::collections::EstimatedVecDeque;
use risingwave_expr::window_function::{
    StateEvictHint, StateKey, WindowFuncCall, WindowStateSnapshot, WindowStates,
    create_window_state,
};
use risingwave_storage::store::PrefetchOptions;
use tracing::debug;

use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::executor::prelude::*;

struct Partition {
    states: WindowStates,
    curr_row_buffer: EstimatedVecDeque<OwnedRow>,
    /// Cached rank state row for this partition, used for upsert operations.
    /// `None` means no prior row exists in the rank state table for this partition.
    /// Schema: partition key columns + state_0..state_{n-1} (one per window function call).
    rank_state_row: Option<OwnedRow>,
}

impl EstimateSize for Partition {
    fn estimated_heap_size(&self) -> usize {
        let mut total_size = self.curr_row_buffer.estimated_heap_size();
        for state in self.states.iter() {
            total_size += state.estimated_heap_size();
        }
        if let Some(row) = &self.rank_state_row {
            total_size += row.estimated_heap_size();
        }
        total_size
    }
}

type PartitionCache = ManagedLruCache<MemcmpEncoded, Partition>; // TODO(rc): use `K: HashKey` as key like in hash agg?

/// Snapshot serialization version.
const SNAPSHOT_VERSION: u8 = 1;

/// Encode a `WindowStateSnapshot` to bytes for persistence.
/// Format: version (u8) + has_last_key (u8) + [order_key_len (u32) + order_key + pk_len (u32) + pk] + payload_len (u32) + payload
fn encode_snapshot(snapshot: &WindowStateSnapshot, pk_ser: &OrderedRowSerde) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(SNAPSHOT_VERSION);

    if let Some(key) = &snapshot.last_output_key {
        bytes.push(1u8);
        // Encode order_key
        let order_key_bytes = &key.order_key;
        bytes.extend_from_slice(&(order_key_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(order_key_bytes);
        // Encode pk using the serde
        let mut pk_bytes = Vec::new();
        pk_ser.serialize(key.pk.as_inner(), &mut pk_bytes);
        bytes.extend_from_slice(&(pk_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&pk_bytes);
    } else {
        bytes.push(0u8);
    }

    // Encode payload
    bytes.extend_from_slice(&(snapshot.payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&snapshot.payload);
    bytes
}

/// Decode a `WindowStateSnapshot` from bytes during recovery.
fn decode_snapshot(
    bytes: &[u8],
    pk_deser: &OrderedRowSerde,
) -> StreamExecutorResult<WindowStateSnapshot> {
    if bytes.is_empty() {
        return Err(StreamExecutorError::from(anyhow::anyhow!(
            "invalid snapshot: empty bytes"
        )));
    }

    let mut offset = 0;
    let version = bytes[offset];
    offset += 1;

    if version != SNAPSHOT_VERSION {
        return Err(StreamExecutorError::from(anyhow::anyhow!(
            "unsupported snapshot version: {}",
            version
        )));
    }

    if bytes.len() < offset + 1 {
        return Err(StreamExecutorError::from(anyhow::anyhow!(
            "invalid snapshot: missing has_last_key"
        )));
    }
    let has_last_key = bytes[offset];
    offset += 1;

    let last_output_key = if has_last_key == 1 {
        // Decode order_key
        if bytes.len() < offset + 4 {
            return Err(StreamExecutorError::from(anyhow::anyhow!(
                "invalid snapshot: missing order_key_len"
            )));
        }
        let order_key_len =
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if bytes.len() < offset + order_key_len {
            return Err(StreamExecutorError::from(anyhow::anyhow!(
                "invalid snapshot: missing order_key bytes"
            )));
        }
        let order_key: MemcmpEncoded = bytes[offset..offset + order_key_len].to_vec().into();
        offset += order_key_len;

        // Decode pk
        if bytes.len() < offset + 4 {
            return Err(StreamExecutorError::from(anyhow::anyhow!(
                "invalid snapshot: missing pk_len"
            )));
        }
        let pk_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if bytes.len() < offset + pk_len {
            return Err(StreamExecutorError::from(anyhow::anyhow!(
                "invalid snapshot: missing pk bytes"
            )));
        }
        let pk_bytes = &bytes[offset..offset + pk_len];
        offset += pk_len;
        let pk = pk_deser.deserialize(pk_bytes).map_err(|e| {
            StreamExecutorError::from(anyhow::anyhow!("failed to deserialize pk: {}", e))
        })?;

        Some(StateKey {
            order_key,
            pk: pk.into(),
        })
    } else {
        None
    };

    // Decode payload
    if bytes.len() < offset + 4 {
        return Err(StreamExecutorError::from(anyhow::anyhow!(
            "invalid snapshot: missing payload_len"
        )));
    }
    let payload_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    if bytes.len() < offset + payload_len {
        return Err(StreamExecutorError::from(anyhow::anyhow!(
            "invalid snapshot: missing payload bytes"
        )));
    }
    let payload = bytes[offset..offset + payload_len].to_vec();

    Ok(WindowStateSnapshot {
        last_output_key,
        payload,
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
/// - Recover: iterate through state table, push rows to `WindowState`, ignore ready windows.
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
    state_table_schema_len: usize,
    watermark_sequence: AtomicU64Ref,
    /// Optional state table for persisting rank function snapshots.
    /// Only present when numbering functions (row_number/rank/dense_rank) are used.
    /// Schema: partition_key columns + state_0..state_{n-1} (one Bytea column per window function call).
    /// PK = partition key columns only.
    rank_state_table: Option<StateTable<S>>,
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
    /// Optional state table for persisting rank function snapshots.
    /// Schema: partition_key columns + state_0..state_{n-1} (one Bytea column per window function call).
    /// PK = partition key columns only.
    pub rank_state_table: Option<StateTable<S>>,
}

impl<S: StateStore> EowcOverWindowExecutor<S> {
    pub fn new(args: EowcOverWindowExecutorArgs<S>) -> Self {
        let input_info = args.input.info().clone();

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
                state_table_schema_len: input_info.schema.len(),
                watermark_sequence: args.watermark_epoch,
                rank_state_table: args.rank_state_table,
            },
        }
    }

    async fn ensure_key_in_cache(
        this: &ExecutorInner<S>,
        cache: &mut PartitionCache,
        partition_key: impl Row,
        encoded_partition_key: &MemcmpEncoded,
    ) -> StreamExecutorResult<()> {
        if cache.contains(encoded_partition_key) {
            return Ok(());
        }

        let num_calls = this.calls.len();
        let mut partition = Partition {
            states: WindowStates::new(this.calls.iter().map(create_window_state).try_collect()?),
            curr_row_buffer: Default::default(),
            rank_state_row: None,
        };

        // If rank state table exists, enable persistence on all states and restore snapshots
        if let Some(rank_state_table) = &this.rank_state_table {
            for state in partition.states.iter_mut() {
                state.enable_persistence();
            }

            // Build pk serde for deserializing StateKey pk
            let pk_data_types: Vec<_> = this
                .input_stream_key
                .iter()
                .map(|&i| this.schema[i].data_type())
                .collect();
            let pk_order_types: Vec<_> = this
                .input_stream_key
                .iter()
                .map(|_| OrderType::ascending())
                .collect();
            let pk_deser = OrderedRowSerde::new(pk_data_types, pk_order_types);

            // Load rank state row for this partition (single row per partition).
            // New schema: partition_key columns + state_0..state_{n-1} (one per window function call).
            // PK = partition key columns only.
            let partition_key_owned = partition_key.to_owned_row();
            if let Some(row) = rank_state_table.get_row(&partition_key_owned).await? {
                let num_partition_key_cols = this.partition_key_indices.len();

                // Each state column is at index: num_partition_key_cols + call_index
                for call_index in 0..num_calls {
                    let state_col = num_partition_key_cols + call_index;
                    if state_col < row.len() {
                        if let Some(state_bytes) = row.datum_at(state_col) {
                            let snapshot = decode_snapshot(state_bytes.into_bytea(), &pk_deser)?;
                            debug!(
                                "Restoring rank state for partition {:?}, call_index {}, has_last_key: {}",
                                encoded_partition_key,
                                call_index,
                                snapshot.last_output_key.is_some()
                            );
                            partition
                                .states
                                .iter_mut()
                                .nth(call_index)
                                .unwrap()
                                .restore(snapshot)?;
                        }
                    } else {
                        tracing::warn!(
                            "Rank state row has fewer columns ({}) than expected ({}), skipping call_index {}",
                            row.len(),
                            num_partition_key_cols + num_calls,
                            call_index
                        );
                    }
                }
                // Cache the row for future upserts
                partition.rank_state_row = Some(row);
            }
        }

        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
        // Recover states from state table.
        let table_iter = this
            .state_table
            .iter_with_prefix(&partition_key, sub_range, PrefetchOptions::default())
            .await?;

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

        // Ignore ready windows (all ready windows were outputted before).
        // Use just_slide which calls slide_no_output and respects recovery skip logic.
        while partition.states.are_ready() {
            partition.states.just_slide()?;
            partition.curr_row_buffer.pop_front();
        }

        cache.put(encoded_partition_key.clone(), partition);
        Ok(())
    }

    async fn apply_chunk(
        this: &mut ExecutorInner<S>,
        vars: &mut ExecutionVars<S>,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        let mut builders = this.schema.create_array_builders(chunk.capacity()); // just an estimate

        // We assume that the input is sorted by order key.
        for record in chunk.records() {
            let input_row = must_match!(record, Record::Insert { new_row } => new_row);

            let partition_key = input_row
                .project(&this.partition_key_indices)
                .into_owned_row();
            let encoded_partition_key = memcmp_encoding::encode_row(
                &partition_key,
                &vec![OrderType::ascending(); this.partition_key_indices.len()],
            )?;

            // Get the partition.
            Self::ensure_key_in_cache(
                this,
                &mut vars.partitions,
                &partition_key,
                &encoded_partition_key,
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

            while partition.states.are_ready() {
                // The partition is ready to output, so we can produce a row.

                // Get all outputs.
                let (ret_values, evict_hint) = partition.states.slide()?;
                let curr_row = partition
                    .curr_row_buffer
                    .pop_front()
                    .expect("ready window must have corresponding current row");

                // Append to output builders.
                for (builder, datum) in builders.iter_mut().zip_eq_debug(
                    curr_row
                        .iter()
                        .chain(ret_values.iter().map(|v| v.to_datum_ref())),
                ) {
                    builder.append(datum);
                }

                // Evict unneeded rows from state table.
                if let StateEvictHint::CanEvict(keys_to_evict) = evict_hint {
                    for key in keys_to_evict {
                        let order_key = memcmp_encoding::decode_row(
                            &key.order_key,
                            &[this.schema[this.order_key_index].data_type()],
                            &[OrderType::ascending()],
                        )?;
                        let state_row_pk = (&partition_key).chain(order_key).chain(key.pk);
                        let state_row = {
                            // FIXME(rc): quite hacky here, we may need `state_table.delete_by_pk`
                            let mut state_row = vec![None; this.state_table_schema_len];
                            for (i_in_pk, &i) in this.state_table.pk_indices().iter().enumerate() {
                                state_row[i] = state_row_pk.datum_at(i_in_pk).to_owned_datum();
                            }
                            OwnedRow::new(state_row)
                        };
                        // NOTE: We don't know the value of the row here, so the table must allow
                        // inconsistent ops.
                        this.state_table.delete(state_row);
                    }
                }

                // Persist rank state snapshots if rank_state_table exists
                // New schema: partition_key columns + state_0..state_{n-1} (one per window function call).
                // Single row per partition.
                if let Some(rank_state_table) = &mut this.rank_state_table {
                    // Build pk serde for serializing StateKey pk
                    let pk_data_types: Vec<_> = this
                        .input_stream_key
                        .iter()
                        .map(|&i| this.schema[i].data_type())
                        .collect();
                    let pk_order_types: Vec<_> = this
                        .input_stream_key
                        .iter()
                        .map(|_| OrderType::ascending())
                        .collect();
                    let pk_ser = OrderedRowSerde::new(pk_data_types, pk_order_types);

                    let num_calls = partition.states.len();
                    let num_partition_key_cols = partition_key.len();

                    // Build the new row: partition_key columns + state_0..state_{n-1}
                    let mut new_row_values = Vec::with_capacity(num_partition_key_cols + num_calls);
                    for datum in partition_key.iter() {
                        new_row_values.push(datum.to_owned_datum());
                    }

                    // For each call, encode snapshot or preserve previous value
                    for (call_index, state) in partition.states.iter().enumerate() {
                        if let Some(snapshot) = state.snapshot() {
                            let snapshot_bytes = encode_snapshot(&snapshot, &pk_ser);
                            new_row_values.push(Some(snapshot_bytes.into_boxed_slice().into()));
                        } else if let Some(ref old_row) = partition.rank_state_row {
                            // Preserve previous value if no new snapshot
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
                    let new_row = OwnedRow::new(new_row_values);

                    // Upsert: update if old row exists, otherwise insert
                    if let Some(old_row) = partition.rank_state_row.take() {
                        rank_state_table.update(old_row, new_row.clone());
                    } else {
                        rank_state_table.insert(new_row.clone());
                    }
                    partition.rank_state_row = Some(new_row);
                }
            }
        }

        let columns: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish().into()).collect();
        let chunk_size = columns[0].len();
        Ok(if chunk_size > 0 {
            Some(StreamChunk::new(vec![Op::Insert; chunk_size], columns))
        } else {
            None
        })
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
        if let Some(rank_state_table) = &mut this.rank_state_table {
            rank_state_table.init_epoch(first_epoch).await?;
        }

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Watermark(_) => {
                    continue;
                }
                Message::Chunk(chunk) => {
                    let output_chunk = Self::apply_chunk(&mut this, &mut vars, chunk).await?;
                    if let Some(chunk) = output_chunk {
                        yield Message::Chunk(chunk);
                    }
                    this.state_table.try_flush().await?;
                    if let Some(rank_state_table) = &mut this.rank_state_table {
                        rank_state_table.try_flush().await?;
                    }
                }
                Message::Barrier(barrier) => {
                    let post_commit = this.state_table.commit(barrier.epoch).await?;
                    let rank_post_commit =
                        if let Some(rank_state_table) = &mut this.rank_state_table {
                            Some(rank_state_table.commit(barrier.epoch).await?)
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
                    if let Some(rank_post_commit) = rank_post_commit {
                        if let Some((_, stale)) = rank_post_commit
                            .post_yield_barrier(update_vnode_bitmap)
                            .await?
                        {
                            cache_may_stale = cache_may_stale || stale;
                        }
                    }
                    if cache_may_stale {
                        vars.partitions.clear();
                    }
                }
            }
        }
    }
}
