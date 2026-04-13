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
use itertools::Itertools;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{ArrayBuilderImpl, ArrayRef, Op};
use risingwave_common::hash::VnodeBitmapExt;
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
use risingwave_pb::window_function::{
    StateKey as PbStateKey, WindowStateSnapshot as PbWindowStateSnapshot,
};
use risingwave_storage::store::PrefetchOptions;
use tracing::debug;

use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::executor::prelude::*;

struct Partition {
    states: WindowStates,
    curr_row_buffer: EstimatedVecDeque<OwnedRow>,
    /// Cached intermediate state row for this partition, used for upsert operations.
    /// `None` means no prior row exists in the intermediate state table for this partition.
    intermediate_state_row: Option<OwnedRow>,
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
        total_size
    }
}

type PartitionCache = ManagedLruCache<MemcmpEncoded, Partition>; // TODO(rc): use `K: HashKey` as key like in hash agg?

/// Tracks partitions with unclosed session windows in a state table.
///
/// Table schema: `(minimal_next_start <order_key_type>, partition_key_cols...)`.
/// PK: `(minimal_next_start ASC, partition_key_cols ASC)` — mns first for efficient range scan
/// on watermark to find partitions ready to close.
struct OpenSessionsTracker<S: StateStore> {
    table: StateTable<S>,
    num_partition_key_cols: usize,
    order_key_data_type: DataType,
}

impl<S: StateStore> OpenSessionsTracker<S> {
    fn new(
        table: StateTable<S>,
        num_partition_key_cols: usize,
        order_key_data_type: DataType,
    ) -> Self {
        Self {
            table,
            num_partition_key_cols,
            order_key_data_type,
        }
    }

    /// Get the current `minimal_next_start` from a partition's window states.
    fn get_mns(partition: &Partition) -> Option<MemcmpEncoded> {
        if !partition.curr_row_buffer.is_empty() {
            partition.states.minimal_next_start()
        } else {
            None
        }
    }

    /// Update the table for a partition based on old vs new mns values.
    fn update(
        &mut self,
        partition_key: &OwnedRow,
        old_mns: &Option<MemcmpEncoded>,
        new_mns: &Option<MemcmpEncoded>,
    ) {
        match (old_mns, new_mns) {
            (None, Some(new)) => {
                self.table.insert(self.build_row(partition_key, new));
            }
            (Some(old), Some(new)) if old != new => {
                self.table.delete(self.build_row(partition_key, old));
                self.table.insert(self.build_row(partition_key, new));
            }
            (Some(old), None) => {
                self.table.delete(self.build_row(partition_key, old));
            }
            _ => {}
        }
    }

    /// Scan for partition entries with `minimal_next_start <= watermark`.
    async fn scan_partitions_to_close(
        &self,
        watermark_encoded: &MemcmpEncoded,
    ) -> StreamExecutorResult<Vec<(MemcmpEncoded, OwnedRow, MemcmpEncoded)>> {
        let watermark_datum = memcmp_encoding::decode_value(
            &self.order_key_data_type,
            &watermark_encoded[..],
            OrderType::ascending(),
        )?;
        let watermark_mns_row = OwnedRow::new(vec![watermark_datum]);
        let pk_range: (Bound<OwnedRow>, Bound<OwnedRow>) =
            (Bound::Unbounded, Bound::Included(watermark_mns_row));

        let mut entries = Vec::new();
        let pk_order_types = vec![OrderType::ascending(); self.num_partition_key_cols];
        for vnode in self.table.vnodes().iter_vnodes() {
            let iter = self
                .table
                .iter_with_vnode(vnode, &pk_range, PrefetchOptions::default())
                .await?;
            #[for_await]
            for row in iter {
                let row = row?.into_owned_row();
                let (partition_key, mns) = self.parse_row(&row);
                let encoded_partition_key =
                    memcmp_encoding::encode_row(&partition_key, &pk_order_types)?;
                entries.push((encoded_partition_key, partition_key, mns));
            }
        }
        Ok(entries)
    }

    /// Delete a closed session entry.
    fn delete(&mut self, partition_key: &OwnedRow, mns: &MemcmpEncoded) {
        self.table.delete(self.build_row(partition_key, mns));
    }

    fn build_row(&self, partition_key: &OwnedRow, mns: &MemcmpEncoded) -> OwnedRow {
        let mns_datum = memcmp_encoding::decode_value(
            &self.order_key_data_type,
            &mns[..],
            OrderType::ascending(),
        )
        .expect("failed to decode mns");
        let mut values = Vec::with_capacity(1 + partition_key.len());
        values.push(mns_datum);
        for datum in partition_key.iter() {
            values.push(datum.to_owned_datum());
        }
        OwnedRow::new(values)
    }

    fn parse_row(&self, row: &OwnedRow) -> (OwnedRow, MemcmpEncoded) {
        let mns_datum = row.datum_at(0);
        let mns = memcmp_encoding::encode_value(mns_datum, OrderType::ascending())
            .expect("failed to encode mns");
        let pk_datums: Vec<_> = (1..=self.num_partition_key_cols)
            .map(|i| row.datum_at(i).to_owned_datum())
            .collect();
        (OwnedRow::new(pk_datums), mns)
    }
}

/// Encode a [`WindowStateSnapshot`] to bytes for persistence using protobuf.
fn encode_snapshot(snapshot: &WindowStateSnapshot, pk_ser: &OrderedRowSerde) -> Vec<u8> {
    use prost::Message;
    let pb = PbWindowStateSnapshot {
        last_output_key: snapshot.last_output_key.as_ref().map(|key| PbStateKey {
            order_key: key.order_key.to_vec(),
            pk: key.pk.as_inner().memcmp_serialize(pk_ser),
        }),
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
        .map(|key| {
            let pk = pk_deser
                .deserialize(&key.pk)
                .context("failed to deserialize pk")?;
            Ok::<_, anyhow::Error>(StateKey {
                order_key: key.order_key.into(),
                pk: pk.into(),
            })
        })
        .transpose()?;
    let function_state = pb
        .function_state
        .context("snapshot missing function_state")?;
    Ok(WindowStateSnapshot {
        last_output_key,
        function_state,
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
    /// Optional state table for persisting window function intermediate states.
    /// See `StreamEowcOverWindow::infer_intermediate_state_table` for schema definition.
    intermediate_state_table: Option<StateTable<S>>,
    /// Serde for input stream key (pk), used for encoding/decoding `StateKey` in snapshots.
    /// Only initialized when `intermediate_state_table` is present.
    pk_serde: Option<OrderedRowSerde>,
    /// Tracks partitions with unclosed session windows via a state table.
    /// Present only when any window function uses a session frame.
    open_sessions_tracker: Option<OpenSessionsTracker<S>>,
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
    /// Optional table tracking partition keys with unclosed session windows.
    pub open_sessions_table: Option<StateTable<S>>,
}

impl<S: StateStore> EowcOverWindowExecutor<S> {
    pub fn new(args: EowcOverWindowExecutorArgs<S>) -> Self {
        let input_info = args.input.info().clone();

        let order_key_data_type = args.schema[args.order_key_index].data_type();
        let open_sessions_tracker = args.open_sessions_table.map(|table| {
            OpenSessionsTracker::new(table, args.partition_key_indices.len(), order_key_data_type)
        });

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
                intermediate_state_table: args.intermediate_state_table,
                pk_serde,
                open_sessions_tracker,
            },
        }
    }

    /// Load intermediate state snapshots from the state table and restore into partition states.
    async fn load_intermediate_state(
        this: &ExecutorInner<S>,
        partition: &mut Partition,
        partition_key: impl Row,
        encoded_partition_key: &MemcmpEncoded,
    ) -> StreamExecutorResult<()> {
        let Some(intermediate_state_table) = &this.intermediate_state_table else {
            return Ok(());
        };
        let pk_serde = this
            .pk_serde
            .as_ref()
            .expect("pk_serde must be set when intermediate_state_table is present");

        for state in partition.states.iter_mut() {
            state.enable_persistence();
        }

        let partition_key_owned = partition_key.to_owned_row();
        if let Some(row) = intermediate_state_table
            .get_row(&partition_key_owned)
            .await?
        {
            let num_partition_key_cols = this.partition_key_indices.len();
            let num_calls = this.calls.len();

            for call_index in 0..num_calls {
                let state_col = num_partition_key_cols + call_index;
                if state_col < row.len() {
                    if let Some(state_bytes) = row.datum_at(state_col) {
                        let snapshot = decode_snapshot(state_bytes.into_bytea(), pk_serde)?;
                        debug!(
                            "Restoring intermediate state for partition {:?}, call_index {}, has_last_key: {}",
                            encoded_partition_key,
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
            partition.intermediate_state_row = Some(row);
        }
        Ok(())
    }

    /// Persist intermediate state snapshots to the state table.
    fn persist_intermediate_state(
        this: &mut ExecutorInner<S>,
        partition: &mut Partition,
        partition_key: impl Row,
    ) {
        let Some(intermediate_state_table) = &mut this.intermediate_state_table else {
            return;
        };
        let pk_serde = this
            .pk_serde
            .as_ref()
            .expect("pk_serde must be set when intermediate_state_table is present");

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
        let new_row = OwnedRow::new(new_row_values);

        // Upsert: update if old row exists, otherwise insert
        if let Some(old_row) = partition.intermediate_state_row.take() {
            intermediate_state_table.update(old_row, new_row.clone());
        } else {
            intermediate_state_table.insert(new_row.clone());
        }
        partition.intermediate_state_row = Some(new_row);
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

        let mut partition = Partition {
            states: WindowStates::new(this.calls.iter().map(create_window_state).try_collect()?),
            curr_row_buffer: Default::default(),
            intermediate_state_row: None,
        };

        // If intermediate state table exists, load and restore intermediate state snapshots
        Self::load_intermediate_state(this, &mut partition, &partition_key, encoded_partition_key)
            .await?;

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

    /// Drain all ready windows from a partition, appending output rows to `builders`.
    /// Returns `true` if any output was produced.
    fn drain_ready_windows(
        this: &mut ExecutorInner<S>,
        partition: &mut Partition,
        partition_key: &OwnedRow,
        builders: &mut [ArrayBuilderImpl],
    ) -> StreamExecutorResult<bool> {
        let mut has_output = false;
        while partition.states.are_ready() {
            has_output = true;

            let (ret_values, evict_hint) = partition.states.slide()?;
            let curr_row = partition
                .curr_row_buffer
                .pop_front()
                .expect("ready window must have corresponding current row");

            for (builder, datum) in builders.iter_mut().zip_eq_debug(
                curr_row
                    .iter()
                    .chain(ret_values.iter().map(|v| v.to_datum_ref())),
            ) {
                builder.append(datum);
            }

            if let StateEvictHint::CanEvict(keys_to_evict) = evict_hint {
                for key in keys_to_evict {
                    let order_key = memcmp_encoding::decode_row(
                        &key.order_key,
                        &[this.schema[this.order_key_index].data_type()],
                        &[OrderType::ascending()],
                    )?;
                    let state_row_pk = partition_key.chain(order_key).chain(key.pk);
                    let state_row = {
                        // FIXME(rc): quite hacky here, we may need `state_table.delete_by_pk`
                        let mut state_row = vec![None; this.state_table_schema_len];
                        for (i_in_pk, &i) in this.state_table.pk_indices().iter().enumerate() {
                            state_row[i] = state_row_pk.datum_at(i_in_pk).to_owned_datum();
                        }
                        OwnedRow::new(state_row)
                    };
                    this.state_table.delete(state_row);
                }
            }
        }
        Ok(has_output)
    }

    async fn apply_chunk(
        this: &mut ExecutorInner<S>,
        vars: &mut ExecutionVars<S>,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        let mut builders = this.schema.create_array_builders(chunk.capacity()); // just an estimate
        let mut dirty_partitions: HashMap<MemcmpEncoded, OwnedRow> = HashMap::new();
        // Capture old mns (before append) for open sessions tracking.
        let track_sessions = this.open_sessions_tracker.is_some();
        let mut session_old_mns: HashMap<MemcmpEncoded, (OwnedRow, Option<MemcmpEncoded>)> =
            HashMap::new();

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

            // Capture old mns before any mutation (first time per partition per chunk).
            if track_sessions {
                session_old_mns
                    .entry(encoded_partition_key.clone())
                    .or_insert_with(|| {
                        (
                            partition_key.clone(),
                            OpenSessionsTracker::<S>::get_mns(partition),
                        )
                    });
            }

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

            let has_output =
                Self::drain_ready_windows(this, partition, &partition_key, &mut builders)?;

            if has_output && this.intermediate_state_table.is_some() {
                dirty_partitions
                    .entry(encoded_partition_key.clone())
                    .or_insert(partition_key.clone());
            }
        }

        // Persist intermediate state snapshots once per dirty partition at the end of the chunk.
        for (encoded_partition_key, partition_key) in &dirty_partitions {
            let partition = &mut *vars.partitions.get_mut(encoded_partition_key).unwrap();
            Self::persist_intermediate_state(this, partition, partition_key);
        }

        // Update open sessions tracker: compare old_mns (before append) with new_mns (after).
        if !session_old_mns.is_empty() {
            let tracker = this.open_sessions_tracker.as_mut().unwrap();
            for (encoded_partition_key, (partition_key, old_mns)) in &session_old_mns {
                let partition = vars.partitions.get_mut(encoded_partition_key).unwrap();
                let new_mns = OpenSessionsTracker::<S>::get_mns(&partition);
                tracker.update(partition_key, old_mns, &new_mns);
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

    async fn apply_watermark(
        this: &mut ExecutorInner<S>,
        vars: &mut ExecutionVars<S>,
        watermark: &Watermark,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        let Some(tracker) = &this.open_sessions_tracker else {
            return Ok(None);
        };
        if watermark.col_idx != this.order_key_index {
            return Ok(None);
        }

        let watermark_encoded = memcmp_encoding::encode_row(
            row::once(Some(watermark.val.clone())),
            &[OrderType::ascending()],
        )?;

        let matching_entries = tracker.scan_partitions_to_close(&watermark_encoded).await?;

        if matching_entries.is_empty() {
            return Ok(None);
        }

        let mut builders = this.schema.create_array_builders(0);
        let mut dirty_partitions: HashMap<MemcmpEncoded, OwnedRow> = HashMap::new();
        let mut sessions_to_delete: Vec<(OwnedRow, MemcmpEncoded)> = Vec::new();

        for (encoded_partition_key, partition_key, mns_from_table) in matching_entries {
            // Ensure partition is in cache (reload from state table if evicted).
            Self::ensure_key_in_cache(
                this,
                &mut vars.partitions,
                &partition_key,
                &encoded_partition_key,
            )
            .await?;

            let mut partition = vars.partitions.get_mut(&encoded_partition_key).unwrap();
            partition.states.on_watermark(&watermark_encoded);

            if !partition.states.are_ready() {
                // TODO(wrbd): When mixing session frames with non-session frames (e.g. ROWS),
                // the session frame may be ready but other frames block the output. The stale
                // mns entry stays in open_sessions_table and gets re-scanned on every watermark.
                // This is correct but causes unnecessary partition reloads. Consider tracking
                // "watermark-checked but not ready" partitions to skip them on subsequent scans.
                continue;
            }

            let has_output =
                Self::drain_ready_windows(this, &mut partition, &partition_key, &mut builders)?;

            if partition.curr_row_buffer.is_empty() {
                sessions_to_delete.push((partition_key.clone(), mns_from_table));
            }

            if has_output && this.intermediate_state_table.is_some() {
                dirty_partitions
                    .entry(encoded_partition_key)
                    .or_insert(partition_key);
            }
        }

        // Apply deferred deletions.
        if !sessions_to_delete.is_empty() {
            let tracker = this.open_sessions_tracker.as_mut().unwrap();
            for (partition_key, mns) in &sessions_to_delete {
                tracker.delete(partition_key, mns);
            }
        }

        for (encoded_partition_key, partition_key) in &dirty_partitions {
            let partition = &mut *vars.partitions.get_mut(encoded_partition_key).unwrap();
            Self::persist_intermediate_state(this, partition, partition_key);
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
        if let Some(intermediate_state_table) = &mut this.intermediate_state_table {
            intermediate_state_table.init_epoch(first_epoch).await?;
        }
        if let Some(tracker) = &mut this.open_sessions_tracker {
            tracker.table.init_epoch(first_epoch).await?;
        }

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Watermark(watermark) => {
                    let output_chunk =
                        Self::apply_watermark(&mut this, &mut vars, &watermark).await?;
                    if let Some(chunk) = output_chunk {
                        yield Message::Chunk(chunk);
                    }
                    this.state_table.try_flush().await?;
                    if let Some(intermediate_state_table) = &mut this.intermediate_state_table {
                        intermediate_state_table.try_flush().await?;
                    }
                    if let Some(tracker) = &mut this.open_sessions_tracker {
                        tracker.table.try_flush().await?;
                    }
                }
                Message::Chunk(chunk) => {
                    let output_chunk = Self::apply_chunk(&mut this, &mut vars, chunk).await?;
                    if let Some(chunk) = output_chunk {
                        yield Message::Chunk(chunk);
                    }
                    this.state_table.try_flush().await?;
                    if let Some(intermediate_state_table) = &mut this.intermediate_state_table {
                        intermediate_state_table.try_flush().await?;
                    }
                    if let Some(tracker) = &mut this.open_sessions_tracker {
                        tracker.table.try_flush().await?;
                    }
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
                    let open_sessions_post_commit =
                        if let Some(tracker) = &mut this.open_sessions_tracker {
                            Some(tracker.table.commit(barrier.epoch).await?)
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
                    if let Some(open_sessions_post_commit) = open_sessions_post_commit
                        && let Some((_, stale)) = open_sessions_post_commit
                            .post_yield_barrier(update_vnode_bitmap)
                            .await?
                    {
                        cache_may_stale = cache_may_stale || stale;
                    }
                    if cache_may_stale {
                        vars.partitions.clear();
                    }
                }
            }
        }
    }
}
