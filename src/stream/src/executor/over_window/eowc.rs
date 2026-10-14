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
use risingwave_common::types::ToDatumRef;
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_common::util::memcmp_encoding::{self, MemcmpEncoded};
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::{must_match, row};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_common_estimate_size::collections::EstimatedVecDeque;
use risingwave_expr::window_function::{
    StateEvictHint, StateKey, WindowFuncCall, WindowStates, create_window_state,
};
use risingwave_storage::store::PrefetchOptions;

use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::executor::prelude::*;

struct Partition {
    states: WindowStates,
    curr_row_buffer: EstimatedVecDeque<OwnedRow>,
}

impl EstimateSize for Partition {
    fn estimated_heap_size(&self) -> usize {
        let mut total_size = self.curr_row_buffer.estimated_heap_size();
        for state in self.states.iter() {
            total_size += state.estimated_heap_size();
        }
        total_size
    }
}

type PartitionCache = ManagedLruCache<MemcmpEncoded, Partition>; // TODO(rc): use `K: HashKey` as key like in hash agg?

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
    /// For each column of the state table, its index in
    /// `partition key | order key | input stream key | NULL`, to rebuild the row of an evicted key.
    evict_row_indices: Vec<usize>,
    watermark_sequence: AtomicU64Ref,
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

        let mut partition = Partition {
            states: WindowStates::new(this.calls.iter().map(create_window_state).try_collect()?),
            curr_row_buffer: Default::default(),
        };

        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
        // Recover states from state table.
        let table_iter = this
            .state_table
            .iter_with_prefix(partition_key, sub_range, PrefetchOptions::default())
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
                        // FIXME(rc): quite hacky here, we may need `state_table.delete_by_pk`
                        let state_row = (&partition_key)
                            .chain(&order_key)
                            .chain(key.pk.as_inner())
                            .chain(row::once(Datum::None))
                            .project(&this.evict_row_indices);
                        // NOTE: We don't know the value of the row here, so the table must allow
                        // inconsistent ops.
                        this.state_table.delete(state_row);
                    }
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
                }
                Message::Barrier(barrier) => {
                    let post_commit = this.state_table.commit(barrier.epoch).await?;
                    vars.partitions.evict();

                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(this.actor_ctx.id);
                    yield Message::Barrier(barrier);

                    if let Some((_, cache_may_stale)) =
                        post_commit.post_yield_barrier(update_vnode_bitmap).await?
                        && cache_may_stale
                    {
                        vars.partitions.clear();
                    }
                }
            }
        }
    }
}
