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

#![allow(dead_code)]

use std::collections::VecDeque;
use std::marker::PhantomData;

use futures::StreamExt;
use futures_async_stream::{for_await, try_stream};
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl, ToDatumRef};
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_common::util::memcmp_encoding;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::{must_match, row};
use risingwave_expr::function::window::WindowFuncCall;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use super::state::{create_window_state, WindowState};
use super::MemcmpEncoded;
use crate::cache::{new_unbounded, ManagedLruCache};
use crate::common::table::state_table::StateTable;
use crate::executor::over_window::state::{StateEvictHint, StateKey};
use crate::executor::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor,
    ExecutorInfo, Message, PkIndices, PkIndicesRef, StreamExecutorError, StreamExecutorResult,
    Watermark,
};
use crate::task::AtomicU64Ref;

struct Partition {
    states: Vec<Box<dyn WindowState + Send>>,
    curr_row_buffer: VecDeque<OwnedRow>,
}

impl Partition {
    fn new(calls: &[WindowFuncCall]) -> StreamExecutorResult<Self> {
        let states = calls.iter().map(create_window_state).try_collect()?;
        Ok(Self {
            states,
            curr_row_buffer: Default::default(),
        })
    }

    fn is_aligned(&self) -> bool {
        if self.states.is_empty() {
            true
        } else {
            self.states
                .iter()
                .map(|state| state.curr_window().key)
                .all_equal()
        }
    }

    fn is_ready(&self) -> bool {
        debug_assert!(self.is_aligned());
        self.states.iter().all(|state| state.curr_window().is_ready)
    }

    fn curr_window_key(&self) -> Option<&StateKey> {
        debug_assert!(self.is_aligned());
        self.states
            .first()
            .and_then(|state| state.curr_window().key)
    }
}

impl EstimateSize for Partition {
    fn estimated_heap_size(&self) -> usize {
        // FIXME: implement correct size
        // https://github.com/risingwavelabs/risingwave/issues/8957
        0
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
/// [`SortBuffer`]: crate::executor::sort_buffer::SortBuffer
///
/// Basic idea:
///
/// ```ignore
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
/// - State table schema = input schema, pk = `partition key | order key | input pk`.
/// - Output schema = input schema + window function results.
/// - Rows in range (`curr evict row`, `curr input row`] are in state table.
/// - `curr evict row` <= min(last evict rows of all `WindowState`s).
/// - `WindowState` should output agg result for `curr output row`.
/// - Recover: iterate through state table, push rows to `WindowState`, ignore ready windows.
pub struct EowcOverWindowExecutor<S: StateStore> {
    input: Box<dyn Executor>,
    inner: ExecutorInner<S>,
}

struct ExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    calls: Vec<WindowFuncCall>,
    pk_data_types: Vec<DataType>,
    input_pk_indices: Vec<usize>,
    partition_key_indices: Vec<usize>,
    order_key_index: usize, // no `OrderType` here, cuz we expect the input is ascending
    state_table: StateTable<S>,
    watermark_epoch: AtomicU64Ref,
}

struct ExecutionVars<S: StateStore> {
    partitions: PartitionCache,
    last_watermark: Option<ScalarImpl>,
    _phantom: PhantomData<S>,
}

impl<S: StateStore> Executor for EowcOverWindowExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.executor_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.inner.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.inner.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.inner.info.identity
    }
}

pub struct EowcOverWindowExecutorArgs<S: StateStore> {
    pub input: BoxedExecutor,

    pub actor_ctx: ActorContextRef,
    pub pk_indices: PkIndices,
    pub executor_id: u64,

    pub calls: Vec<WindowFuncCall>,
    pub partition_key_indices: Vec<usize>,
    pub order_key_index: usize,
    pub state_table: StateTable<S>,
    pub watermark_epoch: AtomicU64Ref,
}

impl<S: StateStore> EowcOverWindowExecutor<S> {
    pub fn new(args: EowcOverWindowExecutorArgs<S>) -> Self {
        let input_info = args.input.info();

        let schema = {
            let mut schema = input_info.schema.clone();
            args.calls.iter().for_each(|call| {
                schema.fields.push(Field::unnamed(call.return_type.clone()));
            });
            schema
        };

        let pk_data_types = input_info
            .pk_indices
            .iter()
            .map(|&i| input_info.schema.fields()[i].data_type())
            .collect();

        Self {
            input: args.input,
            inner: ExecutorInner {
                actor_ctx: args.actor_ctx,
                info: ExecutorInfo {
                    schema,
                    pk_indices: args.pk_indices,
                    identity: format!("EowcOverWindowExecutor {:X}", args.executor_id),
                },
                calls: args.calls,
                pk_data_types,
                input_pk_indices: input_info.pk_indices,
                partition_key_indices: args.partition_key_indices,
                order_key_index: args.order_key_index,
                state_table: args.state_table,
                watermark_epoch: args.watermark_epoch,
            },
        }
    }

    async fn ensure_key_in_cache(
        this: &mut ExecutorInner<S>,
        cache: &mut PartitionCache,
        partition_key: impl Row,
        encoded_partition_key: &MemcmpEncoded,
    ) -> StreamExecutorResult<()> {
        if cache.contains(encoded_partition_key) {
            return Ok(());
        }

        let mut partition = Partition::new(&this.calls)?;

        // Recover states from state table.
        let table_iter = this
            .state_table
            .iter_with_pk_prefix(partition_key, PrefetchOptions::new_for_exhaust_iter())
            .await?;

        #[for_await]
        for row in table_iter {
            let row: OwnedRow = row?;
            let order_key = row
                .datum_at(this.order_key_index)
                .expect("order key column must be non-NULL")
                .into_scalar_impl();
            let encoded_pk = memcmp_encoding::encode_row(
                (&row).project(&this.input_pk_indices),
                &vec![OrderType::ascending(); this.input_pk_indices.len()],
            )?
            .into_boxed_slice();
            let key = StateKey {
                order_key,
                encoded_pk,
            };
            for (call, state) in this.calls.iter().zip_eq_fast(&mut partition.states) {
                state.append(
                    key.clone(),
                    (&row)
                        .project(&call.args.val_indices())
                        .into_owned_row()
                        .into_inner()
                        .into(),
                );
            }
            partition.curr_row_buffer.push_back(row);
        }

        // Ensure states correctness.
        assert!(partition.is_aligned());

        // Ignore ready windows (all ready windows were outputted before).
        while partition.is_ready() {
            for state in &mut partition.states {
                state.output()?;
            }
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
        let mut builders = this.info.schema.create_array_builders(chunk.capacity()); // just an estimate

        // We assume that the input is sorted by order key.
        for record in chunk.records() {
            let input_row = must_match!(record, Record::Insert { new_row } => new_row);

            let partition_key = input_row
                .project(&this.partition_key_indices)
                .into_owned_row();
            let encoded_partition_key = memcmp_encoding::encode_row(
                &partition_key,
                &vec![OrderType::ascending(); this.partition_key_indices.len()],
            )?
            .into_boxed_slice();

            // Get the partition.
            Self::ensure_key_in_cache(
                this,
                &mut vars.partitions,
                &partition_key,
                &encoded_partition_key,
            )
            .await?;
            let mut partition = vars.partitions.get_mut(&encoded_partition_key).unwrap();

            // Materialize input to state table.
            this.state_table.insert(input_row);

            // Feed the row to all window states.
            let order_key = input_row
                .datum_at(this.order_key_index)
                .expect("order key column must be non-NULL")
                .into_scalar_impl();
            let encoded_pk = memcmp_encoding::encode_row(
                input_row.project(&this.input_pk_indices),
                &vec![OrderType::ascending(); this.input_pk_indices.len()],
            )?
            .into_boxed_slice();
            let key = StateKey {
                order_key,
                encoded_pk,
            };
            for (call, state) in this.calls.iter().zip_eq_fast(&mut partition.states) {
                state.append(
                    key.clone(),
                    input_row
                        .project(call.args.val_indices())
                        .into_owned_row()
                        .into_inner()
                        .into(),
                );
            }
            partition
                .curr_row_buffer
                .push_back(input_row.into_owned_row());

            while partition.is_ready() {
                // The partition is ready to output, so we can produce a row.

                // Get all outputs.
                let (ret_values, evict_hints): (Vec<_>, Vec<_>) = {
                    let tmp: Vec<_> = partition
                        .states
                        .iter_mut()
                        .map(|state| state.output().map(|o| (o.return_value, o.evict_hint)))
                        .try_collect()?;
                    tmp.into_iter().unzip()
                };
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
                    builder.append_datum(datum);
                }

                // Evict unneeded rows from state table.
                let evict_hint = evict_hints
                    .into_iter()
                    .reduce(StateEvictHint::merge)
                    .expect("# of evict hints = # of window func calls");
                if let StateEvictHint::CanEvict(keys_to_evict) = evict_hint {
                    for key in keys_to_evict {
                        let pk = memcmp_encoding::decode_row(
                            &key.encoded_pk,
                            &this.pk_data_types,
                            &vec![OrderType::ascending(); this.input_pk_indices.len()],
                        )?;
                        let state_row_pk = (&partition_key)
                            .chain(row::once(Some(key.order_key)))
                            .chain(pk);
                        // NOTE: We don't know the value of the row here, so the table must allow
                        // inconsistent ops.
                        this.state_table.delete(state_row_pk);
                    }
                }
            }
        }

        let columns: Vec<Column> = builders.into_iter().map(|b| b.finish().into()).collect();
        let chunk_size = columns[0].len();
        Ok(if chunk_size > 0 {
            Some(StreamChunk::new(
                vec![Op::Insert; chunk_size],
                columns,
                None,
            ))
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

        let mut vars = ExecutionVars {
            partitions: new_unbounded(this.watermark_epoch.clone()),
            last_watermark: None,
            _phantom: PhantomData::<S>,
        };

        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        this.state_table.init_epoch(barrier.epoch);
        vars.partitions.update_epoch(barrier.epoch.curr);

        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Watermark(_) => {
                    // Since we assume the input a `Sort`, we can emit watermarks by ourselves and
                    // ignore any input watermarks.
                    continue;
                }
                Message::Chunk(chunk) => {
                    let output_chunk = Self::apply_chunk(&mut this, &mut vars, chunk).await?;
                    if let Some(chunk) = output_chunk {
                        let first_order_key = chunk.columns()[this.order_key_index]
                            .array_ref()
                            .datum_at(0)
                            .expect("order key must not be NULL");

                        if vars.last_watermark.is_none()
                            || vars.last_watermark.as_ref().unwrap() < &first_order_key
                        {
                            vars.last_watermark = Some(first_order_key.clone());
                            yield Message::Watermark(Watermark::new(
                                this.order_key_index,
                                this.info.schema.fields()[this.order_key_index].data_type(),
                                first_order_key,
                            ));
                        }

                        yield Message::Chunk(chunk);
                    }
                }
                Message::Barrier(barrier) => {
                    this.state_table.commit(barrier.epoch).await?;
                    vars.partitions.evict();

                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(this.actor_ctx.id) {
                        let (_, cache_may_stale) =
                            this.state_table.update_vnode_bitmap(vnode_bitmap);
                        if cache_may_stale {
                            vars.partitions.clear();
                        }
                    }

                    vars.partitions.update_epoch(barrier.epoch.curr);

                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::function::aggregate::{AggArgs, AggKind};
    use risingwave_expr::function::window::{Frame, FrameBound, WindowFuncCall, WindowFuncKind};
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::StateStore;

    use super::{EowcOverWindowExecutor, EowcOverWindowExecutorArgs};
    use crate::common::table::state_table::StateTable;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};
    use crate::executor::{ActorContext, BoxedMessageStream, Executor};

    async fn create_executor<S: StateStore>(
        calls: Vec<WindowFuncCall>,
        store: S,
    ) -> (MessageSender, BoxedMessageStream) {
        let input_schema = Schema::new(vec![
            Field::unnamed(DataType::Int64),   // order key
            Field::unnamed(DataType::Varchar), // partition key
            Field::unnamed(DataType::Int64),   // pk
            Field::unnamed(DataType::Int32),   // x
        ]);
        let input_pk_indices = vec![2];
        let partition_key_indices = vec![1];
        let order_key_index = 0;

        let table_columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64), // order key
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar), // partition key
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64), // pk
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Int32), // x
        ];
        let table_pk_indices = vec![1, 0, 2];
        let table_order_types = vec![
            OrderType::ascending(),
            OrderType::ascending(),
            OrderType::ascending(),
        ];

        let output_pk_indices = vec![2];

        let state_table = StateTable::new_without_distribution_inconsistent_op(
            store,
            TableId::new(1),
            table_columns,
            table_order_types,
            table_pk_indices,
        )
        .await;

        let (tx, source) = MockSource::channel(input_schema, input_pk_indices.clone());
        let executor = EowcOverWindowExecutor::new(EowcOverWindowExecutorArgs {
            input: source.boxed(),
            actor_ctx: ActorContext::create(123),
            pk_indices: output_pk_indices,
            executor_id: 1,
            calls,
            partition_key_indices,
            order_key_index,
            state_table,
            watermark_epoch: Arc::new(AtomicU64::new(0)),
        });
        (tx, executor.boxed().execute())
    }

    #[tokio::test]
    async fn test_over_window() {
        let store = MemoryStateStore::new();
        let calls = vec![
            WindowFuncCall {
                kind: WindowFuncKind::Lag,
                args: AggArgs::Unary(DataType::Int32, 3),
                return_type: DataType::Int32,
                frame: Frame::Rows(FrameBound::Preceding(1), FrameBound::CurrentRow),
            },
            WindowFuncCall {
                kind: WindowFuncKind::Lead,
                args: AggArgs::Unary(DataType::Int32, 3),
                return_type: DataType::Int32,
                frame: Frame::Rows(FrameBound::CurrentRow, FrameBound::Following(1)),
            },
        ];

        {
            // test basic
            let (mut tx, mut over_window) = create_executor(calls.clone(), store.clone()).await;

            tx.push_barrier(1, false);
            over_window.expect_barrier().await;

            tx.push_chunk(StreamChunk::from_pretty(
                " I T  I   i
                + 1 p1 100 10
                + 1 p1 101 16
                + 4 p2 200 20",
            ));
            assert_eq!(1, over_window.expect_watermark().await.val.into_int64());
            assert_eq!(
                over_window.expect_chunk().await,
                StreamChunk::from_pretty(
                    " I T  I   i  i  i
                    + 1 p1 100 10 .  16"
                )
            );

            tx.push_chunk(StreamChunk::from_pretty(
                " I T  I   i
                + 5 p1 102 18
                + 7 p2 201 22
                + 8 p3 300 33",
            ));
            // NOTE: no watermark message here, since watermark(1) was already received
            assert_eq!(
                over_window.expect_chunk().await,
                StreamChunk::from_pretty(
                    " I T  I   i  i  i
                    + 1 p1 101 16 10 18
                    + 4 p2 200 20 .  22"
                )
            );

            tx.push_barrier(2, false);
            over_window.expect_barrier().await;
        }

        {
            // test recovery
            let (mut tx, mut over_window) = create_executor(calls.clone(), store.clone()).await;

            tx.push_barrier(3, false);
            over_window.expect_barrier().await;

            tx.push_chunk(StreamChunk::from_pretty(
                " I  T  I   i
                + 10 p1 103 13
                + 12 p2 202 28
                + 13 p3 301 39",
            ));
            assert_eq!(5, over_window.expect_watermark().await.val.into_int64());
            assert_eq!(
                over_window.expect_chunk().await,
                StreamChunk::from_pretty(
                    " I T  I   i  i  i
                    + 5 p1 102 18 16 13
                    + 7 p2 201 22 20 28
                    + 8 p3 300 33 .  39"
                )
            );

            tx.push_barrier(4, false);
            over_window.expect_barrier().await;
        }
    }

    #[tokio::test]
    async fn test_over_window_aggregate() {
        let store = MemoryStateStore::new();
        let calls = vec![WindowFuncCall {
            kind: WindowFuncKind::Aggregate(AggKind::Sum),
            args: AggArgs::Unary(DataType::Int32, 3),
            return_type: DataType::Int64,
            frame: Frame::Rows(FrameBound::Preceding(1), FrameBound::Following(1)),
        }];

        let (mut tx, mut over_window) = create_executor(calls.clone(), store.clone()).await;

        tx.push_barrier(1, false);
        over_window.expect_barrier().await;

        tx.push_chunk(StreamChunk::from_pretty(
            " I T  I   i
            + 1 p1 100 10
            + 1 p1 101 16
            + 4 p1 102 20",
        ));
        assert_eq!(1, over_window.expect_watermark().await.val.into_int64());
        let chunk = over_window.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I T  I   i  I
                + 1 p1 100 10 26
                + 1 p1 101 16 46"
            )
        );
    }
}
