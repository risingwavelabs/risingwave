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

use std::marker::PhantomData;

use futures::StreamExt;
use futures_async_stream::{for_await, try_stream};
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl, ToDatumRef};
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_common::util::memcmp_encoding;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::{must_match, row};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use self::call::WindowFuncCall;
use self::partition::Partition;
use self::state::StateKey;
use super::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor,
    ExecutorInfo, Message, PkIndices, StreamExecutorError, StreamExecutorResult, Watermark,
};
use crate::cache::{new_unbounded, ExecutorCache};
use crate::common::table::state_table::StateTable;
use crate::common::StateTableColumnMapping;
use crate::executor::over_window::state::StateEvictHint;
use crate::task::AtomicU64Ref;

mod call;
mod partition;
mod state;

type MemcmpEncoded = Box<[u8]>;
type PartitionCache = ExecutorCache<MemcmpEncoded, Partition>; // TODO(rc): use `K: HashKey` as key like in hash agg?

/// [`OverWindowExecutor`] consumes ordered input (on order key column with watermark) and outputs
/// window function results. One [`OverWindowExecutor`] can handle one combination of partition key
/// and order key.
///
/// The reason not to use [`SortBuffer`] is that the table schemas of [`OverWindowExecutor`] and
/// [`SortBuffer`] are different, since we don't have something like a _grouped_ sort buffer.
///
/// [`SortBuffer`]: crate::executor::sort_buffer::SortBuffer
///
/// State table schema:
///
/// ```ignore
/// partition key | order key | pk | window function arguments
/// ```
///
/// Output schema:
///
/// ```ignore
/// partition key | order key | pk | window function results
/// ```
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
/// - Rows in range (`curr evict row`, `curr input row`] are in `state_table`.
/// - `curr evict row` <= min(last evict rows of all `WindowState`s).
/// - `WindowState` should output agg result for `curr output row`.
/// - Recover: iterate through `state_table`, push rows to `WindowState`, ignore ready windows.
struct OverWindowExecutor<S: StateStore> {
    input: Box<dyn Executor>,
    inner: ExecutorInner<S>,
}

struct ExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    calls: Vec<WindowFuncCall>,
    state_table: StateTable<S>,
    col_mapping: StateTableColumnMapping,
    pk_data_types: Vec<DataType>,
    input_pk_indices: Vec<usize>,
    partition_key_indices: Vec<usize>,
    order_key_index: usize,
    watermark_epoch: AtomicU64Ref,
}

impl<S: StateStore> ExecutorInner<S> {
    fn output_partition_key_index(&self) -> usize {
        0
    }

    fn output_order_key_index(&self) -> usize {
        self.output_partition_key_index() + self.partition_key_indices.len()
    }

    fn _output_pk_index(&self) -> usize {
        self.output_order_key_index() + 1
    }
}

struct ExecutionVars<S: StateStore> {
    partitions: PartitionCache,
    last_watermark: Option<ScalarImpl>,
    _phantom: PhantomData<S>,
}

impl<S: StateStore> Executor for OverWindowExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.executor_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.inner.info.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
        &self.inner.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.inner.info.identity
    }
}

pub struct OverWindowExecutorArgs<S: StateStore> {
    pub input: BoxedExecutor,

    pub actor_ctx: ActorContextRef,
    pub pk_indices: PkIndices,
    pub executor_id: u64,

    pub calls: Vec<WindowFuncCall>,
    pub state_table: StateTable<S>,
    pub col_mapping: StateTableColumnMapping,
    pub partition_key_indices: Vec<usize>,
    pub order_key_index: usize,
    pub watermark_epoch: AtomicU64Ref,
}

impl<S: StateStore> OverWindowExecutor<S> {
    pub fn new(args: OverWindowExecutorArgs<S>) -> Self {
        let input_info = args.input.info();

        let fields = args
            .partition_key_indices
            .iter()
            .chain(std::iter::once(&args.order_key_index))
            .chain(&input_info.pk_indices)
            .map(|&i| input_info.schema.fields()[i].clone())
            .chain(
                args.calls
                    .iter()
                    .map(|call| Field::unnamed(call.return_type.clone())),
            )
            .collect();
        let schema = Schema::new(fields);

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
                    identity: format!("OverWindowExecutor {:X}", args.executor_id),
                },
                calls: args.calls,
                state_table: args.state_table,
                col_mapping: args.col_mapping,
                pk_data_types,
                input_pk_indices: input_info.pk_indices,
                partition_key_indices: args.partition_key_indices,
                order_key_index: args.order_key_index,
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

        let mut partition = Partition::new(&this.calls);

        // Recover states from state table.
        let table_iter = this
            .state_table
            .iter_with_pk_prefix(partition_key, PrefetchOptions::new_for_exhaust_iter())
            .await?;

        #[for_await]
        for row in table_iter {
            let row: OwnedRow = row?;
            let order_key = row
                .datum_at(
                    this.col_mapping
                        .upstream_to_state_table(this.order_key_index)
                        .unwrap(),
                )
                .expect("order key column must be non-NULL")
                .into_scalar_impl();
            let encoded_pk = memcmp_encoding::encode_row(
                (&row).project(
                    &this
                        .input_pk_indices
                        .iter()
                        .map(|idx| this.col_mapping.upstream_to_state_table(*idx).unwrap())
                        .collect_vec(),
                ),
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
                        .project(
                            &call
                                .args
                                .val_indices()
                                .iter()
                                .map(|idx| this.col_mapping.upstream_to_state_table(*idx).unwrap())
                                .collect_vec(),
                        )
                        .into_owned_row()
                        .into_inner()
                        .into(),
                );
            }
        }

        // Ensure states correctness.
        assert!(partition.is_aligned());

        // Ignore ready windows (all ready windows were outputted before).
        while partition
            .states
            .iter()
            .all(|state| state.curr_window().is_ready)
        {
            partition.states.iter_mut().for_each(|state| {
                state.slide();
            });
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
            let partition = vars.partitions.get_mut(&encoded_partition_key).unwrap();

            // Materialize input to state table.
            this.state_table
                .insert(input_row.project(this.col_mapping.upstream_columns()));

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

            while partition.is_ready() {
                // The partition is ready to output, so we can produce a row.
                let key = partition
                    .curr_window_key()
                    .cloned()
                    .expect("ready window must have state key");
                let pk = memcmp_encoding::decode_row(
                    &key.encoded_pk,
                    &this.pk_data_types,
                    &vec![OrderType::ascending(); this.input_pk_indices.len()],
                )?;

                // Get all outputs.
                let (ret_values, evict_hints): (Vec<_>, Vec<_>) = partition
                    .states
                    .iter_mut()
                    .map(|state| state.slide())
                    .map(|o| (o.return_value, o.evict_hint))
                    .unzip();

                // Append to output builders.
                let key_part = (&partition_key)
                    .chain(row::once(Some(key.order_key)))
                    .chain(pk);
                for (builder, datum) in builders.iter_mut().zip_eq_debug(
                    key_part
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

        let columns: Vec<Column> = builders
            .into_iter()
            .map(|b| b.finish().into())
            .collect_vec();
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
        let OverWindowExecutor {
            input,
            inner: mut this,
        } = self;

        let mut vars = ExecutionVars {
            partitions: ExecutorCache::new(new_unbounded(this.watermark_epoch.clone())),
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
                        let output_order_key_idx = this.output_order_key_index();
                        let first_order_key = chunk.columns()[output_order_key_idx]
                            .array_ref()
                            .datum_at(0)
                            .expect("order key must not be NULL");

                        if vars.last_watermark.is_none()
                            || vars.last_watermark.as_ref().unwrap() < &first_order_key
                        {
                            vars.last_watermark = Some(first_order_key.clone());
                            yield Message::Watermark(Watermark::new(
                                output_order_key_idx,
                                this.info.schema.fields()[output_order_key_idx].data_type(),
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
    use risingwave_expr::expr::WindowFuncKind;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::StateStore;

    use super::call::{Frame, WindowFuncCall};
    use super::{OverWindowExecutor, OverWindowExecutorArgs};
    use crate::common::table::state_table::StateTable;
    use crate::common::StateTableColumnMapping;
    use crate::executor::aggregation::AggArgs;
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
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Varchar), // partition key
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),   // order key
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),   // pk
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Int32),   // x
        ];
        let table_pk_indices = vec![0, 1, 2];
        let table_order_types = vec![
            OrderType::ascending(),
            OrderType::ascending(),
            OrderType::ascending(),
        ];
        let col_mapping = StateTableColumnMapping::new(vec![1, 0, 2, 3], None);

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
        let executor = OverWindowExecutor::new(OverWindowExecutorArgs {
            input: source.boxed(),
            actor_ctx: ActorContext::create(123),
            pk_indices: output_pk_indices,
            executor_id: 1,
            calls,
            state_table,
            col_mapping,
            partition_key_indices,
            order_key_index,
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
                frame: Frame::Offset(-1),
            },
            WindowFuncCall {
                kind: WindowFuncKind::Lead,
                args: AggArgs::Unary(DataType::Int32, 3),
                return_type: DataType::Int32,
                frame: Frame::Offset(1),
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
                    " T  I I   i  i
                    + p1 1 100 .  16"
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
                    " T  I I   i  i
                    + p1 1 101 10 18
                    + p2 4 200 .  22"
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
                    " T  I I   i  i
                    + p1 5 102 16 13
                    + p2 7 201 20 28
                    + p3 8 300 .  39"
                )
            );

            tx.push_barrier(4, false);
            over_window.expect_barrier().await;
        }
    }
}
