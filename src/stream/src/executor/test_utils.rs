// Copyright 2024 RisingWave Labs
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

use async_trait::async_trait;
use futures::{FutureExt, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::epoch::{test_epoch, EpochExt};
use tokio::sync::mpsc;

use super::error::StreamExecutorError;
use super::{
    Barrier, BoxedMessageStream, Execute, Executor, ExecutorInfo, Message, MessageStream,
    StreamChunk, StreamExecutorResult, Watermark,
};

pub mod prelude {
    pub use std::sync::atomic::AtomicU64;
    pub use std::sync::Arc;

    pub use risingwave_common::array::StreamChunk;
    pub use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    pub use risingwave_common::test_prelude::StreamChunkTestExt;
    pub use risingwave_common::types::DataType;
    pub use risingwave_common::util::sort_util::OrderType;
    pub use risingwave_storage::memory::MemoryStateStore;
    pub use risingwave_storage::StateStore;

    pub use crate::common::table::state_table::StateTable;
    pub use crate::executor::test_utils::expr::build_from_pretty;
    pub use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};
    pub use crate::executor::{ActorContext, BoxedMessageStream, Execute, PkIndices};
}

pub struct MockSource {
    rx: mpsc::UnboundedReceiver<Message>,

    /// Whether to send a `Stop` barrier on stream finish.
    stop_on_finish: bool,
}

/// A wrapper around `Sender<Message>`.
pub struct MessageSender(mpsc::UnboundedSender<Message>);

impl MessageSender {
    #[allow(dead_code)]
    pub fn push_chunk(&mut self, chunk: StreamChunk) {
        self.0.send(Message::Chunk(chunk)).unwrap();
    }

    #[allow(dead_code)]
    pub fn push_barrier(&mut self, epoch: u64, stop: bool) {
        let mut barrier = Barrier::new_test_barrier(epoch);
        if stop {
            barrier = barrier.with_stop();
        }
        self.0.send(Message::Barrier(barrier)).unwrap();
    }

    pub fn send_barrier(&self, barrier: Barrier) {
        self.0.send(Message::Barrier(barrier)).unwrap();
    }

    #[allow(dead_code)]
    pub fn push_barrier_with_prev_epoch_for_test(
        &mut self,
        cur_epoch: u64,
        prev_epoch: u64,
        stop: bool,
    ) {
        let mut barrier = Barrier::with_prev_epoch_for_test(cur_epoch, prev_epoch);
        if stop {
            barrier = barrier.with_stop();
        }
        self.0.send(Message::Barrier(barrier)).unwrap();
    }

    #[allow(dead_code)]
    pub fn push_watermark(&mut self, col_idx: usize, data_type: DataType, val: ScalarImpl) {
        self.0
            .send(Message::Watermark(Watermark {
                col_idx,
                data_type,
                val,
            }))
            .unwrap();
    }

    #[allow(dead_code)]
    pub fn push_int64_watermark(&mut self, col_idx: usize, val: i64) {
        self.push_watermark(col_idx, DataType::Int64, ScalarImpl::Int64(val));
    }
}

impl std::fmt::Debug for MockSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockSource").finish()
    }
}

impl MockSource {
    #[allow(dead_code)]
    pub fn channel() -> (MessageSender, Self) {
        let (tx, rx) = mpsc::unbounded_channel();
        let source = Self {
            rx,
            stop_on_finish: true,
        };
        (MessageSender(tx), source)
    }

    #[allow(dead_code)]
    pub fn with_messages(msgs: Vec<Message>) -> Self {
        let (tx, source) = Self::channel();
        for msg in msgs {
            tx.0.send(msg).unwrap();
        }
        source
    }

    pub fn with_chunks(chunks: Vec<StreamChunk>) -> Self {
        let (tx, source) = Self::channel();
        for chunk in chunks {
            tx.0.send(Message::Chunk(chunk)).unwrap();
        }
        source
    }

    #[allow(dead_code)]
    #[must_use]
    pub fn stop_on_finish(self, stop_on_finish: bool) -> Self {
        Self {
            stop_on_finish,
            ..self
        }
    }

    pub fn into_executor(self, schema: Schema, pk_indices: Vec<usize>) -> Executor {
        Executor::new(
            ExecutorInfo {
                schema,
                pk_indices,
                identity: "MockSource".to_owned(),
            },
            self.boxed(),
        )
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self: Box<Self>) {
        let mut epoch = test_epoch(1);

        while let Some(msg) = self.rx.recv().await {
            epoch.inc_epoch();
            yield msg;
        }

        if self.stop_on_finish {
            yield Message::Barrier(Barrier::new_test_barrier(epoch).with_stop());
        }
    }
}

impl Execute for MockSource {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

/// `row_nonnull` builds a `OwnedRow` with concrete values.
/// TODO: add macro row!, which requires a new trait `ToScalarValue`.
#[macro_export]
macro_rules! row_nonnull {
    [$( $value:expr ),*] => {
        {
            risingwave_common::row::OwnedRow::new(vec![$(Some($value.into()), )*])
        }
    };
}

/// Trait for testing `StreamExecutor` more easily.
///
/// With `next_unwrap_ready`, we can retrieve the next message from the executor without `await`ing,
/// so that we can immediately panic if the executor is not ready instead of getting stuck. This is
/// useful for testing.
#[async_trait]
pub trait StreamExecutorTestExt: MessageStream + Unpin {
    /// Asserts that the executor is pending (not ready) now.
    ///
    /// Panics if it is ready.
    fn next_unwrap_pending(&mut self) {
        if let Some(r) = self.try_next().now_or_never() {
            panic!("expect pending stream, but got `{:?}`", r);
        }
    }

    /// Asserts that the executor is ready now, returning the next message.
    ///
    /// Panics if it is pending.
    fn next_unwrap_ready(&mut self) -> StreamExecutorResult<Message> {
        match self.next().now_or_never() {
            Some(Some(r)) => r,
            Some(None) => panic!("expect ready stream, but got terminated"),
            None => panic!("expect ready stream, but got pending"),
        }
    }

    /// Asserts that the executor is ready on a [`StreamChunk`] now, returning the next chunk.
    ///
    /// Panics if it is pending or the next message is not a [`StreamChunk`].
    fn next_unwrap_ready_chunk(&mut self) -> StreamExecutorResult<StreamChunk> {
        self.next_unwrap_ready()
            .map(|msg| msg.into_chunk().expect("expect chunk"))
    }

    /// Asserts that the executor is ready on a [`Barrier`] now, returning the next barrier.
    ///
    /// Panics if it is pending or the next message is not a [`Barrier`].
    fn next_unwrap_ready_barrier(&mut self) -> StreamExecutorResult<Barrier> {
        self.next_unwrap_ready()
            .map(|msg| msg.into_barrier().expect("expect barrier"))
    }

    /// Asserts that the executor is ready on a [`Watermark`] now, returning the next barrier.
    ///
    /// Panics if it is pending or the next message is not a [`Watermark`].
    fn next_unwrap_ready_watermark(&mut self) -> StreamExecutorResult<Watermark> {
        self.next_unwrap_ready()
            .map(|msg| msg.into_watermark().expect("expect watermark"))
    }

    async fn expect_barrier(&mut self) -> Barrier {
        let msg = self.next().await.unwrap().unwrap();
        msg.into_barrier().unwrap()
    }

    async fn expect_chunk(&mut self) -> StreamChunk {
        let msg = self.next().await.unwrap().unwrap();
        msg.into_chunk().unwrap()
    }

    async fn expect_watermark(&mut self) -> Watermark {
        let msg = self.next().await.unwrap().unwrap();
        msg.into_watermark().unwrap()
    }
}

// FIXME: implement on any `impl MessageStream` if the analyzer works well.
impl StreamExecutorTestExt for BoxedMessageStream {}

pub mod expr {
    use risingwave_expr::expr::NonStrictExpression;

    pub fn build_from_pretty(s: impl AsRef<str>) -> NonStrictExpression {
        NonStrictExpression::for_test(risingwave_expr::expr::build_from_pretty(s))
    }
}

pub mod agg_executor {
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    use futures::future;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::hash::SerializedKey;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
    use risingwave_expr::aggregate::{AggCall, AggType, PbAggKind};
    use risingwave_pb::stream_plan::PbAggNodeVersion;
    use risingwave_storage::StateStore;

    use crate::common::table::state_table::StateTable;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::common::StateTableColumnMapping;
    use crate::executor::agg_common::{
        AggExecutorArgs, HashAggExecutorExtraArgs, SimpleAggExecutorExtraArgs,
    };
    use crate::executor::aggregation::AggStateStorage;
    use crate::executor::{
        ActorContext, ActorContextRef, Executor, ExecutorInfo, HashAggExecutor, PkIndices,
        SimpleAggExecutor,
    };

    /// Generate aggExecuter's schema from `input`, `agg_calls` and `group_key_indices`.
    /// For [`crate::executor::HashAggExecutor`], the group key indices should be provided.
    pub fn generate_agg_schema(
        input_ref: &Executor,
        agg_calls: &[AggCall],
        group_key_indices: Option<&[usize]>,
    ) -> Schema {
        let aggs = agg_calls
            .iter()
            .map(|agg| Field::unnamed(agg.return_type.clone()));

        let fields = if let Some(key_indices) = group_key_indices {
            let keys = key_indices
                .iter()
                .map(|idx| input_ref.schema().fields[*idx].clone());

            keys.chain(aggs).collect()
        } else {
            aggs.collect()
        };

        Schema { fields }
    }

    /// Create state storage for the given agg call.
    /// Should infer the schema in the same way as `LogicalAgg::infer_stream_agg_state`.
    pub async fn create_agg_state_storage<S: StateStore>(
        store: S,
        table_id: TableId,
        agg_call: &AggCall,
        group_key_indices: &[usize],
        pk_indices: &[usize],
        input_fields: Vec<Field>,
        is_append_only: bool,
    ) -> AggStateStorage<S> {
        match agg_call.agg_type {
            AggType::Builtin(PbAggKind::Min | PbAggKind::Max) if !is_append_only => {
                let mut column_descs = Vec::new();
                let mut order_types = Vec::new();
                let mut upstream_columns = Vec::new();
                let mut order_columns = Vec::new();

                let mut next_column_id = 0;
                let mut add_column = |upstream_idx: usize, data_type: DataType, order_type: Option<OrderType>| {
                    upstream_columns.push(upstream_idx);
                    column_descs.push(ColumnDesc::unnamed(
                        ColumnId::new(next_column_id),
                        data_type,
                    ));
                    if let Some(order_type) = order_type {
                        order_columns.push(ColumnOrder::new(upstream_idx as _, order_type));
                        order_types.push(order_type);
                    }
                    next_column_id += 1;
                };

                for idx in group_key_indices {
                    add_column(*idx, input_fields[*idx].data_type(), None);
                }

                add_column(agg_call.args.val_indices()[0], agg_call.args.arg_types()[0].clone(), if matches!(agg_call.agg_type, AggType::Builtin(PbAggKind::Max)) {
                    Some(OrderType::descending())
                } else {
                    Some(OrderType::ascending())
                });

                for idx in pk_indices {
                    add_column(*idx, input_fields[*idx].data_type(), Some(OrderType::ascending()));
                }

                let state_table = StateTable::from_table_catalog(
                    &gen_pbtable(
                        table_id,
                        column_descs,
                        order_types.clone(),
                        (0..order_types.len()).collect(),
                        0,
                    ),
                    store,
                    None,
                ).await;

                AggStateStorage::MaterializedInput { table: state_table, mapping: StateTableColumnMapping::new(upstream_columns, None), order_columns }
            }
            AggType::Builtin(
                PbAggKind::Min /* append only */
                | PbAggKind::Max /* append only */
                | PbAggKind::Sum
                | PbAggKind::Sum0
                | PbAggKind::Count
                | PbAggKind::Avg
                | PbAggKind::ApproxCountDistinct
            ) => {
                AggStateStorage::Value
            }
            _ => {
                panic!("no need to mock other agg kinds here");
            }
        }
    }

    /// Create intermediate state table for agg executor.
    pub async fn create_intermediate_state_table<S: StateStore>(
        store: S,
        table_id: TableId,
        agg_calls: &[AggCall],
        group_key_indices: &[usize],
        input_fields: Vec<Field>,
    ) -> StateTable<S> {
        let mut column_descs = Vec::new();
        let mut order_types = Vec::new();

        let mut next_column_id = 0;
        let mut add_column_desc = |data_type: DataType| {
            column_descs.push(ColumnDesc::unnamed(
                ColumnId::new(next_column_id),
                data_type,
            ));
            next_column_id += 1;
        };

        group_key_indices.iter().for_each(|idx| {
            add_column_desc(input_fields[*idx].data_type());
            order_types.push(OrderType::ascending());
        });

        agg_calls.iter().for_each(|agg_call| {
            add_column_desc(agg_call.return_type.clone());
        });

        StateTable::from_table_catalog_inconsistent_op(
            &gen_pbtable(
                table_id,
                column_descs,
                order_types,
                (0..group_key_indices.len()).collect(),
                0,
            ),
            store,
            None,
        )
        .await
    }

    /// NOTE(kwannoel): This should only be used by `test` or `bench`.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_boxed_hash_agg_executor<S: StateStore>(
        store: S,
        input: Executor,
        is_append_only: bool,
        agg_calls: Vec<AggCall>,
        row_count_index: usize,
        group_key_indices: Vec<usize>,
        pk_indices: PkIndices,
        extreme_cache_size: usize,
        emit_on_window_close: bool,
        executor_id: u64,
    ) -> Executor {
        let mut storages = Vec::with_capacity(agg_calls.iter().len());
        for (idx, agg_call) in agg_calls.iter().enumerate() {
            storages.push(
                create_agg_state_storage(
                    store.clone(),
                    TableId::new(idx as u32),
                    agg_call,
                    &group_key_indices,
                    &pk_indices,
                    input.info.schema.fields.clone(),
                    is_append_only,
                )
                .await,
            )
        }

        let intermediate_state_table = create_intermediate_state_table(
            store,
            TableId::new(agg_calls.len() as u32),
            &agg_calls,
            &group_key_indices,
            input.info.schema.fields.clone(),
        )
        .await;

        let schema = generate_agg_schema(&input, &agg_calls, Some(&group_key_indices));
        let info = ExecutorInfo {
            schema,
            pk_indices,
            identity: format!("HashAggExecutor {:X}", executor_id),
        };

        let exec = HashAggExecutor::<SerializedKey, S>::new(AggExecutorArgs {
            version: PbAggNodeVersion::Max,

            input,
            actor_ctx: ActorContext::for_test(123),
            info: info.clone(),

            extreme_cache_size,

            agg_calls,
            row_count_index,
            storages,
            intermediate_state_table,
            distinct_dedup_tables: Default::default(),
            watermark_epoch: Arc::new(AtomicU64::new(0)),

            extra: HashAggExecutorExtraArgs {
                group_key_indices,
                chunk_size: 1024,
                max_dirty_groups_heap_size: 64 << 20,
                emit_on_window_close,
            },
        })
        .unwrap();
        (info, exec).into()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn new_boxed_simple_agg_executor<S: StateStore>(
        actor_ctx: ActorContextRef,
        store: S,
        input: Executor,
        is_append_only: bool,
        agg_calls: Vec<AggCall>,
        row_count_index: usize,
        pk_indices: PkIndices,
        executor_id: u64,
        must_output_per_barrier: bool,
    ) -> Executor {
        let storages = future::join_all(agg_calls.iter().enumerate().map(|(idx, agg_call)| {
            create_agg_state_storage(
                store.clone(),
                TableId::new(idx as u32),
                agg_call,
                &[],
                &pk_indices,
                input.info.schema.fields.clone(),
                is_append_only,
            )
        }))
        .await;

        let intermediate_state_table = create_intermediate_state_table(
            store,
            TableId::new(agg_calls.len() as u32),
            &agg_calls,
            &[],
            input.info.schema.fields.clone(),
        )
        .await;

        let schema = generate_agg_schema(&input, &agg_calls, None);
        let info = ExecutorInfo {
            schema,
            pk_indices,
            identity: format!("SimpleAggExecutor {:X}", executor_id),
        };

        let exec = SimpleAggExecutor::new(AggExecutorArgs {
            version: PbAggNodeVersion::Max,

            input,
            actor_ctx,
            info: info.clone(),

            extreme_cache_size: 1024,

            agg_calls,
            row_count_index,
            storages,
            intermediate_state_table,
            distinct_dedup_tables: Default::default(),
            watermark_epoch: Arc::new(AtomicU64::new(0)),
            extra: SimpleAggExecutorExtraArgs {
                must_output_per_barrier,
            },
        })
        .unwrap();
        (info, exec).into()
    }
}

pub mod top_n_executor {
    use itertools::Itertools;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use crate::common::table::state_table::StateTable;
    use crate::common::table::test_utils::gen_pbtable;

    pub async fn create_in_memory_state_table(
        data_types: &[DataType],
        order_types: &[OrderType],
        pk_indices: &[usize],
    ) -> StateTable<MemoryStateStore> {
        create_in_memory_state_table_from_state_store(
            data_types,
            order_types,
            pk_indices,
            MemoryStateStore::new(),
        )
        .await
    }

    pub async fn create_in_memory_state_table_from_state_store(
        data_types: &[DataType],
        order_types: &[OrderType],
        pk_indices: &[usize],
        state_store: MemoryStateStore,
    ) -> StateTable<MemoryStateStore> {
        let column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
            .collect_vec();
        StateTable::from_table_catalog(
            &gen_pbtable(
                TableId::new(0),
                column_descs,
                order_types.to_vec(),
                pk_indices.to_vec(),
                0,
            ),
            state_store,
            None,
        )
        .await
    }
}

pub mod hash_join_executor {
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    use itertools::Itertools;
    use strum_macros::Display;
    use risingwave_common::array::{I64Array, Op};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableId};
    use risingwave_common::hash::Key128;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_pb::plan_common::JoinType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::monitor::StreamingMetrics;
    use crate::executor::prelude::StateTable;
    use crate::executor::test_utils::{MessageSender, MockSource};
    use crate::executor::{ActorContext, HashJoinExecutor, JoinParams, JoinType as ConstJoinType};

    #[derive(Clone, Copy, Debug, Display)]
    pub enum HashJoinWorkload {
        InCache,
        NotInCache,
    }

    pub async fn create_in_memory_state_table(
        mem_state: MemoryStateStore,
        data_types: &[DataType],
        order_types: &[OrderType],
        pk_indices: &[usize],
        table_id: u32,
    ) -> (StateTable<MemoryStateStore>, StateTable<MemoryStateStore>) {
        let column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
            .collect_vec();
        let state_table = StateTable::from_table_catalog(
            &gen_pbtable(
                TableId::new(table_id),
                column_descs,
                order_types.to_vec(),
                pk_indices.to_vec(),
                0,
            ),
            mem_state.clone(),
            None,
        )
        .await;

        // Create degree table
        let mut degree_table_column_descs = vec![];
        pk_indices.iter().enumerate().for_each(|(pk_id, idx)| {
            degree_table_column_descs.push(ColumnDesc::unnamed(
                ColumnId::new(pk_id as i32),
                data_types[*idx].clone(),
            ))
        });
        degree_table_column_descs.push(ColumnDesc::unnamed(
            ColumnId::new(pk_indices.len() as i32),
            DataType::Int64,
        ));
        let degree_state_table = StateTable::from_table_catalog(
            &gen_pbtable(
                TableId::new(table_id + 1),
                degree_table_column_descs,
                order_types.to_vec(),
                pk_indices.to_vec(),
                0,
            ),
            mem_state,
            None,
        )
        .await;
        (state_table, degree_state_table)
    }

    /// 1. Refill state table of build side.
    /// 2. Init executor.
    /// 3. Push data to the probe side.
    /// 4. Check memory utilization.
    pub async fn setup_bench_stream_hash_join(
        amp: usize,
        workload: HashJoinWorkload,
        join_type: JoinType,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        let fields = vec![DataType::Int64, DataType::Int64, DataType::Int64];
        let orders = vec![OrderType::ascending(), OrderType::ascending()];
        let state_store = MemoryStateStore::new();

        // Probe side
        let (lhs_state_table, lhs_degree_state_table) =
            create_in_memory_state_table(state_store.clone(), &fields, &orders, &[0, 1], 0).await;

        // Build side
        let (mut rhs_state_table, rhs_degree_state_table) =
            create_in_memory_state_table(state_store.clone(), &fields, &orders, &[0, 1], 2).await;

        // Insert 100K records into the build side.
        if matches!(workload, HashJoinWorkload::NotInCache) {
            let stream_chunk = build_chunk(amp, 200_000);
            // Write to state table.
            rhs_state_table.write_chunk(stream_chunk);
        }

        let schema = Schema::new(fields.iter().cloned().map(Field::unnamed).collect());

        let (tx_l, source_l) = MockSource::channel();
        let source_l = source_l.into_executor(schema.clone(), vec![1]);
        let (tx_r, source_r) = MockSource::channel();
        let source_r = source_r.into_executor(schema, vec![1]);

        // Schema is the concatenation of the two source schemas.
        // [lhs(jk):0, lhs(pk):1, lhs(value):2, rhs(jk):0, rhs(pk):1, rhs(value):2]
        // [0,         1,         2,            3,         4,         5           ]
        let schema: Vec<_> = [source_l.schema().fields(), source_r.schema().fields()]
            .concat()
            .into_iter()
            .collect();
        let schema_len = schema.len();
        let info = ExecutorInfo {
            schema: Schema { fields: schema },
            pk_indices: vec![0, 1, 3, 4],
            identity: "HashJoinExecutor".to_owned(),
        };

        // join-key is [0], primary-key is [1].
        let params_l = JoinParams::new(vec![0], vec![1]);
        let params_r = JoinParams::new(vec![0], vec![1]);

        match join_type {
            JoinType::Inner => {
                let executor = HashJoinExecutor::<Key128, MemoryStateStore, { ConstJoinType::Inner }>::new(
                    ActorContext::for_test(123),
                    info,
                    source_l,
                    source_r,
                    params_l,
                    params_r,
                    vec![false], // null-safe
                    (0..schema_len).collect_vec(),
                    None,   // condition, it is an eq join, we have no condition
                    vec![], // ineq pairs
                    lhs_state_table,
                    lhs_degree_state_table,
                    rhs_state_table,
                    rhs_degree_state_table,
                    Arc::new(AtomicU64::new(0)), // watermark epoch
                    false,                       // is_append_only
                    Arc::new(StreamingMetrics::unused()),
                    1024, // chunk_size
                    2048, // high_join_amplification_threshold
                );
                (tx_l, tx_r, executor.boxed().execute())
            }
            JoinType::LeftOuter => {
                let executor = HashJoinExecutor::<Key128, MemoryStateStore, { ConstJoinType::LeftOuter }>::new(
                    ActorContext::for_test(123),
                    info,
                    source_l,
                    source_r,
                    params_l,
                    params_r,
                    vec![false], // null-safe
                    (0..schema_len).collect_vec(),
                    None,   // condition, it is an eq join, we have no condition
                    vec![], // ineq pairs
                    lhs_state_table,
                    lhs_degree_state_table,
                    rhs_state_table,
                    rhs_degree_state_table,
                    Arc::new(AtomicU64::new(0)), // watermark epoch
                    false,                       // is_append_only
                    Arc::new(StreamingMetrics::unused()),
                    1024, // chunk_size
                    2048, // high_join_amplification_threshold
                );
                (tx_l, tx_r, executor.boxed().execute())
            }
            _ => panic!("Unsupported join type"),
        }

    }

    fn build_chunk(size: usize, join_key_value: i64) -> StreamChunk {
        // Create column [0]: join key. Each record has the same value, to trigger join amplification.
        let mut int64_jk_builder = DataType::Int64.create_array_builder(size);
        int64_jk_builder
            .append_array(&I64Array::from_iter(vec![Some(join_key_value); size].into_iter()).into());
        let jk = int64_jk_builder.finish();

        // Create column [1]: pk. The original pk will be here, it will be unique.
        let mut int64_pk_data_chunk_builder = DataType::Int64.create_array_builder(size);
        let seq = I64Array::from_iter((0..size as i64).map(Some));
        int64_pk_data_chunk_builder.append_array(&I64Array::from(seq).into());
        let pk = int64_pk_data_chunk_builder.finish();

        // Create column [2]: value. This can be an arbitrary value, so just clone the pk column.
        let values = pk.clone();

        // Build the stream chunk.
        let columns = vec![jk.into(), pk.into(), values.into()];
        let ops = vec![Op::Insert; size];
        StreamChunk::new(ops, columns)
    }

    pub async fn handle_streams(
        hash_join_workload: HashJoinWorkload,
        join_type: JoinType,
        amp: usize,
        mut tx_l: MessageSender,
        mut tx_r: MessageSender,
        mut stream: BoxedMessageStream,
    ) {
        // Init executors
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);

        if matches!(hash_join_workload, HashJoinWorkload::InCache) {
            // Push a single record into tx_r, so 100K records to be matched are cached.
            let chunk = build_chunk(amp, 200_000);
            tx_r.push_chunk(chunk);
        }

        // Push a chunk of records into tx_l, matches 100K records in the build side.
        let chunk_size = match hash_join_workload {
            HashJoinWorkload::InCache => 64,
            HashJoinWorkload::NotInCache => 1,
        };
        let chunk = match join_type {
            // Make sure all match
            JoinType::Inner => build_chunk(chunk_size, 200_000),
            // Make sure no match is found.
            JoinType::LeftOuter => build_chunk(chunk_size, 300_000),
            _ => panic!("Unsupported join type"),
        };
        tx_l.push_chunk(chunk);

        match stream.next().await {
            Some(Ok(Message::Barrier(b))) => {
                assert_eq!(b.epoch.curr, test_epoch(1));
            }
            other => {
                panic!("Expected a barrier, got {:?}", other);
            }
        }

        let expected_count = match join_type {
            JoinType::LeftOuter => chunk_size,
            JoinType::Inner => amp * chunk_size,
            _ => panic!("Unsupported join type"),
        };
        let mut current_count = 0;
        while current_count < expected_count {
            match stream.next().await {
                Some(Ok(Message::Chunk(c))) => {
                    current_count += c.cardinality();
                }
                other => {
                    panic!("Expected a barrier, got {:?}", other);
                }
            }
        }
    }
}
