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

use std::collections::VecDeque;
use futures::{FutureExt, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{DataType, ScalarImpl};
use tokio::sync::mpsc;
use risingwave_common::array::DataChunk;
use risingwave_common::field_generator::FieldGeneratorImpl;
use crate::executor::{BoxedExecutor, ExecutorInfo, PkIndicesRef};

use super::error::StreamExecutorError;
use super::{
    Barrier, BoxedMessageStream, Executor, Message, MessageStream, PkIndices, StreamChunk,
    StreamExecutorResult, Watermark,
};

const SEED: u64 = 0xFF67FEABBAEF76FF;

pub struct MockSource {
    schema: Schema,
    pk_indices: PkIndices,
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
        f.debug_struct("MockSource")
            .field("schema", &self.schema)
            .field("pk_indices", &self.pk_indices)
            .finish()
    }
}

impl MockSource {
    #[allow(dead_code)]
    pub fn channel(schema: Schema, pk_indices: PkIndices) -> (MessageSender, Self) {
        let (tx, rx) = mpsc::unbounded_channel();
        let source = Self {
            schema,
            pk_indices,
            rx,
            stop_on_finish: true,
        };
        (MessageSender(tx), source)
    }

    #[allow(dead_code)]
    pub fn with_messages(schema: Schema, pk_indices: PkIndices, msgs: Vec<Message>) -> Self {
        let (tx, source) = Self::channel(schema, pk_indices);
        for msg in msgs {
            tx.0.send(msg).unwrap();
        }
        source
    }

    pub fn with_chunks(schema: Schema, pk_indices: PkIndices, chunks: Vec<StreamChunk>) -> Self {
        let (tx, source) = Self::channel(schema, pk_indices);
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

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self: Box<Self>) {
        let mut epoch = 1;

        while let Some(msg) = self.rx.recv().await {
            epoch += 1;
            yield msg;
        }

        if self.stop_on_finish {
            yield Message::Barrier(Barrier::new_test_barrier(epoch).with_stop());
        }
    }
}

impl Executor for MockSource {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        "MockSource"
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
}

// FIXME: implement on any `impl MessageStream` if the analyzer works well.
impl StreamExecutorTestExt for BoxedMessageStream {}

pub mod agg_executor {
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::expr::AggKind;
    use risingwave_storage::StateStore;

    use crate::common::table::state_table::StateTable;
    use crate::common::StateTableColumnMapping;
    use crate::executor::agg_common::{AggExecutorArgs, SimpleAggExecutorExtraArgs};
    use crate::executor::aggregation::{AggCall, AggStateStorage};
    use crate::executor::{
        ActorContextRef, BoxedExecutor, Executor, GlobalSimpleAggExecutor, PkIndices,
    };

    /// Create state storage for the given agg call.
    /// Should infer the schema in the same way as `LogicalAgg::infer_stream_agg_state`.
    pub async fn create_agg_state_storage<S: StateStore>(
        store: S,
        table_id: TableId,
        agg_call: &AggCall,
        group_key_indices: &[usize],
        pk_indices: &[usize],
        input_ref: &dyn Executor,
    ) -> AggStateStorage<S> {
        match agg_call.kind {
            AggKind::Min | AggKind::Max if !agg_call.append_only => {
                let input_fields = input_ref.schema().fields();

                let mut column_descs = Vec::new();
                let mut order_types = Vec::new();
                let mut upstream_columns = Vec::new();

                let mut next_column_id = 0;
                let mut add_column = |upstream_idx: usize, data_type: DataType, order_type: OrderType| {
                    upstream_columns.push(upstream_idx);
                    column_descs.push(ColumnDesc::unnamed(
                        ColumnId::new(next_column_id),
                        data_type,
                    ));
                    next_column_id += 1;
                    order_types.push(order_type);
                };

                for idx in group_key_indices {
                    add_column(*idx, input_fields[*idx].data_type(), OrderType::ascending());
                }

                add_column(agg_call.args.val_indices()[0], agg_call.args.arg_types()[0].clone(), if agg_call.kind == AggKind::Max {
                    OrderType::descending()
                } else {
                    OrderType::ascending()
                });

                for idx in pk_indices {
                    add_column(*idx, input_fields[*idx].data_type(), OrderType::ascending());
                }

                let state_table = StateTable::new_without_distribution(
                    store,
                    table_id,
                    column_descs,
                    order_types.clone(),
                    (0..order_types.len()).collect(),
                ).await;

                AggStateStorage::MaterializedInput { table: state_table, mapping: StateTableColumnMapping::new(upstream_columns, None) }
            }
            AggKind::Min /* append only */
            | AggKind::Max /* append only */
            | AggKind::Sum
            | AggKind::Sum0
            | AggKind::Count
            | AggKind::Avg
            | AggKind::ApproxCountDistinct => {
                AggStateStorage::ResultValue
            }
            _ => {
                panic!("no need to mock other agg kinds here");
            }
        }
    }

    /// Create result state table for agg executor.
    pub async fn create_result_table<S: StateStore>(
        store: S,
        table_id: TableId,
        agg_calls: &[AggCall],
        group_key_indices: &[usize],
        input_ref: &dyn Executor,
    ) -> StateTable<S> {
        let input_fields = input_ref.schema().fields();

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

        StateTable::new_without_distribution(
            store,
            table_id,
            column_descs,
            order_types,
            (0..group_key_indices.len()).collect(),
        )
        .await
    }

    pub async fn new_boxed_simple_agg_executor<S: StateStore>(
        actor_ctx: ActorContextRef,
        store: S,
        input: BoxedExecutor,
        agg_calls: Vec<AggCall>,
        row_count_index: usize,
        pk_indices: PkIndices,
        executor_id: u64,
    ) -> Box<dyn Executor> {
        let mut storages = Vec::with_capacity(agg_calls.iter().len());
        for (idx, agg_call) in agg_calls.iter().enumerate() {
            storages.push(
                create_agg_state_storage(
                    store.clone(),
                    TableId::new(idx as u32),
                    agg_call,
                    &[],
                    &pk_indices,
                    input.as_ref(),
                )
                .await,
            )
        }

        let result_table = create_result_table(
            store,
            TableId::new(agg_calls.len() as u32),
            &agg_calls,
            &[],
            input.as_ref(),
        )
        .await;

        GlobalSimpleAggExecutor::new(AggExecutorArgs {
            input,
            actor_ctx,
            pk_indices,
            executor_id,

            extreme_cache_size: 1024,

            agg_calls,
            row_count_index,
            storages,
            result_table,
            distinct_dedup_tables: Default::default(),
            watermark_epoch: Arc::new(AtomicU64::new(0)),

            extra: SimpleAggExecutorExtraArgs {},
        })
        .unwrap()
        .boxed()
    }
}

pub mod top_n_executor {
    use itertools::Itertools;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use crate::common::table::state_table::StateTable;

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
        StateTable::new_without_distribution(
            state_store,
            TableId::new(0),
            column_descs,
            order_types.to_vec(),
            pk_indices.to_vec(),
        )
        .await
    }
}

/// Generate `num_of_chunks` data chunks with type `data_types`, each data chunk has cardinality of
/// `batch_size`.
/// TODO(kwannoel): Refactor this and `batch` gen_data into data_chunk::test_utils
pub fn gen_data(num_of_chunks: usize, chunk_size: usize, data_types: &[DataType]) -> Vec<DataChunk> {
    let mut ret = Vec::<DataChunk>::with_capacity(num_of_chunks);

    for i in 0..num_of_chunks {
        let mut columns = Vec::new();
        for data_type in data_types {
            let mut data_gen =
                FieldGeneratorImpl::with_number_random(data_type.clone(), None, None, SEED)
                    .unwrap();
            let mut array_builder = data_type.create_array_builder(chunk_size);
            for j in 0..chunk_size {
                array_builder.append_datum(&data_gen.generate_datum(((i + 1) * (j + 1)) as u64));
            }
            columns.push(array_builder.finish().into());
        }
        ret.push(DataChunk::new(columns, chunk_size));
    }
    ret
}

