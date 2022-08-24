// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_storage::memory::MemoryStateStore;
use tokio::sync::mpsc;

use super::error::StreamExecutorError;
use super::{Barrier, Executor, Message, PkIndices, StreamChunk};

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
        let mut epoch = 0;

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

    fn pk_indices(&self) -> super::PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        "MockSource"
    }
}

/// `row_nonnull` builds a `Row` with concrete values.
/// TODO: add macro row!, which requires a new trait `ToScalarValue`.
#[macro_export]
macro_rules! row_nonnull {
    [$( $value:expr ),*] => {
        {
            use risingwave_common::types::Scalar;
            use risingwave_common::array::Row;
            Row(vec![$(Some($value.to_scalar_value()), )*])
        }
    };
}

/// Create a vector of memory keyspace with len `num_ks`.
pub fn create_in_memory_keyspace_agg(num_ks: usize) -> Vec<(MemoryStateStore, TableId)> {
    let mut returned_vec = vec![];
    let mem_state = MemoryStateStore::new();
    for idx in 0..num_ks {
        returned_vec.push((mem_state.clone(), TableId::new(idx as u32)));
    }
    returned_vec
}

pub mod agg_executor {
    use itertools::Itertools;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::expr::AggKind;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::streaming_table::state_table::StateTable;
    use risingwave_storage::StateStore;

    use crate::executor::aggregation::AggCall;
    use crate::executor::{
        ActorContextRef, BoxedExecutor, Executor, GlobalSimpleAggExecutor, PkIndices,
    };

    /// Create state table for the given agg call.
    /// Should infer the schema in the same way as `LogicalAgg::infer_internal_table_catalog`.
    pub fn create_state_table<S: StateStore>(
        store: S,
        table_id: TableId,
        agg_call: &AggCall,
        group_key: &[usize],
        pk_indices: &[usize],
        input_ref: &dyn Executor,
    ) -> (StateTable<S>, Vec<usize>) {
        let input_fields = input_ref.schema().fields();

        let mut column_descs = Vec::new();
        let mut order_types = Vec::new();
        let mut column_mapping = Vec::new();

        let mut next_column_id = 0;
        let mut add_column_desc = |data_type: DataType| {
            column_descs.push(ColumnDesc::unnamed(
                ColumnId::new(next_column_id),
                data_type,
            ));
            next_column_id += 1;
        };

        for idx in group_key {
            add_column_desc(input_fields[*idx].data_type());
            column_mapping.push(*idx);
            order_types.push(OrderType::Ascending);
        }

        match agg_call.kind {
            AggKind::Min | AggKind::Max if !agg_call.append_only => {
                add_column_desc(agg_call.args.arg_types()[0].clone());
                column_mapping.push(agg_call.args.val_indices()[0]);
                order_types.push(if agg_call.kind == AggKind::Max {
                    OrderType::Descending
                } else {
                    OrderType::Ascending
                });
                for idx in pk_indices {
                    add_column_desc(input_fields[*idx].data_type());
                    column_mapping.push(*idx);
                    order_types.push(OrderType::Ascending);
                }
            }
            AggKind::Min /* append only */
            | AggKind::Max /* append only */
            | AggKind::Sum
            | AggKind::Count
            | AggKind::Avg
            | AggKind::SingleValue
            | AggKind::ApproxCountDistinct => {
                add_column_desc(agg_call.return_type.clone());
            }
            _ => {
                panic!("no need to mock other agg kinds here");
            }
        }

        let state_table = StateTable::new_without_distribution(
            store,
            table_id,
            column_descs,
            order_types.clone(),
            (0..order_types.len()).collect(),
        );

        (state_table, column_mapping)
    }

    pub fn new_boxed_simple_agg_executor(
        ctx: ActorContextRef,
        keyspace_gen: Vec<(MemoryStateStore, TableId)>,
        input: BoxedExecutor,
        agg_calls: Vec<AggCall>,
        pk_indices: PkIndices,
        executor_id: u64,
        key_indices: Vec<usize>,
    ) -> Box<dyn Executor> {
        let (state_tables, state_table_col_mappings) = keyspace_gen
            .iter()
            .zip_eq(agg_calls.iter())
            .map(|(ks, agg_call)| {
                create_state_table(
                    ks.0.clone(),
                    ks.1,
                    agg_call,
                    &key_indices,
                    &pk_indices,
                    input.as_ref(),
                )
            })
            .unzip();

        Box::new(
            GlobalSimpleAggExecutor::new(
                ctx,
                input,
                agg_calls,
                pk_indices,
                executor_id,
                state_tables,
                state_table_col_mappings,
            )
            .unwrap(),
        )
    }
}

pub mod top_n_executor {
    use itertools::Itertools;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::streaming_table::state_table::StateTable;

    pub fn create_in_memory_state_table(
        data_types: &[DataType],
        order_types: &[OrderType],
        pk_indices: &[usize],
    ) -> StateTable<MemoryStateStore> {
        let column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
            .collect_vec();
        StateTable::new_without_distribution(
            MemoryStateStore::new(),
            TableId::new(0),
            column_descs,
            order_types.to_vec(),
            pk_indices.to_vec(),
        )
    }
}
