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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use either::Either;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::{ArrayBuilder, I64ArrayBuilder, Op, StreamChunk};
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnId, Schema, TableId};
use risingwave_common::util::epoch::UNIX_SINGULARITY_DATE_EPOCH;
use risingwave_connector::source::{ConnectorState, SplitId, SplitImpl, SplitMetaData};
use risingwave_source::connector_source::SourceContext;
use risingwave_source::row_id::RowIdGenerator;
use risingwave_source::*;
use risingwave_storage::{Keyspace, StateStore};
use tokio::sync::mpsc::UnboundedReceiver;

use super::reader::SourceReaderStream;
use crate::error::StreamResult;
use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::source::state::SourceStateHandler;
use crate::executor::*;

/// [`SourceExecutor`] is a streaming source, from risingwave's batch table, or external systems
/// such as Kafka.
pub struct SourceExecutor<S: StateStore> {
    ctx: ActorContextRef,

    source_id: TableId,
    source_desc: SourceDesc,

    /// Row id generator for this source executor.
    row_id_generator: RowIdGenerator,

    column_ids: Vec<ColumnId>,
    schema: Schema,
    pk_indices: PkIndices,

    /// Identity string
    identity: String,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    /// Metrics for monitor.
    metrics: Arc<StreamingMetrics>,

    /// Split info for stream source
    stream_source_splits: Vec<SplitImpl>,

    source_identify: String,

    split_state_store: SourceStateHandler<S>,

    state_cache: HashMap<SplitId, SplitImpl>,

    #[expect(dead_code)]
    /// Expected barrier latency
    expected_barrier_latency_ms: u64,
}

impl<S: StateStore> SourceExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        source_id: TableId,
        source_desc: SourceDesc,
        vnodes: Bitmap,
        keyspace: Keyspace<S>,
        column_ids: Vec<ColumnId>,
        schema: Schema,
        pk_indices: PkIndices,
        barrier_receiver: UnboundedReceiver<Barrier>,
        executor_id: u64,
        _operator_id: u64,
        _op_info: String,
        streaming_metrics: Arc<StreamingMetrics>,
        expected_barrier_latency_ms: u64,
    ) -> StreamResult<Self> {
        // Using vnode range start for row id generator.
        let vnode_id = vnodes.next_set_bit(0).unwrap_or(0);
        Ok(Self {
            ctx,
            source_id,
            source_desc,
            row_id_generator: RowIdGenerator::with_epoch(
                vnode_id as u32,
                *UNIX_SINGULARITY_DATE_EPOCH,
            ),
            column_ids,
            schema,
            pk_indices,
            barrier_receiver: Some(barrier_receiver),
            identity: format!("SourceExecutor {:X}", executor_id),
            metrics: streaming_metrics,
            stream_source_splits: vec![],
            source_identify: "Table_".to_string() + &source_id.table_id().to_string(),
            split_state_store: SourceStateHandler::new(keyspace),
            state_cache: HashMap::new(),
            expected_barrier_latency_ms,
        })
    }

    /// Generate a row ID column.
    async fn gen_row_id_column(&mut self, len: usize) -> Column {
        let mut builder = I64ArrayBuilder::new(len);
        let row_ids = self.row_id_generator.next_batch(len).await;

        for row_id in row_ids {
            builder.append(Some(row_id));
        }

        builder.finish().into()
    }

    /// Generate a row ID column according to ops.
    async fn gen_row_id_column_by_op(&mut self, column: &Column, ops: Ops<'_>) -> Column {
        let len = column.array_ref().len();
        let mut builder = I64ArrayBuilder::new(len);

        for i in 0..len {
            // Only refill row_id for insert operation.
            if ops.get(i) == Some(&Op::Insert) {
                builder.append(Some(self.row_id_generator.next().await));
            } else {
                builder.append(Some(
                    i64::try_from(column.array_ref().datum_at(i).unwrap()).unwrap(),
                ));
            }
        }

        Column::new(Arc::new(ArrayImpl::from(builder.finish())))
    }

    async fn refill_row_id_column(&mut self, chunk: StreamChunk, append_only: bool) -> StreamChunk {
        let row_id_index = self.source_desc.row_id_index;

        // if row_id_index is None, pk is not row_id, so no need to gen row_id and refill chunk
        if let Some(row_id_index) = row_id_index {
            let row_id_column_id = self.source_desc.columns[row_id_index as usize].column_id;
            if let Some(idx) = self
                .column_ids
                .iter()
                .position(|column_id| *column_id == row_id_column_id)
            {
                let (ops, mut columns, bitmap) = chunk.into_inner();
                if append_only {
                    columns[idx] = self.gen_row_id_column(columns[idx].array().len()).await;
                } else {
                    columns[idx] = self.gen_row_id_column_by_op(&columns[idx], &ops).await;
                }
                return StreamChunk::new(ops, columns, bitmap);
            }
        }
        chunk
    }
}

impl<S: StateStore> SourceExecutor<S> {
    fn get_diff(&mut self, rhs: ConnectorState) -> ConnectorState {
        // rhs can not be None because we do not support split number reduction

        let split_change = rhs.unwrap();
        let mut target_state: Vec<SplitImpl> = Vec::with_capacity(split_change.len());
        let mut no_change_flag = true;
        for sc in &split_change {
            // SplitImpl is identified by its id, target_state always follows offsets in cache
            // here we introduce a hypothesis that every split is polled at least once in one epoch
            match self.state_cache.get(&sc.id()) {
                Some(s) => target_state.push(s.clone()),
                None => {
                    no_change_flag = false;
                    // write new assigned split to state cache. snapshot is base on cache.
                    self.state_cache
                        .entry(sc.id())
                        .or_insert_with(|| sc.clone());
                    target_state.push(sc.clone())
                }
            }
        }

        (!no_change_flag).then_some(target_state)
    }

    async fn take_snapshot(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        let cache = self
            .state_cache
            .iter()
            .map(|(_, split_impl)| split_impl.to_owned())
            .collect_vec();

        if !cache.is_empty() {
            self.split_state_store.take_snapshot(cache, epoch).await?
        }

        Ok(())
    }

    async fn build_stream_source_reader(
        &mut self,
        state: ConnectorState,
    ) -> StreamExecutorResult<Box<SourceStreamReaderImpl>> {
        let reader = match self.source_desc.source.as_ref() {
            SourceImpl::Table(t) => t
                .stream_reader(self.column_ids.clone())
                .await
                .map(SourceStreamReaderImpl::Table),
            SourceImpl::Connector(c) => c
                .stream_reader(
                    state,
                    self.column_ids.clone(),
                    self.source_desc.metrics.clone(),
                    SourceContext::new(self.ctx.id as u32, self.source_id),
                )
                .await
                .map(SourceStreamReaderImpl::Connector),
        }
        .map_err(StreamExecutorError::connector_error)?;

        Ok(Box::new(reader))
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let barrier = barrier_receiver
            .recv()
            .stack_trace("source_recv_first_barrier")
            .await
            .unwrap();

        // If the first barrier is configuration change, then the source executor must be newly
        // created, and we should start with the paused state.
        let start_with_paused = barrier.is_update();

        if let Some(mutation) = barrier.mutation.as_ref() {
            if let Mutation::Add { splits, .. } = mutation.as_ref() {
                if let Some(splits) = splits.get(&self.ctx.id) {
                    self.stream_source_splits = splits.clone();
                }
            }
        }

        let epoch = barrier.epoch.prev;

        let mut boot_state = self.stream_source_splits.clone();
        for ele in &mut boot_state {
            if let Some(recover_state) = self
                .split_state_store
                .try_recover_from_state_store(ele, epoch)
                .await?
            {
                *ele = recover_state;
            }
        }

        let recover_state: ConnectorState = (!boot_state.is_empty()).then_some(boot_state);
        tracing::debug!(
            "source {:?} actor {:?} starts with state {:?}",
            self.source_id,
            self.ctx.id,
            recover_state
        );

        // todo: use epoch from msg to restore state from state store
        let source_chunk_reader = self
            .build_stream_source_reader(recover_state)
            .stack_trace("source_build_reader")
            .await?;

        // Merge the chunks from source and the barriers into a single stream.
        let mut stream = SourceReaderStream::new(barrier_receiver, source_chunk_reader);
        if start_with_paused {
            stream.pause_source();
        }

        yield Message::Barrier(barrier);

        while let Some(msg) = stream.next().await {
            match msg {
                // This branch will be preferred.
                Either::Left(barrier) => {
                    let barrier = barrier?;
                    let epoch = barrier.epoch.prev;

                    if let Some(mutation) = barrier.mutation.as_deref() {
                        match mutation {
                            Mutation::SourceChangeSplit(mapping) => {
                                if let Some(target_splits) = mapping.get(&self.ctx.id).cloned() {
                                    if let Some(target_state) = self.get_diff(target_splits) {
                                        tracing::info!(
                                            "actor {:?} apply source split change to {:?}",
                                            self.ctx.id,
                                            target_state
                                        );

                                        // Replace the source reader with a new one of the new
                                        // state.
                                        let reader = self
                                            .build_stream_source_reader(Some(target_state.clone()))
                                            .await?;
                                        stream.replace_source_chunk_reader(reader);

                                        self.stream_source_splits = target_state;
                                    }
                                }
                            }
                            Mutation::Pause => stream.pause_source(),
                            Mutation::Resume => stream.resume_source(),
                            Mutation::Update { vnode_bitmaps, .. } => {
                                // Update row id generator if vnode mapping is changed.
                                // Note that: since update barrier will only occurs between pause
                                // and resume barrier, duplicated row id won't be generated.
                                if let Some(vnode_bitmaps) = vnode_bitmaps.get(&self.ctx.id) {
                                    let vnode_id =
                                        vnode_bitmaps.next_set_bit(0).unwrap_or(0) as u32;
                                    if self.row_id_generator.vnode_id != vnode_id {
                                        self.row_id_generator = RowIdGenerator::with_epoch(
                                            vnode_id,
                                            *UNIX_SINGULARITY_DATE_EPOCH,
                                        );
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    self.take_snapshot(epoch).await?;
                    self.state_cache.clear();
                    yield Message::Barrier(barrier);
                }

                Either::Right(chunk_with_state) => {
                    let StreamChunkWithState {
                        mut chunk,
                        split_offset_mapping,
                    } = chunk_with_state?;

                    if let Some(mapping) = split_offset_mapping {
                        let state: HashMap<_, _> = mapping
                            .iter()
                            .map(|(split, offset)| {
                                let origin_split_impl = self
                                    .stream_source_splits
                                    .iter()
                                    .filter(|origin_split| &origin_split.id() == split)
                                    .collect_vec();

                                if origin_split_impl.is_empty() {
                                    bail!(
                                        "cannot find split: {:?} in stream_source_splits: {:?}",
                                        split,
                                        self.stream_source_splits
                                    )
                                } else {
                                    Ok::<_, StreamExecutorError>((
                                        split.clone(),
                                        origin_split_impl[0].update(offset.clone()),
                                    ))
                                }
                            })
                            .try_collect()?;
                        self.state_cache.extend(state);
                    }

                    // Refill row id column for source.
                    chunk = match self.source_desc.source.as_ref() {
                        SourceImpl::Connector(_) => self.refill_row_id_column(chunk, true).await,
                        SourceImpl::Table(_) => self.refill_row_id_column(chunk, false).await,
                    };

                    self.metrics
                        .source_output_row_count
                        .with_label_values(&[self.source_identify.as_str()])
                        .inc_by(chunk.cardinality() as u64);
                    yield Message::Chunk(chunk);
                }
            }
        }
    }
}

impl<S: StateStore> Executor for SourceExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }
}

impl<S: StateStore> Debug for SourceExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceExecutor")
            .field("source_id", &self.source_id)
            .field("column_ids", &self.column_ids)
            .field("pk_indices", &self.pk_indices)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt;
    use maplit::hashmap;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_connector::source::datagen::DatagenSplit;
    use risingwave_pb::catalog::{ColumnIndex as ProstColumnIndex, StreamSourceInfo};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType as ProstDataType;
    use risingwave_pb::plan_common::{
        ColumnCatalog as ProstColumnCatalog, ColumnDesc as ProstColumnDesc,
        RowFormatType as ProstRowFormatType,
    };
    use risingwave_source::*;
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    #[tokio::test]
    async fn test_table_source() {
        let table_id = TableId::default();

        let rowid_type = DataType::Int64;
        let col1_type = DataType::Int32;
        let col2_type = DataType::Varchar;

        let table_columns = vec![
            ColumnDesc {
                column_id: ColumnId::from(0),
                data_type: rowid_type.clone(),
                name: String::new(),
                field_descs: vec![],
                type_name: "".to_string(),
            },
            ColumnDesc {
                column_id: ColumnId::from(1),
                data_type: col1_type.clone(),
                name: String::new(),
                field_descs: vec![],
                type_name: "".to_string(),
            },
            ColumnDesc {
                column_id: ColumnId::from(2),
                data_type: col2_type.clone(),
                name: String::new(),
                field_descs: vec![],
                type_name: "".to_string(),
            },
        ];
        let row_id_index = Some(0);
        let pk_column_ids = vec![0];
        let source_manager = MemSourceManager::default();
        source_manager
            .create_table_source(&table_id, table_columns, row_id_index, pk_column_ids)
            .unwrap();
        let source_desc = source_manager.get_source(&table_id).unwrap();
        let source = source_desc.clone().source;

        let chunk1 = StreamChunk::from_pretty(
            " I i T
            U+ 1 1 foo
            U+ 2 2 bar
            U+ 3 3 baz",
        );
        let chunk2 = StreamChunk::from_pretty(
            " I i T
            U+ 4 4 hello
            U+ 5 5 .
            U+ 6 6 world",
        );

        let schema = Schema {
            fields: vec![
                Field::unnamed(rowid_type),
                Field::unnamed(col1_type),
                Field::unnamed(col2_type),
            ],
        };

        let column_ids = vec![0, 1, 2].into_iter().map(ColumnId::from).collect();
        let pk_indices = vec![0];

        let (barrier_sender, barrier_receiver) = unbounded_channel();
        let keyspace = Keyspace::table_root(MemoryStateStore::new(), &TableId::from(0x2333));
        let vnodes = Bitmap::from_bytes(Bytes::from_static(&[0b11111111]));

        let executor = SourceExecutor::new(
            ActorContext::create(0x3f3f3f),
            table_id,
            source_desc,
            vnodes,
            keyspace,
            column_ids,
            schema,
            pk_indices,
            barrier_receiver,
            1,
            1,
            "SourceExecutor".to_string(),
            Arc::new(StreamingMetrics::new(prometheus::Registry::new())),
            u64::MAX,
        )
        .unwrap();
        let mut executor = Box::new(executor).execute();

        let write_chunk = |chunk: StreamChunk| {
            let table_source = source.as_table().unwrap();
            table_source.write_chunk(chunk).unwrap();
        };

        barrier_sender.send(Barrier::new_test_barrier(1)).unwrap();

        let msg = executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_barrier().unwrap().epoch,
            EpochPair::new_test_epoch(1)
        );

        // Write 1st chunk
        write_chunk(chunk1);

        let msg = executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I i T
                U+ 1 1 foo
                U+ 2 2 bar
                U+ 3 3 baz",
            )
        );

        // Write 2nd chunk
        write_chunk(chunk2);

        let msg = executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I i T
                U+ 4 4 hello
                U+ 5 5 .
                U+ 6 6 world",
            )
        );
    }

    #[tokio::test]
    async fn test_table_dropped() {
        let table_id = TableId::default();

        let rowid_type = DataType::Int64;
        let col1_type = DataType::Int32;
        let col2_type = DataType::Varchar;

        let table_columns = vec![
            ColumnDesc {
                column_id: ColumnId::from(0),
                data_type: rowid_type.clone(),
                name: String::new(),
                field_descs: vec![],
                type_name: "".to_string(),
            },
            ColumnDesc {
                column_id: ColumnId::from(1),
                data_type: col1_type.clone(),
                name: String::new(),
                field_descs: vec![],
                type_name: "".to_string(),
            },
            ColumnDesc {
                column_id: ColumnId::from(2),
                data_type: col2_type.clone(),
                name: String::new(),
                field_descs: vec![],
                type_name: "".to_string(),
            },
        ];
        let row_id_index = Some(0);
        let pk_column_ids = vec![0];
        let source_manager = MemSourceManager::default();
        source_manager
            .create_table_source(&table_id, table_columns, row_id_index, pk_column_ids)
            .unwrap();
        let source_desc = source_manager.get_source(&table_id).unwrap();
        let source = source_desc.clone().source;

        // Prepare test data chunks
        let chunk = StreamChunk::from_pretty(
            " I i T
            + 0 1 foo
            + 0 2 bar
            + 0 3 baz",
        );

        let schema = Schema {
            fields: vec![
                Field::unnamed(rowid_type),
                Field::unnamed(col1_type),
                Field::unnamed(col2_type),
            ],
        };

        let column_ids = vec![0.into(), 1.into(), 2.into()];
        let pk_indices = vec![0];

        let (barrier_sender, barrier_receiver) = unbounded_channel();
        let keyspace = Keyspace::table_root(MemoryStateStore::new(), &TableId::from(0x2333));
        let vnodes = Bitmap::from_bytes(Bytes::from_static(&[0b11111111]));
        let executor = SourceExecutor::new(
            ActorContext::create(0x3f3f3f),
            table_id,
            source_desc,
            vnodes,
            keyspace,
            column_ids,
            schema,
            pk_indices,
            barrier_receiver,
            1,
            1,
            "SourceExecutor".to_string(),
            Arc::new(StreamingMetrics::unused()),
            u64::MAX,
        )
        .unwrap();
        let mut executor = Box::new(executor).execute();

        let write_chunk = |chunk: StreamChunk| {
            let table_source = source.as_table().unwrap();
            table_source.write_chunk(chunk).unwrap();
        };

        barrier_sender
            .send(Barrier::new_test_barrier(1).with_stop())
            .unwrap();
        executor.next().await.unwrap().unwrap();

        write_chunk(chunk.clone());
        executor.next().await.unwrap().unwrap();
        write_chunk(chunk);
    }

    fn mock_stream_source_info() -> StreamSourceInfo {
        let properties: HashMap<String, String> = hashmap! {
            "connector".to_string() => "datagen".to_string(),
            "fields.v1.min".to_string() => "1".to_string(),
            "fields.v1.max".to_string() => "1000".to_string(),
            "fields.v1.seed".to_string() => "12345".to_string(),
        };

        let columns = vec![
            ProstColumnCatalog {
                column_desc: Some(ProstColumnDesc {
                    column_type: Some(ProstDataType {
                        type_name: TypeName::Int64 as i32,
                        ..Default::default()
                    }),
                    column_id: 0,
                    ..Default::default()
                }),
                is_hidden: false,
            },
            ProstColumnCatalog {
                column_desc: Some(ProstColumnDesc {
                    column_type: Some(ProstDataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    }),
                    column_id: 1,
                    name: "v1".to_string(),
                    ..Default::default()
                }),
                is_hidden: false,
            },
        ];

        StreamSourceInfo {
            properties,
            row_format: ProstRowFormatType::Json as i32,
            row_schema_location: "".to_string(),
            row_id_index: Some(ProstColumnIndex { index: 0 }),
            columns,
            pk_column_ids: vec![0],
        }
    }

    trait StreamChunkExt {
        fn drop_row_id(self) -> Self;
    }
    impl StreamChunkExt for StreamChunk {
        fn drop_row_id(self) -> StreamChunk {
            let (ops, mut columns, bitmap) = self.into_inner();
            columns.remove(0);
            StreamChunk::new(ops, columns, bitmap)
        }
    }

    #[tokio::test]
    async fn test_split_change_mutation() {
        let stream_source_info = mock_stream_source_info();
        let source_table_id = TableId::default();
        let source_manager = Arc::new(MemSourceManager::default());

        source_manager
            .create_source(&source_table_id, stream_source_info)
            .await
            .unwrap();

        let get_schema = |column_ids: &[ColumnId], source_desc: &SourceDesc| {
            let mut fields = Vec::with_capacity(column_ids.len());
            for &column_id in column_ids {
                let column_desc = source_desc
                    .columns
                    .iter()
                    .find(|c| c.column_id == column_id)
                    .unwrap();
                fields.push(Field::unnamed(column_desc.data_type.clone()));
            }
            Schema::new(fields)
        };

        let source_desc = source_manager.get_source(&source_table_id).unwrap();
        let mem_state_store = MemoryStateStore::new();
        let keyspace = Keyspace::table_root(mem_state_store.clone(), &TableId::from(0x2333));
        let column_ids = vec![ColumnId::from(0), ColumnId::from(1)];
        let schema = get_schema(&column_ids, &source_desc);
        let pk_indices = vec![0_usize];
        let (barrier_tx, barrier_rx) = unbounded_channel::<Barrier>();
        let vnodes = Bitmap::from_bytes(Bytes::from_static(&[0b11111111]));
        let source_state_handler = SourceStateHandler::new(keyspace.clone());

        let source_exec = SourceExecutor::new(
            ActorContext::create(0),
            source_table_id,
            source_desc,
            vnodes,
            keyspace.clone(),
            column_ids.clone(),
            schema,
            pk_indices,
            barrier_rx,
            1,
            1,
            "SourceExecutor".to_string(),
            Arc::new(StreamingMetrics::unused()),
            u64::MAX,
        )
        .unwrap();

        let mut materialize = MaterializeExecutor::for_test(
            Box::new(source_exec),
            mem_state_store.clone(),
            TableId::from(0x2333),
            vec![OrderPair::new(0, OrderType::Ascending)],
            column_ids.clone(),
            2,
        )
        .boxed()
        .execute();

        let curr_epoch = 1919;
        let init_barrier = Barrier::new_test_barrier(curr_epoch).with_mutation(Mutation::Add {
            adds: HashMap::new(),
            splits: hashmap! {
                ActorId::default() => vec![
                    SplitImpl::Datagen(
                    DatagenSplit {
                        split_index: 0,
                        split_num: 3,
                        start_offset: None,
                    }),
                ],
            },
        });
        barrier_tx.send(init_barrier).unwrap();

        let _ = materialize.next().await.unwrap(); // barrier

        let chunk_1 = (materialize.next().await.unwrap().unwrap())
            .into_chunk()
            .unwrap()
            .drop_row_id();

        let chunk_1_truth = StreamChunk::from_pretty(
            " I i
            + 0 533
            + 0 833
            + 0 738
            + 0 344",
        )
        .drop_row_id();

        assert_eq!(chunk_1, chunk_1_truth);

        let change_split_mutation = Barrier::new_test_barrier(curr_epoch + 1).with_mutation(
            Mutation::SourceChangeSplit(hashmap! {
                ActorId::default() => Some(vec![
                    SplitImpl::Datagen(
                        DatagenSplit {
                            split_index: 0,
                            split_num: 3,
                            start_offset: None,
                        }
                    ), SplitImpl::Datagen(
                        DatagenSplit {
                            split_index: 1,
                            split_num: 3,
                            start_offset: None,
                        }
                    ),
                ])
            }),
        );
        barrier_tx.send(change_split_mutation).unwrap();

        let _ = materialize.next().await.unwrap(); // barrier

        // there must exist state for new add partition
        source_state_handler
            .restore_states("3-1".to_string().into(), curr_epoch + 1)
            .await
            .unwrap()
            .unwrap();

        let chunk_2 = (materialize.next().await.unwrap().unwrap())
            .into_chunk()
            .unwrap()
            .drop_row_id();
        let chunk_3 = (materialize.next().await.unwrap().unwrap())
            .into_chunk()
            .unwrap()
            .drop_row_id();

        let chunk_2_truth = StreamChunk::from_pretty(
            " I i
            + 0 525
            + 0 425
            + 0 29
            + 0 201",
        )
        .drop_row_id();
        let chunk_3_truth = StreamChunk::from_pretty(
            " I i
            + 0 833
            + 0 533
            + 0 344",
        )
        .drop_row_id();
        assert!(
            chunk_2 == chunk_2_truth && chunk_3 == chunk_3_truth
                || chunk_3 == chunk_2_truth && chunk_2 == chunk_3_truth
        );

        let pause_barrier =
            Barrier::new_test_barrier(curr_epoch + 2).with_mutation(Mutation::Pause);
        barrier_tx.send(pause_barrier).unwrap();

        let pause_barrier =
            Barrier::new_test_barrier(curr_epoch + 3).with_mutation(Mutation::Resume);
        barrier_tx.send(pause_barrier).unwrap();
    }
}
