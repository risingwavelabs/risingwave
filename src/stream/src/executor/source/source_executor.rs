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
use risingwave_common::catalog::{ColumnId, Schema, TableId};
use risingwave_common::util::epoch::UNIX_SINGULARITY_DATE_EPOCH;
use risingwave_connector::source::{ConnectorState, SplitId, SplitImpl, SplitMetaData};
use risingwave_source::connector_source::SourceContext;
use risingwave_source::row_id::RowIdGenerator;
use risingwave_source::*;
use risingwave_storage::StateStore;
use tokio::sync::mpsc::UnboundedReceiver;

use super::reader::SourceReaderStream;
use crate::error::StreamResult;
use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::source::state_table_handler::SourceStateTableHandler;
use crate::executor::*;

/// [`SourceExecutor`] is a streaming source, from risingwave's batch table, or external systems
/// such as Kafka.
pub struct SourceExecutor<S: StateStore> {
    ctx: ActorContextRef,

    source_id: TableId,
    source_builder: SourceDescBuilder,

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

    split_state_store: SourceStateTableHandler<S>,

    state_cache: HashMap<SplitId, SplitImpl>,

    #[expect(dead_code)]
    /// Expected barrier latency
    expected_barrier_latency_ms: u64,
}

impl<S: StateStore> SourceExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        source_builder: SourceDescBuilder,
        source_id: TableId,
        vnodes: Bitmap,
        state_table: SourceStateTableHandler<S>,
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
            source_builder,
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
            split_state_store: state_table,
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

        builder.finish().into()
    }

    async fn refill_row_id_column(
        &mut self,
        chunk: StreamChunk,
        append_only: bool,
        row_id_index: Option<usize>,
    ) -> StreamChunk {
        if let Some(idx) = row_id_index {
            let (ops, mut columns, bitmap) = chunk.into_inner();
            if append_only {
                columns[idx] = self.gen_row_id_column(columns[idx].array().len()).await;
            } else {
                columns[idx] = self.gen_row_id_column_by_op(&columns[idx], &ops).await;
            }
            StreamChunk::new(ops, columns, bitmap)
        } else {
            chunk
        }
    }
}

impl<S: StateStore> SourceExecutor<S> {
    // Note: get_diff will modify the state_cache
    async fn get_diff(&mut self, rhs: ConnectorState) -> StreamExecutorResult<ConnectorState> {
        // rhs can not be None because we do not support split number reduction

        let split_change = rhs.unwrap();
        let mut target_state: Vec<SplitImpl> = Vec::with_capacity(split_change.len());
        let mut no_change_flag = true;
        for sc in &split_change {
            if let Some(s) = self.state_cache.get(&sc.id()) {
                target_state.push(s.clone())
            } else {
                no_change_flag = false;
                // write new assigned split to state cache. snapshot is base on cache.

                let state = if let Some(recover_state) = self
                    .split_state_store
                    .try_recover_from_state_store(sc)
                    .await?
                {
                    recover_state
                } else {
                    sc.clone()
                };

                self.state_cache
                    .entry(sc.id())
                    .or_insert_with(|| state.clone());
                target_state.push(state);
            }
        }

        Ok((!no_change_flag).then_some(target_state))
    }

    async fn take_snapshot(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        let cache = self
            .state_cache
            .iter()
            .map(|(_, split_impl)| split_impl.to_owned())
            .collect_vec();

        if !cache.is_empty() {
            self.split_state_store.take_snapshot(cache).await?
        }
        // commit anyway, even if no message saved
        self.split_state_store.state_store.commit(epoch).await?;

        Ok(())
    }

    async fn build_stream_source_reader(
        &mut self,
        source_desc: &SourceDescRef,
        state: ConnectorState,
    ) -> StreamExecutorResult<BoxSourceWithStateStream> {
        let reader = match &source_desc.source {
            SourceImpl::Table(t) => t
                .stream_reader(self.column_ids.clone())
                .await
                .map_err(StreamExecutorError::connector_error)?
                .into_stream(),
            SourceImpl::Connector(c) => c
                .stream_reader(
                    state,
                    self.column_ids.clone(),
                    source_desc.metrics.clone(),
                    SourceContext::new(self.ctx.id as u32, self.source_id),
                )
                .await
                .map_err(StreamExecutorError::connector_error)?
                .into_stream(),
        };
        Ok(reader)
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let barrier = barrier_receiver
            .recv()
            .stack_trace("source_recv_first_barrier")
            .await
            .unwrap();

        let source_desc = self
            .source_builder
            .build()
            .await
            .context("build source desc failed")?;
        // source_desc's row_id_index is based on its columns, and it is possible
        // that we prune some columns when generating column_ids. So this index
        // can not be directly used.
        let row_id_index = source_desc
            .row_id_index
            .map(|idx| source_desc.columns[idx].column_id)
            .and_then(|ref cid| self.column_ids.iter().position(|id| id.eq(cid)));

        // If the first barrier is configuration change, then the source executor must be newly
        // created, and we should start with the paused state.
        let start_with_paused = barrier.is_update();

        if let Some(mutation) = barrier.mutation.as_ref() {
            match mutation.as_ref() {
                Mutation::Add { splits, .. } => {
                    if let Some(splits) = splits.get(&self.ctx.id) {
                        self.stream_source_splits = splits.clone();
                    }
                }
                Mutation::Update { actor_splits, .. } => {
                    if let Some(splits) = actor_splits.get(&self.ctx.id) {
                        self.stream_source_splits = splits.clone();
                    }
                }
                _ => {}
            }
        }

        self.split_state_store.init_epoch(barrier.epoch);

        let mut boot_state = self.stream_source_splits.clone();
        for ele in &mut boot_state {
            if let Some(recover_state) = self
                .split_state_store
                .try_recover_from_state_store(ele)
                .await?
            {
                *ele = recover_state;
            }
        }

        let recover_state: ConnectorState = (!boot_state.is_empty()).then_some(boot_state);

        // todo: use epoch from msg to restore state from state store
        let source_chunk_reader = self
            .build_stream_source_reader(&source_desc, recover_state)
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
                    let epoch = barrier.epoch;

                    if let Some(mutation) = barrier.mutation.as_deref() {
                        match mutation {
                            Mutation::SourceChangeSplit(actor_splits) => {
                                self.apply_split_change(&source_desc, &mut stream, actor_splits)
                                    .await?
                            }
                            Mutation::Pause => stream.pause_source(),
                            Mutation::Resume => stream.resume_source(),
                            Mutation::Update {
                                vnode_bitmaps,
                                actor_splits,
                                ..
                            } => {
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

                                self.apply_split_change(&source_desc, &mut stream, actor_splits)
                                    .await?;
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
                            .flat_map(|(split, offset)| {
                                let origin_split_impl = self
                                    .stream_source_splits
                                    .iter()
                                    .filter(|origin_split| &origin_split.id() == split)
                                    .exactly_one()
                                    .ok();

                                origin_split_impl.map(|split_impl| {
                                    (split.clone(), split_impl.update(offset.clone()))
                                })
                            })
                            .collect();

                        self.state_cache.extend(state);
                    }

                    // Refill row id column for source.
                    chunk = match &source_desc.source {
                        SourceImpl::Connector(_) => {
                            self.refill_row_id_column(chunk, true, row_id_index).await
                        }
                        SourceImpl::Table(_) => {
                            self.refill_row_id_column(chunk, false, row_id_index).await
                        }
                    };

                    self.metrics
                        .source_output_row_count
                        .with_label_values(&[self.source_identify.as_str()])
                        .inc_by(chunk.cardinality() as u64);
                    yield Message::Chunk(chunk);
                }
            }
        }

        // The source executor should only be stopped by the actor when finding a `Stop` mutation.
        tracing::error!(
            actor_id = self.ctx.id,
            "source executor exited unexpectedly"
        )
    }

    async fn apply_split_change(
        &mut self,
        source_desc: &SourceDescRef,
        stream: &mut SourceReaderStream,
        mapping: &HashMap<ActorId, Vec<SplitImpl>>,
    ) -> StreamExecutorResult<()> {
        if let Some(target_splits) = mapping.get(&self.ctx.id).cloned() {
            if let Some(target_state) = self.get_diff(Some(target_splits)).await? {
                self.replace_stream_reader_with_target_state(source_desc, stream, target_state)
                    .await?;
            }
        }

        Ok(())
    }

    async fn replace_stream_reader_with_target_state(
        &mut self,
        source_desc: &SourceDescRef,
        stream: &mut SourceReaderStream,
        target_state: Vec<SplitImpl>,
    ) -> StreamExecutorResult<()> {
        tracing::info!(
            "actor {:?} apply source split change to {:?}",
            self.ctx.id,
            target_state
        );

        // Replace the source reader with a new one of the new state.
        let reader = self
            .build_stream_source_reader(source_desc, Some(target_state.clone()))
            .await?;
        stream.replace_source_stream(reader);

        self.stream_source_splits = target_state;

        Ok(())
    }
}

impl<S: StateStore> Executor for SourceExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
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
    use std::time::Duration;

    use bytes::Bytes;
    use futures::StreamExt;
    use maplit::{convert_args, hashmap};
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_connector::source::datagen::DatagenSplit;
    use risingwave_pb::catalog::{ColumnIndex as ProstColumnIndex, StreamSourceInfo};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType as ProstDataType;
    use risingwave_pb::plan_common::{
        ColumnCatalog as ProstColumnCatalog, ColumnDesc as ProstColumnDesc,
        RowFormatType as ProstRowFormatType,
    };
    use risingwave_pb::stream_plan::source_node::Info as ProstSourceInfo;
    use risingwave_source::table_test_utils::create_table_info;
    use risingwave_source::*;
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    #[tokio::test]
    async fn test_table_source() {
        let table_id = TableId::default();

        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Varchar),
            ],
        };
        let row_id_index = Some(0);
        let pk_column_ids = vec![0];
        let info = create_table_info(&schema, row_id_index, pk_column_ids);
        let source_manager: TableSourceManagerRef = Arc::new(MemSourceManager::default());
        let source_builder = SourceDescBuilder::new(table_id, &info, &source_manager);
        let source_desc = source_builder.build().await.unwrap();

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

        let column_ids = vec![0, 1, 2].into_iter().map(ColumnId::from).collect();
        let pk_indices = vec![0];

        let (barrier_sender, barrier_receiver) = unbounded_channel();
        let state_table = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            MemoryStateStore::new(),
        );
        let vnodes = Bitmap::from_bytes(Bytes::from_static(&[0b11111111]));

        let executor = SourceExecutor::new(
            ActorContext::create(0x3f3f3f),
            source_builder,
            table_id,
            vnodes,
            state_table,
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
            let table_source = source_desc.source.as_table().unwrap();
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

        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Varchar),
            ],
        };
        let row_id_index = Some(0);
        let pk_column_ids = vec![0];
        let info = create_table_info(&schema, row_id_index, pk_column_ids);
        let source_manager: TableSourceManagerRef = Arc::new(MemSourceManager::default());
        let source_builder = SourceDescBuilder::new(table_id, &info, &source_manager);
        let source_desc = source_builder.build().await.unwrap();

        // Prepare test data chunks
        let chunk = StreamChunk::from_pretty(
            " I i T
            + 0 1 foo
            + 0 2 bar
            + 0 3 baz",
        );

        let column_ids = vec![0.into(), 1.into(), 2.into()];
        let pk_indices = vec![0];

        let (barrier_sender, barrier_receiver) = unbounded_channel();
        let state_table = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            MemoryStateStore::new(),
        );

        let vnodes = Bitmap::from_bytes(Bytes::from_static(&[0b11111111]));
        let executor = SourceExecutor::new(
            ActorContext::create(0x3f3f3f),
            source_builder,
            table_id,
            vnodes,
            state_table,
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
            let table_source = source_desc.source.as_table().unwrap();
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
        let properties = convert_args!(hashmap!(
            "connector" => "datagen",
            "fields.v1.min" => "1",
            "fields.v1.max" => "1000",
            "fields.v1.seed" => "12345",
        ));

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
        let source_manager: TableSourceManagerRef = Arc::new(MemSourceManager::default());

        let get_schema = |column_ids: &[ColumnId], source_desc: &SourceDescRef| {
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

        let source_builder = SourceDescBuilder::new(
            source_table_id,
            &ProstSourceInfo::StreamSource(stream_source_info),
            &source_manager,
        );
        let source_desc = source_builder.clone().build().await.unwrap();
        let mem_state_store = MemoryStateStore::new();

        let column_ids = vec![ColumnId::from(0), ColumnId::from(1)];
        let schema = get_schema(&column_ids, &source_desc);
        let pk_indices = vec![0_usize];
        let (barrier_tx, barrier_rx) = unbounded_channel::<Barrier>();
        let vnodes = Bitmap::from_bytes(Bytes::from_static(&[0b11111111]));
        let mut source_state_handler = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            mem_state_store.clone(),
        );

        let source_exec = SourceExecutor::new(
            ActorContext::create(0),
            source_builder,
            source_table_id,
            vnodes,
            source_state_handler.clone(),
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

        let init_barrier = Barrier::new_test_barrier(1).with_mutation(Mutation::Add {
            adds: HashMap::new(),
            splits: hashmap! {
                ActorId::default() => vec![
                    SplitImpl::Datagen(DatagenSplit {
                        split_index: 0,
                        split_num: 3,
                        start_offset: None,
                    }),
                ],
            },
        });
        barrier_tx.send(init_barrier).unwrap();

        (materialize.next().await.unwrap().unwrap())
            .into_barrier()
            .unwrap();

        let mut ready_chunks = materialize.ready_chunks(10);
        let chunks = (ready_chunks.next().await.unwrap())
            .into_iter()
            .map(|msg| msg.unwrap().into_chunk().unwrap())
            .collect();
        let chunk_1 = StreamChunk::concat(chunks).drop_row_id();
        assert_eq!(
            chunk_1,
            StreamChunk::from_pretty(
                " i
                + 533
                + 833
                + 738
                + 344",
            )
        );

        let new_assignments = vec![
            SplitImpl::Datagen(DatagenSplit {
                split_index: 0,
                split_num: 3,
                start_offset: None,
            }),
            SplitImpl::Datagen(DatagenSplit {
                split_index: 1,
                split_num: 3,
                start_offset: None,
            }),
        ];

        let change_split_mutation =
            Barrier::new_test_barrier(2).with_mutation(Mutation::SourceChangeSplit(hashmap! {
                ActorId::default() => new_assignments.clone()
            }));

        barrier_tx.send(change_split_mutation).unwrap();

        let _ = ready_chunks.next().await.unwrap(); // barrier

        // there must exist state for new add partition
        source_state_handler.init_epoch(EpochPair::new_test_epoch(2));
        source_state_handler
            .get(new_assignments[1].id())
            .await
            .unwrap()
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        let chunks = (ready_chunks.next().await.unwrap())
            .into_iter()
            .map(|msg| msg.unwrap().into_chunk().unwrap())
            .collect();
        let chunk_2 = StreamChunk::concat(chunks).drop_row_id().sort_rows();
        assert_eq!(
            chunk_2,
            // mixed from datagen split 0 and 1
            StreamChunk::from_pretty(
                " i
                + 29
                + 201
                + 344
                + 425
                + 525
                + 533
                + 833",
            )
        );

        let barrier = Barrier::new_test_barrier(3).with_mutation(Mutation::Pause);
        barrier_tx.send(barrier).unwrap();

        let barrier = Barrier::new_test_barrier(4).with_mutation(Mutation::Resume);
        barrier_tx.send(barrier).unwrap();
    }
}
