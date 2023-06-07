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

use std::fmt::Formatter;

use anyhow::anyhow;
use either::Either;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_connector::source::{
    BoxSourceWithStateStream, ConnectorState, SourceContext, SplitMetaData, StreamChunkWithState,
};
use risingwave_source::source_desc::{SourceDesc, SourceDescBuilder};
use risingwave_storage::StateStore;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Instant;

use super::executor_core::StreamSourceCore;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::executor::*;

/// A constant to multiply when calculating the maximum time to wait for a barrier. This is due to
/// some latencies in network and cost in meta.
const WAIT_BARRIER_MULTIPLE_TIMES: u128 = 5;

pub struct SourceExecutor<S: StateStore> {
    ctx: ActorContextRef,

    identity: String,

    schema: Schema,

    pk_indices: PkIndices,

    /// Streaming source for external
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Metrics for monitor.
    metrics: Arc<StreamingMetrics>,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    /// Expected barrier latency.
    expected_barrier_latency_ms: u64,
}

impl<S: StateStore> SourceExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        schema: Schema,
        pk_indices: PkIndices,
        stream_source_core: Option<StreamSourceCore<S>>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        expected_barrier_latency_ms: u64,
        executor_id: u64,
    ) -> Self {
        Self {
            ctx,
            identity: format!("SourceExecutor {:X}", executor_id),
            schema,
            pk_indices,
            stream_source_core,
            metrics,
            barrier_receiver: Some(barrier_receiver),
            expected_barrier_latency_ms,
        }
    }

    async fn build_stream_source_reader(
        &self,
        source_desc: &SourceDesc,
        state: ConnectorState,
    ) -> StreamExecutorResult<BoxSourceWithStateStream> {
        let column_ids = source_desc
            .columns
            .iter()
            .map(|column_desc| column_desc.column_id)
            .collect_vec();
        let mut source_ctx = SourceContext::new(
            self.ctx.id,
            self.stream_source_core.as_ref().unwrap().source_id,
            self.ctx.fragment_id,
            source_desc.metrics.clone(),
        );
        source_ctx.add_suppressor(self.ctx.error_suppressor.clone());
        source_desc
            .source
            .stream_reader(state, column_ids, Arc::new(source_ctx))
            .await
            .map_err(StreamExecutorError::connector_error)
    }

    fn check_split_is_migration(&self, actor_splits: &HashMap<ActorId, Vec<SplitImpl>>) -> bool {
        let core = self.stream_source_core.as_ref().unwrap();

        let mut revert_index = HashMap::new();

        for (actor_id, splits) in actor_splits {
            for split in splits {
                assert!(revert_index.insert(split.id(), actor_id).is_none());
            }
        }

        for split_id in core.state_cache.keys() {
            if let Some(actor_id) = revert_index.remove(split_id) {
                if self.ctx.id != *actor_id {
                    tracing::warn!(
                        "split {} migration detected, from {} to {}",
                        split_id,
                        self.ctx.id,
                        actor_id
                    );
                    return true;
                }
            }
        }

        false
    }

    async fn apply_split_change<const BIASED: bool>(
        &mut self,
        source_desc: &SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, StreamChunkWithState>,
        split_assignment: &HashMap<ActorId, Vec<SplitImpl>>,
    ) -> StreamExecutorResult<Option<Vec<SplitImpl>>> {
        if let Some(target_splits) = split_assignment.get(&self.ctx.id).cloned() {
            if let Some(target_state) = self.update_state_if_changed(Some(target_splits)).await? {
                tracing::info!(
                    actor_id = self.ctx.id,
                    state = ?target_state,
                    "apply split change"
                );

                self.replace_stream_reader_with_target_state(
                    source_desc,
                    stream,
                    target_state.clone(),
                )
                .await?;

                return Ok(Some(target_state));
            }
        }

        Ok(None)
    }

    // Note: `update_state_if_changed` will modify `state_cache`
    async fn update_state_if_changed(
        &mut self,
        state: ConnectorState,
    ) -> StreamExecutorResult<ConnectorState> {
        let core = self.stream_source_core.as_mut().unwrap();

        let target_splits: HashMap<_, _> = state
            .unwrap()
            .into_iter()
            .map(|split| (split.id(), split))
            .collect();

        let mut target_state: Vec<SplitImpl> = Vec::with_capacity(target_splits.len());

        let mut split_changed = false;

        for (split_id, split) in &target_splits {
            if let Some(s) = core.state_cache.get(split_id) {
                // existing split, no change, clone from cache
                target_state.push(s.clone())
            } else {
                split_changed = true;
                // write new assigned split to state cache. snapshot is base on cache.

                let initial_state = if let Some(recover_state) = core
                    .split_state_store
                    .try_recover_from_state_store(split)
                    .await?
                {
                    recover_state
                } else {
                    split.clone()
                };

                core.state_cache
                    .entry(split.id())
                    .or_insert_with(|| initial_state.clone());

                target_state.push(initial_state);
            }
        }

        // state cache may be stale
        for existing_split_id in core.stream_source_splits.keys() {
            if !target_splits.contains_key(existing_split_id) {
                tracing::info!("split dropping detected: {}", existing_split_id);
                split_changed = true;
            }
        }

        Ok(split_changed.then_some(target_state))
    }

    async fn replace_stream_reader_with_target_state<const BIASED: bool>(
        &mut self,
        source_desc: &SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, StreamChunkWithState>,
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

        stream.replace_data_stream(reader);

        Ok(())
    }

    async fn take_snapshot_and_clear_cache(
        &mut self,
        epoch: EpochPair,
        target_state: Option<Vec<SplitImpl>>,
        should_trim_state: bool,
    ) -> StreamExecutorResult<()> {
        let core = self.stream_source_core.as_mut().unwrap();

        let mut cache = core
            .state_cache
            .values()
            .map(|split_impl| split_impl.to_owned())
            .collect_vec();

        if let Some(target_splits) = target_state {
            let target_split_ids: HashSet<_> =
                target_splits.iter().map(|split| split.id()).collect();

            cache.drain_filter(|split| !target_split_ids.contains(&split.id()));

            let dropped_splits = core
                .stream_source_splits
                .drain_filter(|split_id, _| !target_split_ids.contains(split_id))
                .map(|(_, split)| split)
                .collect_vec();

            if should_trim_state && !dropped_splits.is_empty() {
                // trim dropped splits' state
                core.split_state_store.trim_state(&dropped_splits).await?;
            }

            core.stream_source_splits = target_splits
                .into_iter()
                .map(|split| (split.id(), split))
                .collect();
        }

        if !cache.is_empty() {
            tracing::debug!(actor_id = self.ctx.id, state = ?cache, "take snapshot");
            core.split_state_store.take_snapshot(cache).await?
        }
        // commit anyway, even if no message saved
        core.split_state_store.state_store.commit(epoch).await?;

        core.state_cache.clear();

        Ok(())
    }

    /// A source executor with a stream source receives:
    /// 1. Barrier messages
    /// 2. Data from external source
    /// and acts accordingly.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_with_stream_source(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let barrier = barrier_receiver
            .recv()
            .instrument_await("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                StreamExecutorError::from(anyhow!(
                    "failed to receive the first barrier, actor_id: {:?}, source_id: {:?}",
                    self.ctx.id,
                    self.stream_source_core.as_ref().unwrap().source_id
                ))
            })?;

        let mut core = self.stream_source_core.unwrap();

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder = core.source_desc_builder.take().unwrap();
        let source_desc = source_desc_builder
            .build()
            .await
            .map_err(StreamExecutorError::connector_error)?;

        let mut boot_state = Vec::default();
        if let Some(mutation) = barrier.mutation.as_ref() {
            match mutation.as_ref() {
                Mutation::Add { splits, .. }
                | Mutation::Update {
                    actor_splits: splits,
                    ..
                } => {
                    if let Some(splits) = splits.get(&self.ctx.id) {
                        boot_state = splits.clone();
                    }
                }
                _ => {}
            }
        }

        core.split_state_store.init_epoch(barrier.epoch);

        core.stream_source_splits = boot_state
            .clone()
            .into_iter()
            .map(|split| (split.id(), split))
            .collect();

        for ele in &mut boot_state {
            if let Some(recover_state) = core
                .split_state_store
                .try_recover_from_state_store(ele)
                .await?
            {
                *ele = recover_state;
            }
        }

        // Return the ownership of `stream_source_core` to the source executor.
        self.stream_source_core = Some(core);

        let recover_state: ConnectorState = (!boot_state.is_empty()).then_some(boot_state);
        tracing::info!(actor_id = self.ctx.id, state = ?recover_state, "start with state");

        let source_chunk_reader = self
            .build_stream_source_reader(&source_desc, recover_state)
            .instrument_await("source_build_reader")
            .await?;

        // Merge the chunks from source and the barriers into a single stream. We prioritize
        // barriers over source data chunks here.
        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();
        let mut stream = StreamReaderWithPause::<true, StreamChunkWithState>::new(
            barrier_stream,
            source_chunk_reader,
        );

        // If the first barrier is configuration change, then the source executor must be newly
        // created, and we should start with the paused state.
        if barrier.is_update() {
            stream.pause_stream();
        }

        yield Message::Barrier(barrier);

        // We allow data to flow for `WAIT_BARRIER_MULTIPLE_TIMES` * `expected_barrier_latency_ms`
        // milliseconds, considering some other latencies like network and cost in Meta.
        let max_wait_barrier_time_ms =
            self.expected_barrier_latency_ms as u128 * WAIT_BARRIER_MULTIPLE_TIMES;
        let mut last_barrier_time = Instant::now();
        let mut self_paused = false;
        let mut metric_row_per_barrier: u64 = 0;
        while let Some(msg) = stream.next().await {
            match msg? {
                // This branch will be preferred.
                Either::Left(msg) => match &msg {
                    Message::Barrier(barrier) => {
                        last_barrier_time = Instant::now();

                        if self_paused {
                            stream.resume_stream();
                            self_paused = false;
                        }

                        let epoch = barrier.epoch;

                        let mut target_state = None;
                        let mut should_trim_state = false;

                        if let Some(ref mutation) = barrier.mutation.as_deref() {
                            match mutation {
                                Mutation::Pause => stream.pause_stream(),
                                Mutation::Resume => stream.resume_stream(),
                                Mutation::SourceChangeSplit(actor_splits) => {
                                    // In the context of split changes, we do not allow split
                                    // migration because it can lead to inconsistent states.
                                    // Therefore, all split migration must be done via update
                                    // mutation and pause/resume
                                    assert!(!self.check_split_is_migration(actor_splits));

                                    target_state = self
                                        .apply_split_change(&source_desc, &mut stream, actor_splits)
                                        .await?;
                                    should_trim_state = true;
                                }

                                Mutation::Update { actor_splits, .. } => {
                                    target_state = self
                                        .apply_split_change(&source_desc, &mut stream, actor_splits)
                                        .await?;
                                }
                                _ => {}
                            }
                        }

                        self.take_snapshot_and_clear_cache(epoch, target_state, should_trim_state)
                            .await?;

                        self.metrics
                            .source_row_per_barrier
                            .with_label_values(&[
                                self.ctx.id.to_string().as_str(),
                                self.stream_source_core
                                    .as_ref()
                                    .unwrap()
                                    .source_id
                                    .to_string()
                                    .as_ref(),
                            ])
                            .inc_by(metric_row_per_barrier);
                        metric_row_per_barrier = 0;

                        yield msg;
                    }
                    _ => {
                        // For the source executor, the message we receive from this arm should
                        // always be barrier message.
                        unreachable!();
                    }
                },

                Either::Right(StreamChunkWithState {
                    chunk,
                    split_offset_mapping,
                }) => {
                    if last_barrier_time.elapsed().as_millis() > max_wait_barrier_time_ms {
                        // Exceeds the max wait barrier time, the source will be paused. Currently
                        // we can guarantee the source is not paused since it received stream
                        // chunks.
                        self_paused = true;
                        tracing::warn!(
                            "source {} paused, wait barrier for {:?}",
                            self.identity,
                            last_barrier_time.elapsed()
                        );
                        stream.pause_stream();
                    }
                    if let Some(mapping) = split_offset_mapping {
                        let state: HashMap<_, _> = mapping
                            .iter()
                            .flat_map(|(split_id, offset)| {
                                let origin_split_impl = self
                                    .stream_source_core
                                    .as_ref()
                                    .unwrap()
                                    .stream_source_splits
                                    .get(split_id);

                                origin_split_impl.map(|split_impl| {
                                    (split_id.clone(), split_impl.update(offset.clone()))
                                })
                            })
                            .collect();

                        self.stream_source_core
                            .as_mut()
                            .unwrap()
                            .state_cache
                            .extend(state);
                    }
                    metric_row_per_barrier += chunk.cardinality() as u64;

                    self.metrics
                        .source_output_row_count
                        .with_label_values(&[
                            self.stream_source_core
                                .as_ref()
                                .unwrap()
                                .source_id
                                .to_string()
                                .as_ref(),
                            self.stream_source_core
                                .as_ref()
                                .unwrap()
                                .source_name
                                .as_ref(),
                            self.ctx.id.to_string().as_str(),
                        ])
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

    /// A source executor without stream source only receives barrier messages and sends them to
    /// the downstream executor.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_without_stream_source(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let barrier = barrier_receiver
            .recv()
            .instrument_await("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                StreamExecutorError::from(anyhow!(
                    "failed to receive the first barrier, actor_id: {:?} with no stream source",
                    self.ctx.id
                ))
            })?;
        yield Message::Barrier(barrier);

        while let Some(barrier) = barrier_receiver.recv().await {
            yield Message::Barrier(barrier);
        }
    }
}

impl<S: StateStore> Executor for SourceExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        if self.stream_source_core.is_some() {
            self.execute_with_stream_source().boxed()
        } else {
            self.execute_without_stream_source().boxed()
        }
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
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("SourceExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .field("pk_indices", &self.pk_indices)
                .finish()
        } else {
            f.debug_struct("SourceExecutor").finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use maplit::{convert_args, hashmap};
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnId, Field, Schema, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_connector::source::datagen::DatagenSplit;
    use risingwave_pb::catalog::StreamSourceInfo;
    use risingwave_pb::plan_common::PbRowFormatType;
    use risingwave_source::connector_test_utils::create_source_desc_builder;
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::sync::mpsc::unbounded_channel;
    use tracing_test::traced_test;

    use super::*;
    use crate::executor::ActorContext;

    const MOCK_SOURCE_NAME: &str = "mock_source";

    #[tokio::test]
    async fn test_source_executor() {
        let table_id = TableId::default();
        let schema = Schema {
            fields: vec![Field::with_name(DataType::Int32, "sequence_int")],
        };
        let row_id_index = None;
        let pk_indices = vec![0];
        let source_info = StreamSourceInfo {
            row_format: PbRowFormatType::Native as i32,
            ..Default::default()
        };
        let (barrier_tx, barrier_rx) = unbounded_channel::<Barrier>();
        let column_ids = vec![0].into_iter().map(ColumnId::from).collect();

        // This datagen will generate 3 rows at one time.
        let properties: HashMap<String, String> = convert_args!(hashmap!(
            "connector" => "datagen",
            "datagen.rows.per.second" => "3",
            "fields.sequence_int.kind" => "sequence",
            "fields.sequence_int.start" => "11",
            "fields.sequence_int.end" => "11111",
        ));
        let source_desc_builder =
            create_source_desc_builder(&schema, row_id_index, source_info, properties);
        let split_state_store = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            MemoryStateStore::new(),
        )
        .await;
        let core = StreamSourceCore::<MemoryStateStore> {
            source_id: table_id,
            column_ids,
            source_desc_builder: Some(source_desc_builder),
            stream_source_splits: HashMap::new(),
            split_state_store,
            state_cache: HashMap::new(),
            source_name: MOCK_SOURCE_NAME.to_string(),
        };

        let executor = SourceExecutor::new(
            ActorContext::create(0),
            schema,
            pk_indices,
            Some(core),
            Arc::new(StreamingMetrics::unused()),
            barrier_rx,
            u64::MAX,
            1,
        );
        let mut executor = Box::new(executor).execute();

        let init_barrier = Barrier::new_test_barrier(1).with_mutation(Mutation::Add {
            adds: HashMap::new(),
            added_actors: HashSet::new(),
            splits: hashmap! {
                ActorId::default() => vec![
                    SplitImpl::Datagen(DatagenSplit {
                        split_index: 0,
                        split_num: 1,
                        start_offset: None,
                    }),
                ],
            },
        });
        barrier_tx.send(init_barrier).unwrap();

        // Consume barrier.
        executor.next().await.unwrap().unwrap();

        // Consume data chunk.
        let msg = executor.next().await.unwrap().unwrap();

        // Row id will not be filled here.
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " i
                + 11
                + 12
                + 13"
            )
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_split_change_mutation() {
        let table_id = TableId::default();
        let schema = Schema {
            fields: vec![Field::with_name(DataType::Int32, "v1")],
        };
        let row_id_index = None;
        let pk_indices = vec![0_usize];
        let source_info = StreamSourceInfo {
            row_format: PbRowFormatType::Native as i32,
            ..Default::default()
        };
        let properties = convert_args!(hashmap!(
            "connector" => "datagen",
            "fields.v1.kind" => "sequence",
            "fields.v1.start" => "11",
            "fields.v1.end" => "11111",
        ));

        let source_desc_builder =
            create_source_desc_builder(&schema, row_id_index, source_info, properties);
        let mem_state_store = MemoryStateStore::new();

        let column_ids = vec![ColumnId::from(0)];
        let (barrier_tx, barrier_rx) = unbounded_channel::<Barrier>();
        let split_state_store = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            mem_state_store.clone(),
        )
        .await;

        let core = StreamSourceCore::<MemoryStateStore> {
            source_id: table_id,
            column_ids: column_ids.clone(),
            source_desc_builder: Some(source_desc_builder),
            stream_source_splits: HashMap::new(),
            split_state_store,
            state_cache: HashMap::new(),
            source_name: MOCK_SOURCE_NAME.to_string(),
        };

        let executor = SourceExecutor::new(
            ActorContext::create(0),
            schema,
            pk_indices,
            Some(core),
            Arc::new(StreamingMetrics::unused()),
            barrier_rx,
            u64::MAX,
            1,
        );
        let mut handler = Box::new(executor).execute();

        let init_barrier = Barrier::new_test_barrier(1).with_mutation(Mutation::Add {
            adds: HashMap::new(),
            added_actors: HashSet::new(),
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

        // Consume barrier.
        handler
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_barrier()
            .unwrap();

        let mut ready_chunks = handler.ready_chunks(10);

        let _ = ready_chunks.next().await.unwrap();

        let new_assignment = vec![
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
            SplitImpl::Datagen(DatagenSplit {
                split_index: 2,
                split_num: 3,
                start_offset: None,
            }),
        ];

        let change_split_mutation =
            Barrier::new_test_barrier(2).with_mutation(Mutation::SourceChangeSplit(hashmap! {
                ActorId::default() => new_assignment.clone()
            }));

        barrier_tx.send(change_split_mutation).unwrap();

        let _ = ready_chunks.next().await.unwrap(); // barrier

        let mut source_state_handler = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            mem_state_store.clone(),
        )
        .await;
        // there must exist state for new add partition
        source_state_handler.init_epoch(EpochPair::new_test_epoch(2));
        source_state_handler
            .get(new_assignment[1].id())
            .await
            .unwrap()
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let _ = ready_chunks.next().await.unwrap();

        let barrier = Barrier::new_test_barrier(3).with_mutation(Mutation::Pause);
        barrier_tx.send(barrier).unwrap();

        let barrier = Barrier::new_test_barrier(4).with_mutation(Mutation::Resume);
        barrier_tx.send(barrier).unwrap();

        // receive all
        ready_chunks.next().await.unwrap();

        let prev_assignment = new_assignment;
        let new_assignment = vec![prev_assignment[2].clone()];

        let drop_split_mutation =
            Barrier::new_test_barrier(5).with_mutation(Mutation::SourceChangeSplit(hashmap! {
                ActorId::default() => new_assignment.clone()
            }));

        barrier_tx.send(drop_split_mutation).unwrap();

        ready_chunks.next().await.unwrap(); // barrier

        let mut source_state_handler = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            mem_state_store.clone(),
        )
        .await;

        source_state_handler.init_epoch(EpochPair::new_test_epoch(5));

        assert!(source_state_handler
            .try_recover_from_state_store(&prev_assignment[0])
            .await
            .unwrap()
            .is_none());

        assert!(source_state_handler
            .try_recover_from_state_store(&prev_assignment[1])
            .await
            .unwrap()
            .is_none());

        assert!(source_state_handler
            .try_recover_from_state_store(&prev_assignment[2])
            .await
            .unwrap()
            .is_some());
    }
}
