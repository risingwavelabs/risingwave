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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use anyhow::anyhow;
use either::Either;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;
use risingwave_connector::source::{
    BoxSourceWithStateStream, ConnectorState, SourceInfo, SplitId, SplitImpl, SplitMetaData,
    StreamChunkWithState,
};
use risingwave_source::source_desc::{FsSourceDesc, SourceDescBuilder};
use risingwave_storage::StateStore;
use tokio::sync::mpsc::UnboundedReceiver;

use super::executor_core::StreamSourceCore;
use crate::error::StreamResult;
use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::source::reader::SourceReaderStream;
use crate::executor::throttler::SourceThrottlerImpl;
use crate::executor::*;

/// [`FsSourceExecutor`] is a streaming source, fir external file systems
/// such as s3.
pub struct FsSourceExecutor<S: StateStore> {
    ctx: ActorContextRef,

    identity: String,

    schema: Schema,

    pk_indices: PkIndices,

    /// Streaming source  for external
    stream_source_core: StreamSourceCore<S>,

    /// Metrics for monitor.
    metrics: Arc<StreamingMetrics>,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    throttlers: Vec<SourceThrottlerImpl>,
}

impl<S: StateStore> FsSourceExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        schema: Schema,
        pk_indices: PkIndices,
        stream_source_core: StreamSourceCore<S>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        throttlers: Vec<SourceThrottlerImpl>,
        executor_id: u64,
    ) -> StreamResult<Self> {
        Ok(Self {
            ctx,
            identity: format!("SourceExecutor {:X}", executor_id),
            schema,
            pk_indices,
            stream_source_core,
            metrics,
            barrier_receiver: Some(barrier_receiver),
            throttlers,
        })
    }

    async fn build_stream_source_reader(
        &mut self,
        source_desc: &FsSourceDesc,
        state: ConnectorState,
    ) -> StreamExecutorResult<BoxSourceWithStateStream> {
        let column_ids = source_desc
            .columns
            .iter()
            .map(|column_desc| column_desc.column_id)
            .collect_vec();
        let steam_reader = source_desc
            .source
            .stream_reader(
                state,
                column_ids,
                source_desc.metrics.clone(),
                SourceInfo::new(self.ctx.id, self.stream_source_core.source_id),
            )
            .await
            .map_err(StreamExecutorError::connector_error)?;
        Ok(steam_reader.into_stream())
    }

    async fn apply_split_change(
        &mut self,
        source_desc: &FsSourceDesc,
        stream: &mut SourceReaderStream,
        mapping: &HashMap<ActorId, Vec<SplitImpl>>,
    ) -> StreamExecutorResult<()> {
        if let Some(target_splits) = mapping.get(&self.ctx.id).cloned() {
            if let Some(target_state) = self.get_diff(target_splits).await? {
                tracing::info!(
                    actor_id = self.ctx.id,
                    state = ?target_state,
                    "apply split change"
                );

                self.replace_stream_reader_with_target_state(source_desc, stream, target_state)
                    .await?;
            }
        }

        Ok(())
    }

    // Note: get_diff will modify the state_cache
    // rhs can not be None because we do not support split number reduction
    async fn get_diff(&mut self, rhs: Vec<SplitImpl>) -> StreamExecutorResult<ConnectorState> {
        let core = &mut self.stream_source_core;
        let all_completed: HashSet<SplitId> = core.split_state_store.get_all_completed().await?;

        tracing::debug!(actor = self.ctx.id, all_completed = ?all_completed , "get diff");

        let mut target_state: Vec<SplitImpl> = Vec::new();
        let mut no_change_flag = true;
        for sc in rhs {
            if let Some(s) = core.state_cache.get(&sc.id()) {
                let fs = s
                    .as_fs()
                    .unwrap_or_else(|| panic!("split {:?} is not fs", s));
                // unfinished this epoch
                if fs.offset < fs.size {
                    target_state.push(s.clone())
                }
            } else if all_completed.contains(&sc.id()) {
                // finish in prev epoch
                continue;
            } else {
                no_change_flag = false;
                // write new assigned split to state cache. snapshot is base on cache.
                let state = if let Some(recover_state) = core
                    .split_state_store
                    .try_recover_from_state_store(&sc)
                    .await?
                {
                    recover_state
                } else {
                    sc
                };

                core.state_cache
                    .entry(state.id())
                    .or_insert_with(|| state.clone());
                target_state.push(state);
            }
        }
        Ok((!no_change_flag).then_some(target_state))
    }

    async fn replace_stream_reader_with_target_state(
        &mut self,
        source_desc: &FsSourceDesc,
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

        self.stream_source_core.stream_source_splits = target_state
            .into_iter()
            .map(|split| (split.id(), split))
            .collect();

        Ok(())
    }

    async fn take_snapshot_and_clear_cache(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<()> {
        let core = &mut self.stream_source_core;
        let incompleted = core
            .state_cache
            .values()
            .filter(|split| {
                let fs = split
                    .as_fs()
                    .unwrap_or_else(|| panic!("split {:?} is not fs", split));
                fs.offset < fs.size
            })
            .cloned()
            .collect_vec();

        let completed = core
            .state_cache
            .values()
            .filter(|split| {
                let fs = split
                    .as_fs()
                    .unwrap_or_else(|| panic!("split {:?} is not fs", split));
                fs.offset == fs.size
            })
            .cloned()
            .collect_vec();

        if !incompleted.is_empty() {
            tracing::debug!(actor_id = self.ctx.id, incompleted = ?incompleted, "take snapshot");
            core.split_state_store.take_snapshot(incompleted).await?
        }

        if !completed.is_empty() {
            tracing::debug!(actor_id = self.ctx.id, completed = ?completed, "take snapshot");
            core.split_state_store.set_all_complete(completed).await?
        }
        // commit anyway, even if no message saved
        core.split_state_store.state_store.commit(epoch).await?;

        core.state_cache.clear();
        Ok(())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let barrier = barrier_receiver
            .recv()
            .stack_trace("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                StreamExecutorError::from(anyhow!(
                    "failed to receive the first barrier, actor_id: {:?}, source_id: {:?}",
                    self.ctx.id,
                    self.stream_source_core.source_id
                ))
            })?;

        let source_desc_builder: SourceDescBuilder =
            self.stream_source_core.source_desc_builder.take().unwrap();

        let source_desc = source_desc_builder
            .build_fs_source_desc()
            .await
            .map_err(StreamExecutorError::connector_error)?;

        // If the first barrier is configuration change, then the source executor must be newly
        // created, and we should start with the paused state.
        let start_with_paused = barrier.is_update();

        let mut boot_state = Vec::default();
        if let Some(mutation) = barrier.mutation.as_deref() {
            match mutation {
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

        self.stream_source_core
            .split_state_store
            .init_epoch(barrier.epoch);

        let all_completed: HashSet<SplitId> = self
            .stream_source_core
            .split_state_store
            .get_all_completed()
            .await?;
        tracing::debug!(actor = self.ctx.id, all_completed = ?all_completed , "get diff");

        let mut boot_state = boot_state
            .into_iter()
            .filter(|split| !all_completed.contains(&split.id()))
            .collect_vec();

        self.stream_source_core.stream_source_splits = boot_state
            .clone()
            .into_iter()
            .map(|split| (split.id(), split))
            .collect();

        // restore the newest split info
        for ele in &mut boot_state {
            if let Some(recover_state) = self
                .stream_source_core
                .split_state_store
                .try_recover_from_state_store(ele)
                .await?
            {
                *ele = recover_state;
            }
        }

        let recover_state: ConnectorState = (!boot_state.is_empty()).then_some(boot_state);
        tracing::info!(actor_id = self.ctx.id, state = ?recover_state, "start with state");

        let source_chunk_reader = self
            .build_stream_source_reader(&source_desc, recover_state)
            .stack_trace("fs_source_start_reader")
            .await?;

        // Merge the chunks from source and the barriers into a single stream.
        let mut stream = SourceReaderStream::new(barrier_receiver, source_chunk_reader);
        if start_with_paused {
            stream.pause_source();
        }

        yield Message::Barrier(barrier);

        let mut self_paused = false;
        let mut metric_row_per_barrier: u64 = 0;
        while let Some(msg) = stream.next().await {
            match msg? {
                // This branch will be preferred.
                Either::Left(barrier) => {
                    for throttler in &mut self.throttlers {
                        throttler.on_barrier();
                    }
                    if self_paused && self.throttlers.iter().all(|t| !t.should_pause()) {
                        stream.resume_source();
                        self_paused = false;
                    }
                    let epoch = barrier.epoch;

                    if let Some(ref mutation) = barrier.mutation.as_deref() {
                        match mutation {
                            Mutation::SourceChangeSplit(actor_splits) => {
                                self.apply_split_change(&source_desc, &mut stream, actor_splits)
                                    .await?
                            }
                            Mutation::Pause => stream.pause_source(),
                            Mutation::Resume => stream.resume_source(),
                            Mutation::Update { actor_splits, .. } => {
                                self.apply_split_change(&source_desc, &mut stream, actor_splits)
                                    .await?;
                            }
                            _ => {}
                        }
                    }
                    self.take_snapshot_and_clear_cache(epoch).await?;

                    self.metrics
                        .source_row_per_barrier
                        .with_label_values(&[
                            self.ctx.id.to_string().as_str(),
                            self.stream_source_core.source_identify.as_ref(),
                        ])
                        .inc_by(metric_row_per_barrier);
                    metric_row_per_barrier = 0;

                    yield Message::Barrier(barrier);
                }

                Either::Right(StreamChunkWithState {
                    chunk,
                    split_offset_mapping,
                }) => {
                    if !self_paused && self.throttlers.iter().any(|t| t.should_pause()) {
                        self_paused = true;
                        stream.pause_source();
                    }
                    // update split offset
                    if let Some(mapping) = split_offset_mapping {
                        let state: Vec<(SplitId, SplitImpl)> = mapping
                            .iter()
                            .flat_map(|(id, offset)| {
                                let origin_split =
                                    self.stream_source_core.stream_source_splits.get(id);

                                origin_split.map(|split| (id.clone(), split.update(offset.clone())))
                            })
                            .collect_vec();

                        self.stream_source_core.state_cache.extend(state);
                    }

                    self.metrics
                        .source_output_row_count
                        .with_label_values(&[
                            self.stream_source_core.source_identify.as_str(),
                            self.stream_source_core.source_name.as_ref(),
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
}

impl<S: StateStore> Executor for FsSourceExecutor<S> {
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

impl<S: StateStore> Debug for FsSourceExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsSourceExecutor")
            .field("source_id", &self.stream_source_core.source_id)
            .field("column_ids", &self.stream_source_core.column_ids)
            .field("pk_indices", &self.pk_indices)
            .finish()
    }
}
