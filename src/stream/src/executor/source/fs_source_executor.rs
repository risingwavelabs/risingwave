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

#![deprecated = "will be replaced by new fs source (list + fetch)"]
#![expect(deprecated)]

use std::collections::{HashMap, HashSet};

use anyhow::anyhow;
use either::Either;
use futures::TryStreamExt;
use itertools::Itertools;
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::util::epoch::EpochPair;
use risingwave_connector::error::ConnectorError;
use risingwave_connector::source::reader::desc::{FsSourceDesc, SourceDescBuilder};
use risingwave_connector::source::{
    BoxChunkSourceStream, ConnectorState, SourceContext, SourceCtrlOpts, SplitId, SplitImpl,
    SplitMetaData,
};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Instant;

use super::executor_core::StreamSourceCore;
use super::{
    apply_rate_limit, barrier_to_message_stream, get_split_offset_col_idx,
    get_split_offset_mapping_from_chunk, prune_additional_cols,
};
use crate::common::rate_limit::limited_chunk_size;
use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;
use crate::executor::{AddMutation, UpdateMutation};

/// A constant to multiply when calculating the maximum time to wait for a barrier. This is due to
/// some latencies in network and cost in meta.
const WAIT_BARRIER_MULTIPLE_TIMES: u128 = 5;

/// [`FsSourceExecutor`] is a streaming source, fir external file systems
/// such as s3.
pub struct FsSourceExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Streaming source  for external
    stream_source_core: StreamSourceCore<S>,

    /// Metrics for monitor.
    metrics: Arc<StreamingMetrics>,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    /// System parameter reader to read barrier interval
    system_params: SystemParamsReaderRef,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,
}

impl<S: StateStore> FsSourceExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        system_params: SystemParamsReaderRef,
        rate_limit_rps: Option<u32>,
    ) -> StreamResult<Self> {
        Ok(Self {
            actor_ctx,
            stream_source_core,
            metrics,
            barrier_receiver: Some(barrier_receiver),
            system_params,
            rate_limit_rps,
        })
    }

    async fn build_stream_source_reader(
        &mut self,
        source_desc: &FsSourceDesc,
        state: ConnectorState,
    ) -> StreamExecutorResult<BoxChunkSourceStream> {
        let column_ids = source_desc
            .columns
            .iter()
            .map(|column_desc| column_desc.column_id)
            .collect_vec();
        let source_ctx = SourceContext::new(
            self.actor_ctx.id,
            self.stream_source_core.source_id,
            self.actor_ctx.fragment_id,
            self.stream_source_core.source_name.clone(),
            source_desc.metrics.clone(),
            SourceCtrlOpts {
                chunk_size: limited_chunk_size(self.rate_limit_rps),
                rate_limit: self.rate_limit_rps,
            },
            source_desc.source.config.clone(),
        );
        let stream = source_desc
            .source
            .to_stream(state, column_ids, Arc::new(source_ctx))
            .await
            .map_err(StreamExecutorError::connector_error)?;

        Ok(apply_rate_limit(stream, self.rate_limit_rps).boxed())
    }

    async fn rebuild_stream_reader<const BIASED: bool>(
        &mut self,
        source_desc: &FsSourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, StreamChunk>,
    ) -> StreamExecutorResult<()> {
        let target_state: Vec<SplitImpl> = self
            .stream_source_core
            .latest_split_info
            .values()
            .cloned()
            .collect();
        let reader = self
            .build_stream_source_reader(source_desc, Some(target_state))
            .await?
            .map_err(StreamExecutorError::connector_error);
        stream.replace_data_stream(reader);

        Ok(())
    }

    async fn apply_split_change<const BIASED: bool>(
        &mut self,
        source_desc: &FsSourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, StreamChunk>,
        mapping: &HashMap<ActorId, Vec<SplitImpl>>,
    ) -> StreamExecutorResult<()> {
        if let Some(target_splits) = mapping.get(&self.actor_ctx.id).cloned() {
            if let Some(target_state) = self.get_diff(target_splits).await? {
                tracing::info!(
                    actor_id = self.actor_ctx.id,
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

        tracing::debug!(actor = self.actor_ctx.id, all_completed = ?all_completed , "get diff");

        let mut target_state: Vec<SplitImpl> = Vec::new();
        let mut no_change_flag = true;
        for sc in rhs {
            if let Some(s) = core.updated_splits_in_epoch.get(&sc.id()) {
                let fs = s
                    .as_s3()
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

                core.updated_splits_in_epoch
                    .entry(state.id())
                    .or_insert_with(|| state.clone());
                target_state.push(state);
            }
        }
        Ok((!no_change_flag).then_some(target_state))
    }

    async fn replace_stream_reader_with_target_state<const BIASED: bool>(
        &mut self,
        source_desc: &FsSourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, StreamChunk>,
        target_state: Vec<SplitImpl>,
    ) -> StreamExecutorResult<()> {
        tracing::info!(
            "actor {:?} apply source split change to {:?}",
            self.actor_ctx.id,
            target_state
        );

        // Replace the source reader with a new one of the new state.
        let reader = self
            .build_stream_source_reader(source_desc, Some(target_state.clone()))
            .await?
            .map_err(StreamExecutorError::connector_error);
        stream.replace_data_stream(reader);

        self.stream_source_core.latest_split_info = target_state
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
            .updated_splits_in_epoch
            .values()
            .filter(|split| {
                let fs = split
                    .as_s3()
                    .unwrap_or_else(|| panic!("split {:?} is not fs", split));
                fs.offset < fs.size
            })
            .cloned()
            .collect_vec();

        let completed = core
            .updated_splits_in_epoch
            .values()
            .filter(|split| {
                let fs = split
                    .as_s3()
                    .unwrap_or_else(|| panic!("split {:?} is not fs", split));
                fs.offset == fs.size
            })
            .cloned()
            .collect_vec();

        if !incompleted.is_empty() {
            tracing::debug!(incompleted = ?incompleted, "take snapshot");
            core.split_state_store.set_states(incompleted).await?
        }

        if !completed.is_empty() {
            tracing::debug!(completed = ?completed, "take snapshot");
            core.split_state_store.set_all_complete(completed).await?
        }
        // commit anyway, even if no message saved
        core.split_state_store.state_table.commit(epoch).await?;

        core.updated_splits_in_epoch.clear();
        Ok(())
    }

    async fn try_flush_data(&mut self) -> StreamExecutorResult<()> {
        let core = &mut self.stream_source_core;

        core.split_state_store.state_table.try_flush().await?;

        Ok(())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let barrier = barrier_receiver
            .recv()
            .instrument_await("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                anyhow!(
                    "failed to receive the first barrier, actor_id: {:?}, source_id: {:?}",
                    self.actor_ctx.id,
                    self.stream_source_core.source_id
                )
            })?;

        let source_desc_builder: SourceDescBuilder =
            self.stream_source_core.source_desc_builder.take().unwrap();

        let source_desc = source_desc_builder
            .build_fs_source_desc()
            .map_err(StreamExecutorError::connector_error)?;

        let (Some(split_idx), Some(offset_idx)) = get_split_offset_col_idx(&source_desc.columns)
        else {
            unreachable!("Partition and offset columns must be set.");
        };

        // If the first barrier requires us to pause on startup, pause the stream.
        let start_with_paused = barrier.is_pause_on_startup();

        let mut boot_state = Vec::default();
        if let Some(mutation) = barrier.mutation.as_deref() {
            match mutation {
                Mutation::Add(AddMutation { splits, .. })
                | Mutation::Update(UpdateMutation {
                    actor_splits: splits,
                    ..
                }) => {
                    if let Some(splits) = splits.get(&self.actor_ctx.id) {
                        boot_state.clone_from(splits);
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
        tracing::debug!(actor = self.actor_ctx.id, all_completed = ?all_completed , "get diff");

        let mut boot_state = boot_state
            .into_iter()
            .filter(|split| !all_completed.contains(&split.id()))
            .collect_vec();

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

        // init in-memory split states with persisted state if any
        self.stream_source_core.init_split_state(boot_state.clone());
        let recover_state: ConnectorState = (!boot_state.is_empty()).then_some(boot_state);
        tracing::debug!(state = ?recover_state, "start with state");

        let source_chunk_reader = self
            .build_stream_source_reader(&source_desc, recover_state)
            .instrument_await("fs_source_start_reader")
            .await?
            .map_err(StreamExecutorError::connector_error);

        // Merge the chunks from source and the barriers into a single stream. We prioritize
        // barriers over source data chunks here.
        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();
        let mut stream =
            StreamReaderWithPause::<true, StreamChunk>::new(barrier_stream, source_chunk_reader);
        if start_with_paused {
            stream.pause_stream();
        }

        yield Message::Barrier(barrier);

        // We allow data to flow for 5 * `expected_barrier_latency_ms` milliseconds, considering
        // some other latencies like network and cost in Meta.
        let mut max_wait_barrier_time_ms =
            self.system_params.load().barrier_interval_ms() as u128 * WAIT_BARRIER_MULTIPLE_TIMES;
        let mut last_barrier_time = Instant::now();
        let mut self_paused = false;
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

                        if let Some(ref mutation) = barrier.mutation.as_deref() {
                            match mutation {
                                Mutation::SourceChangeSplit(actor_splits) => {
                                    self.apply_split_change(&source_desc, &mut stream, actor_splits)
                                        .await?
                                }
                                Mutation::Pause => stream.pause_stream(),
                                Mutation::Resume => stream.resume_stream(),
                                Mutation::Update(UpdateMutation { actor_splits, .. }) => {
                                    self.apply_split_change(
                                        &source_desc,
                                        &mut stream,
                                        actor_splits,
                                    )
                                    .await?;
                                }
                                Mutation::Throttle(actor_to_apply) => {
                                    if let Some(new_rate_limit) =
                                        actor_to_apply.get(&self.actor_ctx.id)
                                        && *new_rate_limit != self.rate_limit_rps
                                    {
                                        self.rate_limit_rps = *new_rate_limit;
                                        self.rebuild_stream_reader(&source_desc, &mut stream)
                                            .await?;
                                    }
                                }
                                _ => {}
                            }
                        }
                        self.take_snapshot_and_clear_cache(epoch).await?;

                        yield msg;
                    }
                    _ => {
                        // For the source executor, the message we receive from this arm should
                        // always be barrier message.
                        unreachable!();
                    }
                },

                Either::Right(chunk) => {
                    // TODO: confirm when split_offset_mapping is None
                    let split_offset_mapping =
                        get_split_offset_mapping_from_chunk(&chunk, split_idx, offset_idx);
                    if last_barrier_time.elapsed().as_millis() > max_wait_barrier_time_ms {
                        // Exceeds the max wait barrier time, the source will be paused. Currently
                        // we can guarantee the source is not paused since it received stream
                        // chunks.
                        self_paused = true;
                        stream.pause_stream();

                        // Only update `max_wait_barrier_time_ms` to capture `barrier_interval_ms`
                        // changes here to avoid frequently accessing the shared `system_params`.
                        max_wait_barrier_time_ms = self.system_params.load().barrier_interval_ms()
                            as u128
                            * WAIT_BARRIER_MULTIPLE_TIMES;
                    }
                    // update split offset
                    if let Some(mapping) = split_offset_mapping {
                        let state: Vec<(SplitId, SplitImpl)> = mapping
                            .iter()
                            .flat_map(|(id, offset)| {
                                self.stream_source_core.latest_split_info.get_mut(id).map(
                                    |origin_split| {
                                        origin_split.update_in_place(offset.clone())?;
                                        Ok::<_, ConnectorError>((id.clone(), origin_split.clone()))
                                    },
                                )
                            })
                            .try_collect()?;

                        self.stream_source_core
                            .updated_splits_in_epoch
                            .extend(state);
                    }

                    self.metrics
                        .source_output_row_count
                        .with_guarded_label_values(&[
                            self.stream_source_core.source_id.to_string().as_ref(),
                            self.stream_source_core.source_name.as_ref(),
                            self.actor_ctx.id.to_string().as_str(),
                            self.actor_ctx.fragment_id.to_string().as_str(),
                        ])
                        .inc_by(chunk.cardinality() as u64);

                    let chunk =
                        prune_additional_cols(&chunk, split_idx, offset_idx, &source_desc.columns);
                    yield Message::Chunk(chunk);

                    self.try_flush_data().await?;
                }
            }
        }

        // The source executor should only be stopped by the actor when finding a `Stop` mutation.
        tracing::error!(
            actor_id = self.actor_ctx.id,
            "source executor exited unexpectedly"
        )
    }
}

impl<S: StateStore> Execute for FsSourceExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for FsSourceExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsSourceExecutor")
            .field("source_id", &self.stream_source_core.source_id)
            .field("column_ids", &self.stream_source_core.column_ids)
            .finish()
    }
}
