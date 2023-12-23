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

// FIXME: rebuild_stream_reader_from_error

use std::assert_matches::assert_matches;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::pin::pin;

use anyhow::anyhow;
use either::Either;
use futures::stream::{select_with_strategy, AbortHandle, Abortable};
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::row::{Row, RowExt};
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_common::types::JsonbVal;
use risingwave_connector::source::reader::desc::{SourceDesc, SourceDescBuilder};
use risingwave_connector::source::{
    BoxChunkSourceStream, ConnectorState, SourceContext, SourceCtrlOpts, SplitMetaData,
};
use risingwave_connector::ConnectorParams;
use risingwave_storage::StateStore;
use serde::{Deserialize, Serialize};

use super::executor_core::StreamSourceCore;
use super::kafka_backfill_state_table::BackfillStateTableHandler;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::*;

pub type SplitId = String;
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum BackfillState {
    /// `None` means not started yet. It's the initial state.
    Backfilling(Option<String>),
    /// Backfill is stopped at this offset. Source needs to filter out messages before this offset.
    SourceCachingUp(String),
    Finished,
}
pub type BackfillStates = HashMap<SplitId, BackfillState>;

impl BackfillState {
    pub fn encode_to_json(self) -> JsonbVal {
        serde_json::to_value(self).unwrap().into()
    }

    pub fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
    }

    /// Returns whether the row from upstream `SourceExecutor` is visible.
    fn handle_upstream_row(
        &mut self,
        split: &str,
        offset: &str,
        abort_handles: &HashMap<String, AbortHandle>,
    ) -> bool {
        let mut vis = false;
        match self {
            BackfillState::Backfilling(backfill_offset) => {
                match compare_kafka_offset(backfill_offset.as_ref(), offset) {
                    Ordering::Less => {
                        // continue backfilling. Ignore this row
                    }
                    Ordering::Equal => {
                        // backfilling for this split is finished just right.
                        *self = BackfillState::Finished;
                        abort_handles.get(split).unwrap().abort();
                    }
                    Ordering::Greater => {
                        // backfilling for this split produced more data.
                        *self = BackfillState::SourceCachingUp(offset.to_string());
                        abort_handles.get(&split.to_string()).unwrap().abort();
                    }
                }
            }
            BackfillState::SourceCachingUp(backfill_offset) => {
                match compare_kafka_offset(Some(backfill_offset), offset) {
                    Ordering::Less => {
                        // XXX: Is this possible? i.e., Source doesn't contain the
                        // last backfilled row.
                        vis = true;
                        *self = BackfillState::Finished;
                    }
                    Ordering::Equal => {
                        // Source just caught up with backfilling.
                        *self = BackfillState::Finished;
                    }
                    Ordering::Greater => {
                        // Source is still behind backfilling.
                        *backfill_offset = offset.to_string();
                    }
                }
            }
            BackfillState::Finished => {
                vis = true;
                // This split's backfilling is finisehd, we are waiting for other splits
            }
        }
        vis
    }
}

pub struct KafkaBackfillExecutorWrapper<S: StateStore> {
    pub inner: KafkaBackfillExecutor<S>,
    /// Upstream changelog stream which may contain metadata columns, e.g. `_rw_offset`
    pub input: Box<dyn Executor>,
}

pub struct KafkaBackfillExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Streaming source for external
    // FIXME: some fields e.g. its state table is not used. We might need to refactor
    stream_source_core: StreamSourceCore<S>,
    backfill_state_store: BackfillStateTableHandler<S>,

    /// Metrics for monitor.
    metrics: Arc<StreamingMetrics>,

    // /// Receiver of barrier channel.
    // barrier_receiver: Option<UnboundedReceiver<Barrier>>,
    /// System parameter reader to read barrier interval
    system_params: SystemParamsReaderRef,

    // control options for connector level
    source_ctrl_opts: SourceCtrlOpts,

    // config for the connector node
    connector_params: ConnectorParams,
}

impl<S: StateStore> KafkaBackfillExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        info: ExecutorInfo,
        stream_source_core: StreamSourceCore<S>,
        metrics: Arc<StreamingMetrics>,
        system_params: SystemParamsReaderRef,
        source_ctrl_opts: SourceCtrlOpts,
        connector_params: ConnectorParams,
        backfill_state_store: BackfillStateTableHandler<S>,
    ) -> Self {
        Self {
            actor_ctx,
            info,
            stream_source_core,
            backfill_state_store,
            metrics,
            system_params,
            source_ctrl_opts,
            connector_params,
        }
    }

    /// Unlike `SourceExecutor`, which creates a `stream_reader` with all splits,
    /// we create a separate `stream_reader` for each split here, because we
    /// want to abort early for each split after the split's backfilling is finished.
    async fn build_stream_source_reader(
        &self,
        source_desc: &SourceDesc,
        state: ConnectorState,
    ) -> StreamExecutorResult<(BoxChunkSourceStream, HashMap<SplitId, AbortHandle>)> {
        let column_ids = source_desc
            .columns
            .iter()
            .map(|column_desc| column_desc.column_id)
            .collect_vec();
        let source_ctx = SourceContext::new_with_suppressor(
            self.actor_ctx.id,
            self.stream_source_core.source_id,
            self.actor_ctx.fragment_id,
            source_desc.metrics.clone(),
            self.source_ctrl_opts.clone(),
            self.connector_params.connector_client.clone(),
            self.actor_ctx.error_suppressor.clone(),
            source_desc.source.config.clone(),
            self.stream_source_core.source_name.clone(),
        );
        let source_ctx = Arc::new(source_ctx);

        match state {
            Some(splits) => {
                let mut abort_handles = HashMap::new();
                let mut streams = vec![];
                for split in splits {
                    let split_id = split.id().to_string();
                    let reader = source_desc
                        .source
                        .to_stream(Some(vec![split]), column_ids.clone(), source_ctx.clone())
                        .await
                        .map_err(StreamExecutorError::connector_error)?;
                    let (abort_handle, abort_registration) = AbortHandle::new_pair();
                    let stream = Abortable::new(reader, abort_registration);
                    abort_handles.insert(split_id, abort_handle);
                    streams.push(stream);
                }
                return Ok((futures::stream::select_all(streams).boxed(), abort_handles));
            }
            None => return Ok((futures::stream::pending().boxed(), HashMap::new())),
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute(mut self, input: BoxedExecutor) {
        let mut input = input.execute();

        // Poll the upstream to get the first barrier.
        let barrier = expect_first_barrier(&mut input).await?;

        let mut core = self.stream_source_core;

        let source_desc_builder: SourceDescBuilder = core.source_desc_builder.take().unwrap();
        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;
        let (Some(split_idx), Some(offset_idx)) = get_split_offset_col_idx(&source_desc.columns)
        else {
            unreachable!("Partition and offset columns must be set.");
        };

        let mut boot_state = Vec::default();
        if let Some(mutation) = barrier.mutation.as_ref() {
            match mutation.as_ref() {
                Mutation::Add(AddMutation { splits, .. })
                | Mutation::Update(UpdateMutation {
                    actor_splits: splits,
                    ..
                }) => {
                    if let Some(splits) = splits.get(&self.actor_ctx.id) {
                        boot_state = splits.clone();
                    }
                }
                _ => {}
            }
        }
        self.backfill_state_store.init_epoch(barrier.epoch);

        let mut backfill_states: BackfillStates = HashMap::new();

        let mut unfinished_splits = vec![];
        for ele in boot_state {
            let split_id = ele.id().to_string();
            let (split, backfill_state) = self
                .backfill_state_store
                .try_recover_from_state_store(ele)
                .await?;

            backfill_states.insert(split_id, backfill_state);
            if split.is_some() {
                unfinished_splits.push(split.unwrap());
            }
        }
        let need_backfill = backfill_states
            .values()
            .any(|state| !matches!(state, BackfillState::Finished));
        tracing::debug!(?backfill_states, "source backfill started");

        // init in-memory split states with persisted state if any
        core.init_split_state(unfinished_splits.clone());

        // Return the ownership of `stream_source_core` to the source executor.
        self.stream_source_core = core;

        let recover_state: ConnectorState =
            (!unfinished_splits.is_empty()).then_some(unfinished_splits);
        let (source_chunk_reader, abort_handles) = self
            .build_stream_source_reader(&source_desc, recover_state)
            .instrument_await("source_build_reader")
            .await?;
        let source_chunk_reader = pin!(source_chunk_reader);

        // If the first barrier requires us to pause on startup, pause the stream.
        if barrier.is_pause_on_startup() {
            // TODO: support pause on startup
        }

        yield Message::Barrier(barrier);

        // XXX:
        // - What's the best poll strategy?
        // - Should we also add a barrier stream for backfill executor?
        let mut backfill_stream = select_with_strategy(
            input.by_ref().map(Either::Left),
            source_chunk_reader.map(Either::Right),
            |_: &mut ()| futures::stream::PollNext::Left,
        );

        if need_backfill {
            #[for_await]
            'backfill_loop: for either in &mut backfill_stream {
                match either {
                    // Upstream
                    Either::Left(msg) => {
                        match msg? {
                            Message::Barrier(barrier) => {
                                // TODO: handle split change
                                // TODO: handle Mutation::Pause

                                self.backfill_state_store
                                    .set_states(backfill_states.clone())
                                    .await?;
                                self.backfill_state_store
                                    .state_store
                                    .commit(barrier.epoch)
                                    .await?;

                                yield Message::Barrier(barrier);

                                if backfill_states
                                    .values()
                                    .all(|state| matches!(state, BackfillState::Finished))
                                {
                                    // all splits finished backfilling
                                    self.backfill_state_store
                                        .set_states(backfill_states.clone())
                                        .await?;
                                    break 'backfill_loop;
                                }
                            }
                            Message::Chunk(chunk) => {
                                // We need to iterate over all rows because there might be multiple splits in a chunk.
                                // Note: We assume offset from the source is monotonically increasing for the algorithm to work correctly.
                                let mut new_vis = BitmapBuilder::zeroed(chunk.visibility().len());

                                for (i, (_, row)) in chunk.rows().enumerate() {
                                    tracing::debug!(row = %row.display());
                                    let split = row.datum_at(split_idx).unwrap().into_utf8();
                                    let offset = row.datum_at(offset_idx).unwrap().into_utf8();
                                    let backfill_state = backfill_states.get_mut(split).unwrap();
                                    let vis = backfill_state.handle_upstream_row(
                                        split,
                                        offset,
                                        &abort_handles,
                                    );
                                    new_vis.set(i, vis);
                                }
                                // emit chunk if vis is not empty. i.e., some splits finished backfilling.
                                let new_vis = new_vis.finish();
                                if new_vis.count_ones() != 0 {
                                    let new_chunk = chunk.clone_with_vis(new_vis);
                                    yield Message::Chunk(new_chunk);
                                }
                            }
                            Message::Watermark(_) => {
                                // Ignore watermark during backfill. (?)
                            }
                        }
                    }
                    // backfill
                    Either::Right(msg) => {
                        let chunk = msg?;
                        let split_offset_mapping =
                            get_split_offset_mapping_from_chunk(&chunk, split_idx, offset_idx)
                                .unwrap();
                        let _state: HashMap<_, _> = split_offset_mapping
                            .iter()
                            .flat_map(|(split_id, offset)| {
                                let origin_split_impl = self
                                    .stream_source_core
                                    .stream_source_splits
                                    .get_mut(split_id);

                                // update backfill progress
                                let prev_state = backfill_states.insert(
                                    split_id.to_string(),
                                    BackfillState::Backfilling(Some(offset.to_string())),
                                );
                                // abort_handles should prevents other cases happening
                                assert_matches!(
                                    prev_state,
                                    Some(BackfillState::Backfilling(_)),
                                    "Unexpected backfilling state, split_id: {split_id}"
                                );

                                origin_split_impl.map(|split_impl| {
                                    split_impl.update_in_place(offset.clone())?;
                                    Ok::<_, anyhow::Error>((split_id.clone(), split_impl.clone()))
                                })
                            })
                            .try_collect()?;

                        yield Message::Chunk(chunk);
                    }
                }
            }
        }

        // All splits finished backfilling. Now we only forward the source data.
        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Barrier(barrier) => {
                    // 
                    
                    // We might need to persist its state. Is is possible that we need to backfill?
                    yield Message::Barrier(barrier);
                }
                Message::Chunk(chunk) => {
                    yield Message::Chunk(chunk);
                }
                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
            }
        }
    }
}

fn compare_kafka_offset(a: Option<&String>, b: &str) -> Ordering {
    match a {
        Some(a) => {
            let a = a.parse::<i64>().unwrap();
            let b = b.parse::<i64>().unwrap();
            a.cmp(&b)
        }
        None => Ordering::Less,
    }
}

impl<S: StateStore> Executor for KafkaBackfillExecutorWrapper<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.inner.execute(self.input).boxed()
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

impl<S: StateStore> Debug for KafkaBackfillExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let core = &self.stream_source_core;
        f.debug_struct("KafkaBackfillExecutor")
            .field("source_id", &core.source_id)
            .field("column_ids", &core.column_ids)
            .field("pk_indices", &self.info.pk_indices)
            .finish()
    }
}
