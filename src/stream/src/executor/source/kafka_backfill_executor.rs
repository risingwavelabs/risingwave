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

use std::assert_matches::assert_matches;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::fmt::Formatter;

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
    BoxChunkSourceStream, ConnectorState, SourceContext, SourceCtrlOpts, SplitId, SplitMetaData,
};
use risingwave_connector::ConnectorParams;
use risingwave_storage::StateStore;
use serde::{Deserialize, Serialize};

use super::executor_core::StreamSourceCore;
use super::kafka_backfill_state_table::BackfillStateTableHandler;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::*;

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
        abort_handles: &HashMap<SplitId, AbortHandle>,
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
                        abort_handles.get(split).unwrap().abort();
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

pub struct KafkaBackfillExecutor<S: StateStore> {
    pub inner: KafkaBackfillExecutorInner<S>,
    /// Upstream changelog stream which may contain metadata columns, e.g. `_rw_offset`
    pub input: Box<dyn Executor>,
}

pub struct KafkaBackfillExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Streaming source for external
    // FIXME: some fields e.g. its state table is not used. We might need to refactor. Even latest_split_info is not used.
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

/// Local variables used in the backfill stage.
struct BackfillStage {
    // stream: Option<EitherStream<'a>>,
    abort_handles: HashMap<SplitId, AbortHandle>,
    states: BackfillStates,
    /// Note: the offsets are not updated. Should use `state`'s offset to update before using it.
    unfinished_splits: Vec<SplitImpl>,
}

mod stream {
    use either::Either;
    use futures::stream::{select_with_strategy, BoxStream, PollNext, SelectWithStrategy};
    use futures::{Stream, StreamExt};
    use risingwave_common::array::StreamChunk;
    use risingwave_connector::source::BoxChunkSourceStream;

    use super::{BoxedMessageStream, MessageStreamItem};

    type EitherItem = Either<MessageStreamItem, anyhow::Result<StreamChunk>>;

    pub type EitherStream<'a> = SelectWithStrategy<
        impl Stream<Item = EitherItem>,
        impl Stream<Item = EitherItem>,
        impl FnMut(&mut ()) -> PollNext,
        (),
    >;

    pub fn build_combined_stream<'a>(
        upstream: &'a mut BoxedMessageStream,
        backfill: BoxChunkSourceStream,
    ) -> EitherStream<'a> {
        select_with_strategy(
            upstream.by_ref().map(Either::Left),
            backfill.map(Either::Right),
            |_: &mut ()| futures::stream::PollNext::Left,
        )
    }
}
use stream::{build_combined_stream, EitherStream};

impl<S: StateStore> KafkaBackfillExecutorInner<S> {
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
        splits: Vec<SplitImpl>,
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

        let mut abort_handles = HashMap::new();
        let mut streams = vec![];
        for split in splits {
            let split_id = split.id();
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
        Ok((futures::stream::select_all(streams).boxed(), abort_handles))
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
        let mut unfinished_splits = Vec::new();
        for ele in boot_state {
            let split_id = ele.id();
            let (split, backfill_state) = self
                .backfill_state_store
                .try_recover_from_state_store(ele)
                .await?;

            backfill_states.insert(split_id, backfill_state);
            if let Some(split) = split {
                unfinished_splits.push(split);
            }
        }
        tracing::debug!(?backfill_states, "source backfill started");

        // Return the ownership of `stream_source_core` to the source executor.
        self.stream_source_core = core;

        // If the first barrier requires us to pause on startup, pause the stream.
        if barrier.is_pause_on_startup() {
            // TODO: support pause on startup
        }

        yield Message::Barrier(barrier);

        if !backfill_finished(&backfill_states) {
            let (source_chunk_reader, abort_handles) = self
                .build_stream_source_reader(&source_desc, unfinished_splits.clone())
                .instrument_await("source_build_reader")
                .await?;

            // XXX:
            // - What's the best poll strategy?
            // - Should we also add a barrier stream for backfill executor?
            let mut backfill_stream =
                Some(build_combined_stream(&mut input, source_chunk_reader));
            let mut backfill_stage = BackfillStage {
                abort_handles,
                states: backfill_states,
                unfinished_splits,
            };

            'backfill_loop: while let Some(either) = backfill_stream.as_mut().unwrap().next().await
            {
                match either {
                    // Upstream
                    Either::Left(msg) => {
                        let Ok(msg) = msg else {
                            todo!("rebuild stream reader from error")
                        };
                        match msg {
                            Message::Barrier(barrier) => {
                                if let Some(ref mutation) = barrier.mutation.as_deref() {
                                    match mutation {
                                        Mutation::Pause => { // TODO:
                                        }
                                        Mutation::Resume => { // TODO:
                                        }
                                        Mutation::SourceChangeSplit(actor_splits) => {
                                            tracing::info!(
                                                actor_id = self.actor_ctx.id,
                                                actor_splits = ?actor_splits,
                                                "source change split received"
                                            );

                                            _ = backfill_stream.take();
                                            self.apply_split_change(
                                                &source_desc,
                                                actor_splits,
                                                &mut backfill_stage,
                                                &mut backfill_stream,
                                                &mut input,
                                                true,
                                            )
                                            .await?;
                                        }
                                        Mutation::Update(UpdateMutation {
                                            actor_splits, ..
                                        }) => {
                                            _ = backfill_stream.take();
                                            let _ = self
                                                .apply_split_change(
                                                    &source_desc,
                                                    actor_splits,
                                                    &mut backfill_stage,
                                                    &mut backfill_stream,
                                                    &mut input,
                                                    false,
                                                )
                                                .await?;
                                        }
                                        _ => {}
                                    }
                                }

                                self.backfill_state_store
                                    .set_states(backfill_stage.states.clone())
                                    .await?;
                                self.backfill_state_store
                                    .state_store
                                    .commit(barrier.epoch)
                                    .await?;

                                yield Message::Barrier(barrier);

                                if backfill_finished(&backfill_stage.states) {
                                    // all splits finished backfilling
                                    self.backfill_state_store
                                        .set_states(backfill_stage.states.clone())
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
                                    let backfill_state =
                                        backfill_stage.states.get_mut(split).unwrap();
                                    let vis = backfill_state.handle_upstream_row(
                                        split,
                                        offset,
                                        &backfill_stage.abort_handles,
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
                                // Ignore watermark during backfill.
                            }
                        }
                    }
                    // backfill
                    Either::Right(msg) => {
                        let chunk = msg?;
                        // TODO(optimize): actually each msg is from one split. We can
                        // include split from the message and avoid iterating over all rows.
                        let split_offset_mapping =
                            get_split_offset_mapping_from_chunk(&chunk, split_idx, offset_idx)
                                .unwrap();
                        split_offset_mapping.iter().for_each(|(split_id, offset)| {
                            // update backfill progress
                            let prev_state = backfill_stage.states.insert(
                                split_id.clone(),
                                BackfillState::Backfilling(Some(offset.to_string())),
                            );
                            // abort_handles should prevents other cases happening
                            assert_matches!(
                                prev_state,
                                Some(BackfillState::Backfilling(_)),
                                "Unexpected backfilling state, split_id: {split_id}"
                            );
                        });

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
                    // TODO: split change

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

    /// All splits finished backfilling.
    ///
    /// We check all splits for the source, including other actors' splits here, before going to the forward stage.
    /// Otherwise if we break early, but after rescheduling, an unfinished split is migrated to
    /// this actor, we still need to backfill it.
    async fn backfill_finished(&self, states: &BackfillStates) -> StreamExecutorResult<bool> {
        Ok(states
            .values()
            .all(|state| matches!(state, BackfillState::Finished))
            && self
                .backfill_state_store
                .scan()
                .await?
                .into_iter()
                .all(|state| matches!(state, BackfillState::Finished)))
    }

    /// For newly added splits, we do not need to backfill and can directly forward from upstream.
    async fn apply_split_change<'input>(
        &mut self,
        source_desc: &SourceDesc,
        split_assignment: &HashMap<ActorId, Vec<SplitImpl>>,
        stage: &mut BackfillStage,
        stream: &mut Option<EitherStream<'input>>,
        input: &'input mut BoxedMessageStream,
        should_trim_state: bool,
    ) -> StreamExecutorResult<()> {
        if let Some(target_splits) = split_assignment.get(&self.actor_ctx.id).cloned() {
            if self
                .update_state_if_changed(target_splits, stage, should_trim_state)
                .await?
            {
                self.rebuild_stream_reader(source_desc, stage, stream, input)
                    .await?;
            }
        }

        Ok(())
    }

    /// Returns `true` if split changed. Otherwise `false`.
    async fn update_state_if_changed(
        &mut self,
        target_splits: Vec<SplitImpl>,
        stage: &mut BackfillStage,
        should_trim_state: bool,
    ) -> StreamExecutorResult<bool> {
        let target_splits: HashMap<_, _> = target_splits
            .into_iter()
            .map(|split| (split.id(), split))
            .collect();

        let mut target_state: HashMap<SplitId, BackfillState> =
            HashMap::with_capacity(target_splits.len());

        let mut split_changed = false;

        // Checks added splits
        for (split_id, split) in target_splits {
            if let Some(s) = stage.states.get(&split_id) {
                target_state.insert(split_id, s.clone());
            } else {
                split_changed = true;

                let (split, backfill_state) = self
                    .backfill_state_store
                    .try_recover_from_state_store(split)
                    .await?;

                target_state.insert(split_id, backfill_state);
                if let Some(split) = split {
                    stage.unfinished_splits.push(split);
                }
            }
        }

        // Checks dropped splits
        for existing_split_id in stage.states.keys() {
            if !target_state.contains_key(existing_split_id) {
                tracing::info!("split dropping detected: {}", existing_split_id);
                split_changed = true;
            }
        }

        if split_changed {
            tracing::info!(
                actor_id = self.actor_ctx.id,
                state = ?target_state,
                "apply split change"
            );

            stage
                .unfinished_splits
                .retain(|split| target_state.get(split.id().as_ref()).is_some());

            let dropped_splits = stage
                .states
                .extract_if(|split_id, _| target_state.get(split_id).is_none())
                .map(|(split_id, _)| split_id);

            if should_trim_state {
                // trim dropped splits' state
                self.backfill_state_store.trim_state(dropped_splits).await?;
            }

            stage.states = target_state;
        }

        Ok(split_changed)
    }

    async fn rebuild_stream_reader<'input>(
        &mut self,
        source_desc: &SourceDesc,
        stage: &mut BackfillStage,
        stream: &mut Option<EitherStream<'input>>,
        input: &'input mut BoxedMessageStream,
    ) -> StreamExecutorResult<()> {
        let mut unfinished_splits = Vec::new();
        for split in &mut stage.unfinished_splits {
            let state = stage.states.get(split.id().as_ref()).unwrap();
            match state {
                BackfillState::Backfilling(Some(offset)) => {
                    split.update_in_place(offset.clone())?;
                    unfinished_splits.push(split.clone());
                }
                BackfillState::Backfilling(None)
                | BackfillState::SourceCachingUp(_)
                | BackfillState::Finished => {}
            }
        }
        stage.unfinished_splits = unfinished_splits;

        tracing::info!(
            "actor {:?} apply source split change to {:?}",
            self.actor_ctx.id,
            stage.unfinished_splits
        );

        // Replace the source reader with a new one of the new state.
        let (reader, new_abort_handles) = self
            .build_stream_source_reader(source_desc, stage.unfinished_splits.clone())
            .await?;
        stage.abort_handles = new_abort_handles;

        *stream = Some(build_combined_stream(input, reader));

        Ok(())
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

impl<S: StateStore> Executor for KafkaBackfillExecutor<S> {
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

impl<S: StateStore> Debug for KafkaBackfillExecutorInner<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let core = &self.stream_source_core;
        f.debug_struct("KafkaBackfillExecutor")
            .field("source_id", &core.source_id)
            .field("column_ids", &core.column_ids)
            .field("pk_indices", &self.info.pk_indices)
            .finish()
    }
}
