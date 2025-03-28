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

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use anyhow::anyhow;
use either::Either;
use futures::stream::{select_with_strategy, PollNext};
use itertools::Itertools;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::metrics::{LabelGuardedIntCounter, GLOBAL_ERROR_METRICS};
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::types::JsonbVal;
use risingwave_connector::source::reader::desc::{SourceDesc, SourceDescBuilder};
use risingwave_connector::source::{
    BoxChunkSourceStream, SourceContext, SourceCtrlOpts, SplitId, SplitImpl, SplitMetaData,
};
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;

use super::executor_core::StreamSourceCore;
use super::source_backfill_state_table::BackfillStateTableHandler;
use super::{apply_rate_limit, get_split_offset_col_idx};
use crate::common::rate_limit::limited_chunk_size;
use crate::executor::prelude::*;
use crate::executor::source::source_executor::WAIT_BARRIER_MULTIPLE_TIMES;
use crate::executor::UpdateMutation;

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
    fn handle_upstream_row(&mut self, offset: &str) -> bool {
        let mut vis = false;
        match self {
            BackfillState::Backfilling(None) => {
                // backfilling for this split is not started yet. Ignore this row
            }
            BackfillState::Backfilling(Some(backfill_offset)) => {
                match compare_kafka_offset(backfill_offset, offset) {
                    Ordering::Less => {
                        // continue backfilling. Ignore this row
                    }
                    Ordering::Equal => {
                        // backfilling for this split is finished just right.
                        *self = BackfillState::Finished;
                    }
                    Ordering::Greater => {
                        // backfilling for this split produced more data than current source's progress.
                        // We should stop backfilling, and filter out rows from upstream with offset <= backfill_offset.
                        *self = BackfillState::SourceCachingUp(backfill_offset.clone());
                    }
                }
            }
            BackfillState::SourceCachingUp(backfill_offset) => {
                match compare_kafka_offset(backfill_offset, offset) {
                    Ordering::Less => {
                        // Source caught up, but doesn't contain the last backfilled row.
                        // This may happen e.g., if Kafka performed compaction.
                        vis = true;
                        *self = BackfillState::Finished;
                    }
                    Ordering::Equal => {
                        // Source just caught up with backfilling.
                        *self = BackfillState::Finished;
                    }
                    Ordering::Greater => {
                        // Source is still behind backfilling.
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

pub struct SourceBackfillExecutor<S: StateStore> {
    pub inner: SourceBackfillExecutorInner<S>,
    /// Upstream changelog stream which may contain metadata columns, e.g. `_rw_offset`
    pub input: Executor,
}

pub struct SourceBackfillExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Streaming source for external
    // FIXME: some fields e.g. its state table is not used. We might need to refactor. Even latest_split_info is not used.
    stream_source_core: StreamSourceCore<S>,
    backfill_state_store: BackfillStateTableHandler<S>,

    /// Metrics for monitor.
    metrics: Arc<StreamingMetrics>,
    source_split_change_count: LabelGuardedIntCounter<4>,

    // /// Receiver of barrier channel.
    // barrier_receiver: Option<UnboundedReceiver<Barrier>>,
    /// System parameter reader to read barrier interval
    system_params: SystemParamsReaderRef,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,
}

/// Local variables used in the backfill stage.
struct BackfillStage {
    // stream: Option<EitherStream<'a>>,
    states: BackfillStates,
    /// Note: the offsets are not updated. Should use `state`'s offset to update before using it.
    unfinished_splits: Vec<SplitImpl>,
}

impl BackfillStage {
    /// Get unfinished splits with latest offsets according to the backfill states.
    fn get_latest_unfinished_splits(&mut self) -> StreamExecutorResult<&Vec<SplitImpl>> {
        let mut unfinished_splits = Vec::new();
        for split in &mut self.unfinished_splits {
            let state = self.states.get(split.id().as_ref()).unwrap();
            match state {
                BackfillState::Backfilling(Some(offset)) => {
                    split.update_in_place(offset.clone())?;
                    unfinished_splits.push(split.clone());
                }
                BackfillState::Backfilling(None) => unfinished_splits.push(split.clone()),
                BackfillState::SourceCachingUp(_) | BackfillState::Finished => {}
            }
        }
        self.unfinished_splits = unfinished_splits;
        Ok(&self.unfinished_splits)
    }
}

impl<S: StateStore> SourceBackfillExecutorInner<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        info: ExecutorInfo,
        stream_source_core: StreamSourceCore<S>,
        metrics: Arc<StreamingMetrics>,
        system_params: SystemParamsReaderRef,
        backfill_state_store: BackfillStateTableHandler<S>,
        rate_limit_rps: Option<u32>,
    ) -> Self {
        let source_split_change_count = metrics
            .source_split_change_count
            .with_guarded_label_values(&[
                &stream_source_core.source_id.to_string(),
                &stream_source_core.source_name,
                &actor_ctx.id.to_string(),
                &actor_ctx.fragment_id.to_string(),
            ]);
        Self {
            actor_ctx,
            info,
            stream_source_core,
            backfill_state_store,
            metrics,
            source_split_change_count,
            system_params,
            rate_limit_rps,
        }
    }

    async fn build_stream_source_reader(
        &self,
        source_desc: &SourceDesc,
        splits: Vec<SplitImpl>,
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
            None,
        );
        let stream = source_desc
            .source
            .build_stream(Some(splits), column_ids, Arc::new(source_ctx))
            .await
            .map_err(StreamExecutorError::connector_error)?;
        Ok(apply_rate_limit(stream, self.rate_limit_rps).boxed())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute(mut self, input: Executor) {
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

        let mut owned_splits = Vec::default();
        if let Some(splits) = barrier.initial_split_assignment(self.actor_ctx.id) {
            owned_splits = splits.to_vec();
        }

        self.backfill_state_store.init_epoch(barrier.epoch);

        let mut backfill_states: BackfillStates = HashMap::new();
        let mut unfinished_splits = Vec::new();
        for mut split in owned_splits {
            let split_id = split.id();
            let backfill_state = self
                .backfill_state_store
                .try_recover_from_state_store(&split_id)
                .await?;
            match backfill_state {
                None => {
                    backfill_states.insert(split_id, BackfillState::Backfilling(None));
                    unfinished_splits.push(split);
                }
                Some(backfill_state) => {
                    match backfill_state {
                        BackfillState::Backfilling(ref offset) => {
                            if let Some(offset) = offset {
                                split.update_in_place(offset.clone())?;
                            }
                            unfinished_splits.push(split);
                        }
                        BackfillState::SourceCachingUp(_) | BackfillState::Finished => {}
                    }
                    backfill_states.insert(split_id, backfill_state);
                }
            }
        }
        tracing::debug!(?backfill_states, "source backfill started");

        // Return the ownership of `stream_source_core` to the source executor.
        self.stream_source_core = core;

        let source_chunk_reader = self
            .build_stream_source_reader(&source_desc, unfinished_splits.clone())
            .instrument_await("source_build_reader")
            .await?;

        fn select_strategy(_: &mut ()) -> PollNext {
            futures::stream::PollNext::Left
        }

        // We choose "preferring upstream" strategy here, because:
        // - When the upstream source's workload is high (i.e., Kafka has new incoming data), it just makes backfilling slower.
        //   For chunks from upstream, they are simply dropped, so there's no much overhead.
        //   So possibly this can also affect other running jobs less.
        // - When the upstream Source's becomes less busy, SourceBackfill can begin to catch up.
        let mut backfill_stream = select_with_strategy(
            input.by_ref().map(Either::Left),
            source_chunk_reader.map(Either::Right),
            select_strategy,
        );

        type PausedReader = Option<impl Stream>;
        let mut paused_reader: PausedReader = None;

        macro_rules! pause_reader {
            () => {
                let (left, right) = backfill_stream.into_inner();
                backfill_stream = select_with_strategy(
                    left,
                    futures::stream::pending().boxed().map(Either::Right),
                    select_strategy,
                );
                // XXX: do we have to store the original reader? Can we simply rebuild the reader later?
                paused_reader = Some(right);
            };
        }

        // If the first barrier requires us to pause on startup, pause the stream.
        if barrier.is_pause_on_startup() {
            pause_reader!();
        }

        yield Message::Barrier(barrier);

        let mut backfill_stage = BackfillStage {
            states: backfill_states,
            unfinished_splits,
        };

        if !self.backfill_finished(&backfill_stage.states).await? {
            let source_backfill_row_count = self
                .metrics
                .source_backfill_row_count
                .with_guarded_label_values(&[
                    &self.stream_source_core.source_id.to_string(),
                    &self.stream_source_core.source_name,
                    &self.actor_ctx.id.to_string(),
                    &self.actor_ctx.fragment_id.to_string(),
                ]);

            // We allow data to flow for `WAIT_BARRIER_MULTIPLE_TIMES` * `expected_barrier_latency_ms`
            // milliseconds, considering some other latencies like network and cost in Meta.
            let mut max_wait_barrier_time_ms = self.system_params.load().barrier_interval_ms()
                as u128
                * WAIT_BARRIER_MULTIPLE_TIMES;
            let mut last_barrier_time = Instant::now();
            let mut self_paused = false;

            'backfill_loop: while let Some(either) = backfill_stream.next().await {
                match either {
                    // Upstream
                    Either::Left(msg) => {
                        let Ok(msg) = msg else {
                            let e = msg.unwrap_err();
                            let core = &self.stream_source_core;
                            tracing::warn!(
                                error = %e.as_report(),
                                source_id = %core.source_id,
                                "stream source reader error",
                            );
                            GLOBAL_ERROR_METRICS.user_source_error.report([
                                "SourceReaderError".to_owned(),
                                core.source_id.to_string(),
                                core.source_name.to_owned(),
                                self.actor_ctx.fragment_id.to_string(),
                            ]);

                            let reader = self
                                .build_stream_source_reader(
                                    &source_desc,
                                    backfill_stage.get_latest_unfinished_splits()?.clone(),
                                )
                                .await?;

                            backfill_stream = select_with_strategy(
                                input.by_ref().map(Either::Left),
                                reader.map(Either::Right),
                                select_strategy,
                            );
                            continue;
                        };
                        match msg {
                            Message::Barrier(barrier) => {
                                last_barrier_time = Instant::now();

                                if self_paused {
                                    backfill_stream = select_with_strategy(
                                        input.by_ref().map(Either::Left),
                                        paused_reader.take().expect("no paused reader to resume"),
                                        select_strategy,
                                    );
                                    self_paused = false;
                                }

                                let mut split_changed = false;
                                if let Some(ref mutation) = barrier.mutation.as_deref() {
                                    match mutation {
                                        Mutation::Pause => {
                                            pause_reader!();
                                        }
                                        Mutation::Resume => {
                                            backfill_stream = select_with_strategy(
                                                input.by_ref().map(Either::Left),
                                                paused_reader
                                                    .take()
                                                    .expect("no paused reader to resume"),
                                                select_strategy,
                                            );
                                        }
                                        Mutation::SourceChangeSplit(actor_splits) => {
                                            tracing::info!(
                                                actor_splits = ?actor_splits,
                                                "source change split received"
                                            );
                                            split_changed = self
                                                .apply_split_change(
                                                    actor_splits,
                                                    &mut backfill_stage,
                                                    true,
                                                )
                                                .await?;
                                        }
                                        Mutation::Update(UpdateMutation {
                                            actor_splits, ..
                                        }) => {
                                            split_changed = self
                                                .apply_split_change(
                                                    actor_splits,
                                                    &mut backfill_stage,
                                                    false,
                                                )
                                                .await?;
                                        }
                                        _ => {}
                                    }
                                }
                                if split_changed {
                                    // rebuild backfill_stream
                                    // Note: we don't put this part in a method, due to some complex lifetime issues.

                                    let latest_unfinished_splits =
                                        backfill_stage.get_latest_unfinished_splits()?;
                                    tracing::info!(
                                        "actor {:?} apply source split change to {:?}",
                                        self.actor_ctx.id,
                                        latest_unfinished_splits
                                    );

                                    // Replace the source reader with a new one of the new state.
                                    let reader = self
                                        .build_stream_source_reader(
                                            &source_desc,
                                            latest_unfinished_splits.clone(),
                                        )
                                        .await?;

                                    backfill_stream = select_with_strategy(
                                        input.by_ref().map(Either::Left),
                                        reader.map(Either::Right),
                                        select_strategy,
                                    );
                                }

                                self.backfill_state_store
                                    .set_states(backfill_stage.states.clone())
                                    .await?;
                                self.backfill_state_store
                                    .state_store
                                    .commit(barrier.epoch)
                                    .await?;

                                yield Message::Barrier(barrier);

                                if self.backfill_finished(&backfill_stage.states).await? {
                                    break 'backfill_loop;
                                }
                            }
                            Message::Chunk(chunk) => {
                                // We need to iterate over all rows because there might be multiple splits in a chunk.
                                // Note: We assume offset from the source is monotonically increasing for the algorithm to work correctly.
                                let mut new_vis = BitmapBuilder::zeroed(chunk.visibility().len());

                                for (i, (_, row)) in chunk.rows().enumerate() {
                                    let split = row.datum_at(split_idx).unwrap().into_utf8();
                                    let offset = row.datum_at(offset_idx).unwrap().into_utf8();
                                    let backfill_state =
                                        backfill_stage.states.get_mut(split).unwrap();
                                    let vis = backfill_state.handle_upstream_row(offset);
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

                        if last_barrier_time.elapsed().as_millis() > max_wait_barrier_time_ms {
                            // Exceeds the max wait barrier time, the source will be paused.
                            // Currently we can guarantee the
                            // source is not paused since it received stream
                            // chunks.
                            self_paused = true;
                            tracing::warn!(
                                "source {} paused, wait barrier for {:?}",
                                self.info.identity,
                                last_barrier_time.elapsed()
                            );
                            pause_reader!();

                            // Only update `max_wait_barrier_time_ms` to capture
                            // `barrier_interval_ms`
                            // changes here to avoid frequently accessing the shared
                            // `system_params`.
                            max_wait_barrier_time_ms =
                                self.system_params.load().barrier_interval_ms() as u128
                                    * WAIT_BARRIER_MULTIPLE_TIMES;
                        }
                        // TODO(optimize): actually each msg is from one split. We can
                        // include split from the message and avoid iterating over all rows.
                        let mut new_vis = BitmapBuilder::zeroed(chunk.visibility().len());

                        for (i, (_, row)) in chunk.rows().enumerate() {
                            let split_id: Arc<str> =
                                row.datum_at(split_idx).unwrap().into_utf8().into();
                            let offset: String =
                                row.datum_at(offset_idx).unwrap().into_utf8().into();
                            // update backfill progress
                            let mut vis = true;
                            match backfill_stage.states.entry(split_id.clone()) {
                                Entry::Occupied(mut entry) => {
                                    let state = entry.get_mut();
                                    match state {
                                        BackfillState::Backfilling(_) => {
                                            *state =
                                                BackfillState::Backfilling(Some(offset.clone()));
                                        }
                                        BackfillState::SourceCachingUp(_)
                                        | BackfillState::Finished => {
                                            // backfilling stopped. ignore
                                            vis = false
                                        }
                                    }
                                }
                                Entry::Vacant(entry) => {
                                    entry.insert(BackfillState::Backfilling(Some(offset.clone())));
                                }
                            }
                            new_vis.set(i, vis);
                        }

                        let new_vis = new_vis.finish();
                        let card = new_vis.count_ones();
                        if card != 0 {
                            let new_chunk = chunk.clone_with_vis(new_vis);
                            yield Message::Chunk(new_chunk);
                            source_backfill_row_count.inc_by(card as u64);
                        }
                    }
                }
            }
        }

        let mut splits: HashSet<SplitId> = backfill_stage.states.keys().cloned().collect();

        // All splits finished backfilling. Now we only forward the source data.
        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Barrier(barrier) => {
                    if let Some(ref mutation) = barrier.mutation.as_deref() {
                        match mutation {
                            Mutation::Pause | Mutation::Resume => {
                                // We don't need to do anything. Handled by upstream.
                            }
                            Mutation::SourceChangeSplit(actor_splits) => {
                                tracing::info!(
                                    actor_splits = ?actor_splits,
                                    "source change split received"
                                );
                                self.apply_split_change_forward_stage(
                                    actor_splits,
                                    &mut splits,
                                    true,
                                )
                                .await?;
                            }
                            Mutation::Update(UpdateMutation { actor_splits, .. }) => {
                                self.apply_split_change_forward_stage(
                                    actor_splits,
                                    &mut splits,
                                    true,
                                )
                                .await?;
                            }
                            _ => {}
                        }
                    }
                    self.backfill_state_store
                        .state_store
                        .commit(barrier.epoch)
                        .await?;
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
    async fn apply_split_change(
        &mut self,
        split_assignment: &HashMap<ActorId, Vec<SplitImpl>>,
        stage: &mut BackfillStage,
        should_trim_state: bool,
    ) -> StreamExecutorResult<bool> {
        self.source_split_change_count.inc();
        if let Some(target_splits) = split_assignment.get(&self.actor_ctx.id).cloned() {
            if self
                .update_state_if_changed(target_splits, stage, should_trim_state)
                .await?
            {
                // Note: we don't rebuild backfill_stream here, due to some complex lifetime issues.
                return Ok(true);
            }
        }

        Ok(false)
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
        for (split_id, mut split) in target_splits {
            if let Some(s) = stage.states.get(&split_id) {
                target_state.insert(split_id, s.clone());
            } else {
                split_changed = true;

                let backfill_state = self
                    .backfill_state_store
                    .try_recover_from_state_store(&split_id)
                    .await?;
                match backfill_state {
                    None => {
                        // Newly added split. We don't need to backfill.
                        target_state.insert(split_id, BackfillState::Finished);
                    }
                    Some(backfill_state) => {
                        // Migrated split. Backfill if unfinished.
                        match backfill_state {
                            BackfillState::Backfilling(ref offset) => {
                                if let Some(offset) = offset {
                                    split.update_in_place(offset.clone())?;
                                }
                                stage.unfinished_splits.push(split);
                            }
                            BackfillState::SourceCachingUp(_) | BackfillState::Finished => {}
                        }
                        target_state.insert(split_id, backfill_state);
                    }
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
            stage
                .unfinished_splits
                .retain(|split| target_state.contains_key(split.id().as_ref()));

            let dropped_splits = stage
                .states
                .extract_if(|split_id, _| !target_state.contains_key(split_id))
                .map(|(split_id, _)| split_id);

            if should_trim_state {
                // trim dropped splits' state
                self.backfill_state_store.trim_state(dropped_splits).await?;
            }
            tracing::info!(old_state=?stage.states, new_state=?target_state, "finish split change");
            stage.states = target_state;
        }

        Ok(split_changed)
    }

    /// For split change during forward stage, all newly added splits should be already finished.
    // We just need to update the state store if necessary.
    async fn apply_split_change_forward_stage(
        &mut self,
        split_assignment: &HashMap<ActorId, Vec<SplitImpl>>,
        splits: &mut HashSet<SplitId>,
        should_trim_state: bool,
    ) -> StreamExecutorResult<()> {
        self.source_split_change_count.inc();
        if let Some(target_splits) = split_assignment.get(&self.actor_ctx.id).cloned() {
            self.update_state_if_changed_forward_stage(target_splits, splits, should_trim_state)
                .await?;
        }

        Ok(())
    }

    async fn update_state_if_changed_forward_stage(
        &mut self,
        target_splits: Vec<SplitImpl>,
        current_splits: &mut HashSet<SplitId>,
        should_trim_state: bool,
    ) -> StreamExecutorResult<()> {
        let target_splits: HashSet<SplitId> = target_splits
            .into_iter()
            .map(|split| (split.id()))
            .collect();

        let mut split_changed = false;

        // Checks added splits
        for split_id in &target_splits {
            if !current_splits.contains(split_id) {
                split_changed = true;

                let backfill_state = self
                    .backfill_state_store
                    .try_recover_from_state_store(split_id)
                    .await?;
                match backfill_state {
                    None => {
                        // Newly added split. We don't need to backfill!
                    }
                    Some(backfill_state) => {
                        // Migrated split. It should also be finished since we are in forwarding stage.
                        match backfill_state {
                            BackfillState::Finished => {}
                            _ => {
                                return Err(anyhow::anyhow!(
                                    "Unexpected backfill state: {:?}",
                                    backfill_state
                                )
                                .into());
                            }
                        }
                    }
                }
            }
        }

        // Checks dropped splits
        for existing_split_id in current_splits.iter() {
            if !target_splits.contains(existing_split_id) {
                tracing::info!("split dropping detected: {}", existing_split_id);
                split_changed = true;
            }
        }

        if split_changed {
            tracing::info!(
                target_splits = ?target_splits,
                "apply split change"
            );

            let dropped_splits =
                current_splits.extract_if(|split_id| !target_splits.contains(split_id));

            if should_trim_state {
                // trim dropped splits' state
                self.backfill_state_store.trim_state(dropped_splits).await?;
            }

            self.backfill_state_store
                .set_states(
                    target_splits
                        .into_iter()
                        .map(|split_id| (split_id, BackfillState::Finished))
                        .collect(),
                )
                .await?;
        }

        Ok(())
    }
}

fn compare_kafka_offset(a: &str, b: &str) -> Ordering {
    let a = a.parse::<i64>().unwrap();
    let b = b.parse::<i64>().unwrap();
    a.cmp(&b)
}

impl<S: StateStore> Execute for SourceBackfillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.inner.execute(self.input).boxed()
    }
}

impl<S: StateStore> Debug for SourceBackfillExecutorInner<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let core = &self.stream_source_core;
        f.debug_struct("SourceBackfillExecutor")
            .field("source_id", &core.source_id)
            .field("column_ids", &core.column_ids)
            .field("pk_indices", &self.info.pk_indices)
            .finish()
    }
}
