// Copyright 2025 RisingWave Labs
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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use anyhow::Context;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use risingwave_hummock_sdk::compact_task::ReportTask;
use risingwave_hummock_sdk::{CompactionGroupId, HummockContextId};
use risingwave_pb::hummock::compact_task::{TaskStatus, TaskType};
use risingwave_pb::hummock::subscribe_compaction_event_request::{
    Event as RequestEvent, HeartBeat, PullTask,
};
use risingwave_pb::hummock::subscribe_compaction_event_response::{
    Event as ResponseEvent, PullTaskAck,
};
use risingwave_pb::hummock::{CompactTaskProgress, SubscribeCompactionEventRequest};
use risingwave_pb::iceberg_compaction::SubscribeIcebergCompactionEventRequest;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::{
    Event as IcebergRequestEvent, PullTask as IcebergPullTask,
};
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::{
    Event as IcebergResponseEvent, PullTaskAck as IcebergPullTaskAck,
};
use rw_futures_util::pending_on_none;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot::{Receiver as OneShotReceiver, Sender};
use tokio::task::JoinHandle;
use tonic::Streaming;
use tracing::warn;

use super::init_selectors;
use crate::hummock::HummockManager;
use crate::hummock::compaction::CompactionSelector;
use crate::hummock::compactor_manager::Compactor;
use crate::hummock::error::{Error, Result};
use crate::hummock::sequence::next_compaction_task_id;
use crate::manager::MetaOpts;
use crate::manager::iceberg_compaction::IcebergCompactionManagerRef;
use crate::rpc::metrics::MetaMetrics;

const MAX_SKIP_TIMES: usize = 8;
const MAX_REPORT_COUNT: usize = 16;

#[async_trait::async_trait]
pub trait CompactionEventDispatcher: Send + Sync + 'static {
    type EventType: Send + Sync + 'static;

    async fn on_event_locally(&self, context_id: HummockContextId, event: Self::EventType) -> bool;

    async fn on_event_remotely(
        &self,
        context_id: HummockContextId,
        event: Self::EventType,
    ) -> Result<()>;

    fn should_forward(&self, event: &Self::EventType) -> bool;

    fn remove_compactor(&self, context_id: HummockContextId);
}

pub trait CompactorStreamEvent: Send + Sync + 'static {
    type EventType: Send + Sync + 'static;
    fn take_event(self) -> Self::EventType;
    fn create_at(&self) -> u64;
}

pub struct CompactionEventLoop<
    D: CompactionEventDispatcher<EventType = E::EventType>,
    E: CompactorStreamEvent,
> {
    hummock_compactor_dispatcher: D,
    metrics: Arc<MetaMetrics>,
    compactor_streams_change_rx: UnboundedReceiver<(HummockContextId, Streaming<E>)>,
}

pub type HummockCompactionEventLoop =
    CompactionEventLoop<HummockCompactionEventDispatcher, SubscribeCompactionEventRequest>;

pub type IcebergCompactionEventLoop =
    CompactionEventLoop<IcebergCompactionEventDispatcher, SubscribeIcebergCompactionEventRequest>;

pub struct HummockCompactionEventDispatcher {
    meta_opts: Arc<MetaOpts>,
    hummock_compaction_event_handler: HummockCompactionEventHandler,
    tx: Option<UnboundedSender<(HummockContextId, RequestEvent)>>,
}

#[async_trait::async_trait]
impl CompactionEventDispatcher for HummockCompactionEventDispatcher {
    type EventType = RequestEvent;

    fn should_forward(&self, event: &Self::EventType) -> bool {
        if self.tx.is_none() {
            return false;
        }

        matches!(event, RequestEvent::PullTask(_)) || matches!(event, RequestEvent::ReportTask(_))
    }

    async fn on_event_locally(&self, context_id: HummockContextId, event: Self::EventType) -> bool {
        let mut compactor_alive = true;
        match event {
            RequestEvent::HeartBeat(HeartBeat { progress }) => {
                compactor_alive = self
                    .hummock_compaction_event_handler
                    .handle_heartbeat(context_id, progress)
                    .await;
            }

            RequestEvent::Register(_event) => {
                unreachable!()
            }

            RequestEvent::PullTask(pull_task) => {
                compactor_alive = self
                    .hummock_compaction_event_handler
                    .handle_pull_task_event(
                        context_id,
                        pull_task.pull_task_count as usize,
                        &mut init_selectors(),
                        self.meta_opts.max_get_task_probe_times,
                    )
                    .await;
            }

            RequestEvent::ReportTask(report_event) => {
                if let Err(e) = self
                    .hummock_compaction_event_handler
                    .handle_report_task_event(vec![report_event.into()])
                    .await
                {
                    tracing::error!(error = %e.as_report(), "report compact_tack fail")
                }
            }
        }

        compactor_alive
    }

    async fn on_event_remotely(
        &self,
        context_id: HummockContextId,
        event: Self::EventType,
    ) -> Result<()> {
        if let Some(tx) = &self.tx {
            tx.send((context_id, event))
                .with_context(|| format!("Failed to send event to compactor {context_id}"))?;
        } else {
            unreachable!();
        }
        Ok(())
    }

    fn remove_compactor(&self, context_id: HummockContextId) {
        self.hummock_compaction_event_handler
            .hummock_manager
            .compactor_manager
            .remove_compactor(context_id);
    }
}

impl HummockCompactionEventDispatcher {
    pub fn new(
        meta_opts: Arc<MetaOpts>,
        hummock_compaction_event_handler: HummockCompactionEventHandler,
        tx: Option<UnboundedSender<(HummockContextId, RequestEvent)>>,
    ) -> Self {
        Self {
            meta_opts,
            hummock_compaction_event_handler,
            tx,
        }
    }
}

#[derive(Clone)]
pub struct HummockCompactionEventHandler {
    pub hummock_manager: Arc<HummockManager>,
}

impl HummockCompactionEventHandler {
    pub fn new(hummock_manager: Arc<HummockManager>) -> Self {
        Self { hummock_manager }
    }

    async fn handle_heartbeat(
        &self,
        context_id: HummockContextId,
        progress: Vec<CompactTaskProgress>,
    ) -> bool {
        let mut compactor_alive = true;
        let compactor_manager = self.hummock_manager.compactor_manager.clone();
        let (mut cancel_tasks, mut untracked_tasks) =
            compactor_manager.update_task_heartbeats(&progress);
        if !cancel_tasks.is_empty() {
            tracing::info!(
                ?cancel_tasks,
                %context_id,
                "Tasks cancel has expired due to lack of visible progress",
            );

            if let Err(e) = self
                .hummock_manager
                .cancel_compact_tasks(cancel_tasks.clone(), TaskStatus::HeartbeatProgressCanceled)
                .await
            {
                tracing::error!(
                    error = %e.as_report(),
                    "Attempt to remove compaction task due to elapsed heartbeat failed. We will continue to track its heartbeat
                    until we can successfully report its status."
                );
            }
        }

        // Untracked tasks have already been removed from meta. They only need a cancellation
        // event on the reporting compactor, without another attempt to remove their meta state.
        cancel_tasks.append(&mut untracked_tasks);

        match compactor_manager.get_compactor(context_id) {
            Some(compactor) => {
                // Forcefully cancel the task so that it terminates
                // early on the compactor
                // node.
                if !cancel_tasks.is_empty() {
                    let _ = compactor.cancel_tasks(&cancel_tasks);
                    tracing::info!(
                        ?cancel_tasks,
                        %context_id,
                        "CancelTask operation has been sent to compactor node",
                    );
                }
            }
            _ => {
                // Determine the validity of the compactor streaming rpc. When the compactor no longer exists in the manager, the stream will be removed.
                // Tip: Connectivity to the compactor will be determined through the `send_event` operation. When send fails, it will be removed from the manager
                compactor_alive = false;
            }
        }

        compactor_alive
    }

    async fn handle_pull_task_event(
        &self,
        context_id: HummockContextId,
        pull_task_count: usize,
        compaction_selectors: &mut HashMap<TaskType, Box<dyn CompactionSelector>>,
        max_get_task_probe_times: usize,
    ) -> bool {
        assert_ne!(0, pull_task_count);
        let Some(compactor) = self
            .hummock_manager
            .compactor_manager
            .get_compactor(context_id)
        else {
            return false;
        };

        // Task selection + dispatch (any early return inside is safe — PullTaskAck below is unreachable only if we return here)
        self.try_dispatch_tasks(
            &compactor,
            pull_task_count,
            compaction_selectors,
            max_get_task_probe_times,
        )
        .await;

        // PullTaskAck: structurally guaranteed to execute after try_dispatch_tasks returns
        if let Err(e) = compactor.send_event(ResponseEvent::PullTaskAck(PullTaskAck {})) {
            tracing::warn!(
                error = %e.as_report(),
                "Failed to send ack to {}",
                context_id,
            );
            return false;
        }

        true
    }

    /// Selects and dispatches compaction tasks to a compactor.
    ///
    /// Separated from `handle_pull_task_event` so that `PullTaskAck` cannot be
    /// accidentally skipped by early returns.
    async fn try_dispatch_tasks(
        &self,
        compactor: &Arc<Compactor>,
        pull_task_count: usize,
        compaction_selectors: &mut HashMap<TaskType, Box<dyn CompactionSelector>>,
        max_get_task_probe_times: usize,
    ) {
        let snapshot = self.hummock_manager.compaction_state.snapshot();
        let Some((groups, task_type)) = snapshot.pick_compaction_groups_and_type() else {
            return;
        };

        if let TaskType::Ttl = task_type {
            match self
                .hummock_manager
                .metadata_manager
                .get_all_table_options()
                .await
                .map_err(|err| Error::MetaStore(err.into()))
            {
                Ok(table_options) => {
                    self.hummock_manager
                        .update_table_id_to_table_option(table_options);
                }
                Err(err) => {
                    warn!(error = %err.as_report(), "Failed to get table options");
                }
            }
        }

        let selector: &mut Box<dyn CompactionSelector> =
            compaction_selectors.get_mut(&task_type).unwrap();

        let mut generated_task_count = 0;
        let mut existed_groups = groups.clone();
        let mut no_task_groups: HashSet<CompactionGroupId> = HashSet::default();
        let mut failed_tasks = vec![];
        let mut loop_times = 0;

        while generated_task_count < pull_task_count
            && failed_tasks.is_empty()
            && loop_times < max_get_task_probe_times
        {
            loop_times += 1;
            let compact_ret = self
                .hummock_manager
                .get_compact_tasks(
                    compactor.context_id(),
                    existed_groups.clone(),
                    pull_task_count - generated_task_count,
                    selector,
                )
                .await;

            match compact_ret {
                Ok((compact_tasks, unschedule_groups)) => {
                    no_task_groups.extend(unschedule_groups);
                    if compact_tasks.is_empty() {
                        break;
                    }
                    generated_task_count += compact_tasks.len();
                    for task in compact_tasks {
                        let task_id = task.task_id;
                        if let Err(e) =
                            compactor.send_event(ResponseEvent::CompactTask(task.into()))
                        {
                            tracing::warn!(
                                error = %e.as_report(),
                                "Failed to send task {} to {}",
                                task_id,
                                compactor.context_id(),
                            );
                            failed_tasks.push(task_id);
                        }
                    }
                    if !failed_tasks.is_empty() {
                        self.hummock_manager
                            .compactor_manager
                            .remove_compactor(compactor.context_id());
                    }
                    existed_groups.retain(|group_id| !no_task_groups.contains(group_id));
                }
                Err(err) => {
                    tracing::warn!(error = %err.as_report(), "Failed to get compaction task");
                    break;
                }
            };
        }
        for group in no_task_groups {
            self.hummock_manager.compaction_state.unschedule(
                group,
                task_type,
                snapshot.snapshot_time(),
            );
        }
        if let Err(err) = self
            .hummock_manager
            .cancel_compact_tasks(failed_tasks, TaskStatus::SendFailCanceled)
            .await
        {
            tracing::warn!(error = %err.as_report(), "Failed to cancel compaction task");
        }
    }

    async fn handle_report_task_event(&self, report_events: Vec<ReportTask>) -> Result<()> {
        if let Err(e) = self
            .hummock_manager
            .report_compact_tasks(report_events)
            .await
        {
            tracing::error!(error = %e.as_report(), "report compact_tack fail")
        }
        Ok(())
    }
}

impl<D: CompactionEventDispatcher<EventType = E::EventType>, E: CompactorStreamEvent>
    CompactionEventLoop<D, E>
{
    pub fn new(
        hummock_compactor_dispatcher: D,
        metrics: Arc<MetaMetrics>,
        compactor_streams_change_rx: UnboundedReceiver<(HummockContextId, Streaming<E>)>,
    ) -> Self {
        Self {
            hummock_compactor_dispatcher,
            metrics,
            compactor_streams_change_rx,
        }
    }

    pub fn run(mut self) -> (JoinHandle<()>, Sender<()>) {
        let mut compactor_request_streams = FuturesUnordered::new();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let shutdown_rx_shared = shutdown_rx.shared();

        let join_handle = tokio::spawn(async move {
            let push_stream =
                |context_id: HummockContextId,
                 stream: Streaming<E>,
                 compactor_request_streams: &mut FuturesUnordered<_>| {
                    let future = StreamExt::into_future(stream)
                        .map(move |stream_future| (context_id, stream_future));

                    compactor_request_streams.push(future);
                };

            let mut event_loop_iteration_now = Instant::now();

            loop {
                let shutdown_rx_shared = shutdown_rx_shared.clone();
                self.metrics
                    .compaction_event_loop_iteration_latency
                    .observe(event_loop_iteration_now.elapsed().as_millis() as _);
                event_loop_iteration_now = Instant::now();

                tokio::select! {
                    _ = shutdown_rx_shared => { return; },

                    compactor_stream = self.compactor_streams_change_rx.recv() => {
                        if let Some((context_id, stream)) = compactor_stream {
                            tracing::info!("compactor {} enters the cluster", context_id);
                            push_stream(context_id, stream, &mut compactor_request_streams);
                        }
                    },

                    result = pending_on_none(compactor_request_streams.next()) => {
                        let (context_id, compactor_stream_req): (_, (std::option::Option<std::result::Result<E, _>>, _)) = result;
                        let (event, create_at, stream) = match compactor_stream_req {
                            (Some(Ok(req)), stream) => {
                                let create_at = req.create_at();
                                let event  = req.take_event();
                                (event, create_at, stream)
                            }

                            (Some(Err(err)), _stream) => {
                                tracing::warn!(error = %err.as_report(), %context_id, "compactor stream poll with err, recv stream may be destroyed");
                                continue
                            }

                            _ => {
                                // remove compactor from compactor manager
                                tracing::warn!(%context_id, "compactor stream poll err, recv stream may be destroyed");
                                self.hummock_compactor_dispatcher.remove_compactor(context_id);
                                continue
                            },
                        };

                        {
                            let consumed_latency_ms = SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .expect("Clock may have gone backwards")
                                .as_millis()
                                as u64
                            - create_at;
                            self.metrics
                                .compaction_event_consumed_latency
                                .observe(consumed_latency_ms as _);
                        }

                        let mut compactor_alive = true;
                        if self
                            .hummock_compactor_dispatcher
                            .should_forward(&event)
                        {
                            if let Err(e) = self
                                .hummock_compactor_dispatcher
                                .on_event_remotely(context_id, event)
                                .await
                            {
                                tracing::warn!(error = %e.as_report(), "Failed to forward event");
                            }
                        } else {
                            compactor_alive = self.hummock_compactor_dispatcher.on_event_locally(
                                context_id,
                                event,
                            ).await;
                        }

                        if compactor_alive {
                            push_stream(context_id, stream, &mut compactor_request_streams);
                        } else {
                            tracing::warn!(%context_id, "compactor stream error, send stream may be destroyed");
                            self
                            .hummock_compactor_dispatcher
                            .remove_compactor(context_id);
                        }
                    },
                }
            }
        });

        (join_handle, shutdown_tx)
    }
}

impl CompactorStreamEvent for SubscribeCompactionEventRequest {
    type EventType = RequestEvent;

    fn take_event(self) -> Self::EventType {
        self.event.unwrap()
    }

    fn create_at(&self) -> u64 {
        self.create_at
    }
}

pub struct HummockCompactorDedicatedEventLoop {
    hummock_manager: Arc<HummockManager>,
    hummock_compaction_event_handler: HummockCompactionEventHandler,
}

impl HummockCompactorDedicatedEventLoop {
    pub fn new(
        hummock_manager: Arc<HummockManager>,
        hummock_compaction_event_handler: HummockCompactionEventHandler,
    ) -> Self {
        Self {
            hummock_manager,
            hummock_compaction_event_handler,
        }
    }

    /// dedicated event runtime for CPU/IO bound event
    async fn compact_task_dedicated_event_handler(
        &self,
        mut rx: UnboundedReceiver<(HummockContextId, RequestEvent)>,
        shutdown_rx: OneShotReceiver<()>,
    ) {
        let mut compaction_selectors = init_selectors();

        tokio::select! {
            _ = shutdown_rx => {}

            _ = async {
                while let Some((context_id, event)) = rx.recv().await {
                    let mut report_events = vec![];
                    let mut skip_times = 0;
                    match event {
                        RequestEvent::PullTask(PullTask { pull_task_count }) => {
                            self.hummock_compaction_event_handler.handle_pull_task_event(context_id, pull_task_count as usize, &mut compaction_selectors, self.hummock_manager.env.opts.max_get_task_probe_times).await;
                        }

                        RequestEvent::ReportTask(task) => {
                           report_events.push(task.into());
                        }

                        _ => unreachable!(),
                    }
                    while let Ok((context_id, event)) = rx.try_recv() {
                        match event {
                            RequestEvent::PullTask(PullTask { pull_task_count }) => {
                                self.hummock_compaction_event_handler.handle_pull_task_event(context_id, pull_task_count as usize, &mut compaction_selectors, self.hummock_manager.env.opts.max_get_task_probe_times).await;
                                if !report_events.is_empty() {
                                    if skip_times > MAX_SKIP_TIMES {
                                        break;
                                    }
                                    skip_times += 1;
                                }
                            }

                            RequestEvent::ReportTask(task) => {
                                report_events.push(task.into());
                                if report_events.len() >= MAX_REPORT_COUNT {
                                    break;
                                }
                            }
                        _ => unreachable!(),
                        }
                    }
                    if !report_events.is_empty()
                        && let Err(e) = self.hummock_compaction_event_handler.handle_report_task_event(report_events).await
                        {
                            tracing::error!(error = %e.as_report(), "report compact_tack fail")
                        }
                }
            } => {}
        }
    }

    pub fn run(
        self,
    ) -> (
        JoinHandle<()>,
        UnboundedSender<(HummockContextId, RequestEvent)>,
        Sender<()>,
    ) {
        let (tx, rx) = unbounded_channel();
        let (shutdon_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handler = tokio::spawn(async move {
            self.compact_task_dedicated_event_handler(rx, shutdown_rx)
                .await;
        });
        (join_handler, tx, shutdon_tx)
    }
}

pub struct IcebergCompactionEventHandler {
    compaction_manager: IcebergCompactionManagerRef,
}

impl IcebergCompactionEventHandler {
    pub fn new(compaction_manager: IcebergCompactionManagerRef) -> Self {
        Self { compaction_manager }
    }

    async fn handle_pull_task_event(
        &self,
        context_id: HummockContextId,
        pull_task_count: usize,
    ) -> bool {
        assert_ne!(0, pull_task_count);
        if let Some(compactor) = self
            .compaction_manager
            .iceberg_compactor_manager
            .get_compactor(context_id)
        {
            let mut compactor_alive = true;

            let iceberg_compaction_handles = self
                .compaction_manager
                .get_top_n_iceberg_commit_sink_ids(pull_task_count)
                .await;

            for handle in iceberg_compaction_handles {
                let compactor = compactor.clone();
                // send iceberg commit task to compactor
                if let Err(e) = async move {
                    handle
                        .send_compact_task(
                            compactor,
                            next_compaction_task_id(&self.compaction_manager.env).await?,
                        )
                        .await
                }
                .await
                {
                    tracing::warn!(
                        error = %e.as_report(),
                        "Failed to send iceberg commit task to {}",
                        context_id,
                    );
                    compactor_alive = false;
                }
            }

            if let Err(e) =
                compactor.send_event(IcebergResponseEvent::PullTaskAck(IcebergPullTaskAck {}))
            {
                tracing::warn!(
                    error = %e.as_report(),
                    "Failed to send ask to {}",
                    context_id,
                );
                compactor_alive = false;
            }

            return compactor_alive;
        }

        false
    }
}

pub struct IcebergCompactionEventDispatcher {
    compaction_event_handler: IcebergCompactionEventHandler,
}

#[async_trait::async_trait]
impl CompactionEventDispatcher for IcebergCompactionEventDispatcher {
    type EventType = IcebergRequestEvent;

    async fn on_event_locally(&self, context_id: HummockContextId, event: Self::EventType) -> bool {
        match event {
            IcebergRequestEvent::PullTask(IcebergPullTask { pull_task_count }) => {
                return self
                    .compaction_event_handler
                    .handle_pull_task_event(context_id, pull_task_count as usize)
                    .await;
            }
            _ => unreachable!(),
        }
    }

    async fn on_event_remotely(
        &self,
        _context_id: HummockContextId,
        _event: Self::EventType,
    ) -> Result<()> {
        unreachable!()
    }

    fn should_forward(&self, _event: &Self::EventType) -> bool {
        false
    }

    fn remove_compactor(&self, context_id: HummockContextId) {
        self.compaction_event_handler
            .compaction_manager
            .iceberg_compactor_manager
            .remove_compactor(context_id);
    }
}

impl IcebergCompactionEventDispatcher {
    pub fn new(compaction_event_handler: IcebergCompactionEventHandler) -> Self {
        Self {
            compaction_event_handler,
        }
    }
}

impl CompactorStreamEvent for SubscribeIcebergCompactionEventRequest {
    type EventType = IcebergRequestEvent;

    fn take_event(self) -> Self::EventType {
        self.event.unwrap()
    }

    fn create_at(&self) -> u64 {
        self.create_at
    }
}

#[cfg(test)]
mod scheduling_tests {
    use std::time::Duration;

    use risingwave_hummock_sdk::{LocalSstableInfo, SyncResult};
    use risingwave_meta_model::compaction_task;
    use risingwave_rpc_client::HummockMetaClient;
    use sea_orm::EntityTrait;

    use super::*;
    use crate::hummock::MockHummockMetaClient;
    use crate::hummock::compaction::selector::{
        ManualCompactionOption, default_compaction_selector,
    };
    use crate::hummock::manager::tests::{gen_sstable_info, setup_compute_env_with_meta_opts};

    async fn dispatch(manager: Arc<HummockManager>) -> Vec<risingwave_pb::hummock::CompactTask> {
        let mut receiver = manager.compactor_manager.add_compactor(99.into());
        let compactor = manager.compactor_manager.get_compactor(99.into()).unwrap();
        let mut selectors = HashMap::from([(TaskType::Dynamic, default_compaction_selector())]);
        HummockCompactionEventHandler::new(manager)
            .try_dispatch_tasks(&compactor, 8, &mut selectors, 1)
            .await;
        let mut tasks = vec![];
        while let Ok(event) = receiver.try_recv() {
            if let Some(ResponseEvent::CompactTask(task)) = event.unwrap().event {
                tasks.push(task);
            }
        }
        tasks
    }

    async fn fixture(deterministic: bool) -> (Arc<HummockManager>, MockHummockMetaClient) {
        let mut opts = MetaOpts::test(false);
        opts.compaction_deterministic_test = deterministic;
        let (_, manager, _, worker_id) = setup_compute_env_with_meta_opts(80, opts).await;
        manager
            .register_table_ids_for_test(&[(100, 2.into()), (101, 2.into())])
            .await
            .unwrap();
        let client = MockHummockMetaClient::new(manager.clone(), worker_id as _);
        commit_ssts(&client, 30, 1).await;
        (manager, client)
    }

    async fn commit_ssts(client: &MockHummockMetaClient, epoch: u64, first_sst: u64) {
        client
            .commit_epoch(
                risingwave_common::util::epoch::test_epoch(epoch),
                SyncResult {
                    uncommitted_ssts: (first_sst..first_sst + 4)
                        .map(|id| LocalSstableInfo {
                            sst_info: gen_sstable_info(
                                id,
                                vec![100, 101],
                                risingwave_common::util::epoch::test_epoch(epoch - 10),
                            ),
                            table_stats: Default::default(),
                            created_at: u64::MAX,
                        })
                        .collect(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    async fn assert_task_assignee(
        manager: &HummockManager,
        task_id: u64,
        context_id: HummockContextId,
    ) {
        let assignment = manager
            .compaction_task_from_assignment_for_test(task_id)
            .await
            .unwrap();
        assert_eq!(assignment.context_id, context_id);

        let model = compaction_task::Entity::find_by_id(task_id as i64)
            .one(&manager.env.meta_store_ref().conn)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(model.context_id, context_id);
        let protobuf_assignment: risingwave_pb::hummock::CompactTaskAssignment = model.into();
        assert_eq!(protobuf_assignment.context_id, context_id);

        let (_, assignments) = manager.list_compaction_status().await;
        let assignment = assignments
            .iter()
            .find(|a| a.compact_task.as_ref().unwrap().task_id == task_id)
            .unwrap();
        assert_eq!(assignment.context_id, context_id);
    }

    #[tokio::test]
    async fn test_assignment_records_compactor_for_pulled_tasks() {
        let (manager, _) = fixture(false).await;
        let tasks = dispatch(manager.clone()).await;
        assert!(!tasks.is_empty());
        for task in tasks {
            assert_task_assignee(&manager, task.task_id, 99.into()).await;
        }
    }

    #[tokio::test]
    async fn test_assignment_records_compactor_for_manual_tasks() {
        let (manager, _) = fixture(false).await;
        let context_id = 77.into();
        let mut receiver = manager.compactor_manager.add_compactor(context_id);
        let manual_manager = manager.clone();
        let trigger = tokio::spawn(async move {
            manual_manager
                .trigger_manual_compaction(
                    2.into(),
                    ManualCompactionOption {
                        level: 0,
                        ..Default::default()
                    },
                )
                .await
        });
        let response = tokio::time::timeout(Duration::from_secs(10), receiver.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Some(ResponseEvent::CompactTask(task)) = response.event else {
            panic!("expected a manual compaction task");
        };
        assert_task_assignee(&manager, task.task_id, context_id).await;
        assert!(
            manager
                .cancel_compact_task(task.task_id, TaskStatus::ManualCanceled)
                .await
                .unwrap()
        );
        trigger.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_manual_cancellation_notifies_owner_and_retries_on_heartbeat() {
        let (manager, _) = fixture(false).await;
        let context_id = 99.into();
        let mut receiver = manager.compactor_manager.add_compactor(context_id);
        let mut other_receiver = manager.compactor_manager.add_compactor(100.into());
        let task = manager
            .get_compact_task(context_id, 2.into(), &mut default_compaction_selector())
            .await
            .unwrap()
            .unwrap();
        let handler = HummockCompactionEventHandler::new(manager.clone());
        let progress = CompactTaskProgress {
            task_id: task.task_id,
            num_progress_key: 1,
            ..Default::default()
        };

        assert!(handler.handle_heartbeat(context_id, vec![progress]).await);
        assert!(receiver.try_recv().is_err());

        // This is the same cancellation path used by risectl. Meta no longer lists the task.
        assert!(
            manager
                .cancel_compact_task(task.task_id, TaskStatus::ManualCanceled)
                .await
                .unwrap()
        );
        // The request itself notifies the recorded owner without waiting for another heartbeat.
        let event = receiver.try_recv().unwrap().unwrap().event.unwrap();
        let ResponseEvent::CancelCompactTask(cancel) = event else {
            panic!("expected compactor cancellation, got {event:?}");
        };
        assert_eq!(cancel.task_id, task.task_id);
        assert_eq!(cancel.context_id, context_id);
        assert!(receiver.try_recv().is_err());
        assert!(other_receiver.try_recv().is_err());
        assert!(manager.compactor_manager.get_progress().is_empty());
        assert!(
            manager
                .compaction_task_from_assignment_for_test(task.task_id)
                .await
                .is_none()
        );
        let next_task = manager
            .get_compact_task(context_id, 2.into(), &mut default_compaction_selector())
            .await
            .unwrap()
            .unwrap();

        // Retry cancellation while the old task still runs, including after reconnecting.
        // A newly assigned task in the same heartbeat must remain active.
        for num_progress_key in 1..=3 {
            if num_progress_key == 3 {
                receiver = manager.compactor_manager.add_compactor(context_id);
            }
            assert!(
                handler
                    .handle_heartbeat(
                        context_id,
                        vec![
                            progress,
                            CompactTaskProgress {
                                task_id: next_task.task_id,
                                num_progress_key,
                                ..Default::default()
                            },
                        ],
                    )
                    .await
            );
            let event = receiver.try_recv().unwrap().unwrap().event.unwrap();
            let ResponseEvent::CancelCompactTask(cancel) = event else {
                panic!("expected compactor cancellation, got {event:?}");
            };
            assert_eq!(cancel.task_id, task.task_id);
            assert_eq!(cancel.context_id, context_id);
            assert!(receiver.try_recv().is_err());
            let tracked = manager.compactor_manager.get_progress();
            assert_eq!(tracked.len(), 1);
            assert_eq!(tracked[0].task_id, next_task.task_id);
        }

        // A late result cannot revive the canceled task or apply its output.
        assert!(
            !manager
                .report_compact_task(
                    task.task_id,
                    TaskStatus::Success,
                    vec![],
                    None,
                    HashMap::default(),
                )
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_batch_cancellation_notifies_each_owner_once() {
        let (manager, _) = fixture(false).await;
        let (left, mapping) = manager
            .move_state_tables_to_dedicated_compaction_group(2.into(), &[100.into()], None)
            .await
            .unwrap();
        let right = *mapping.keys().find(|&&id| id != left).unwrap();
        let left_context_id: HummockContextId = 77.into();
        let right_context_id: HummockContextId = 88.into();
        let mut left_receiver = manager.compactor_manager.add_compactor(left_context_id);
        let mut right_receiver = manager.compactor_manager.add_compactor(right_context_id);
        let left_task = manager
            .get_compact_task(left_context_id, left, &mut default_compaction_selector())
            .await
            .unwrap()
            .unwrap();
        let right_task = manager
            .get_compact_task(right_context_id, right, &mut default_compaction_selector())
            .await
            .unwrap()
            .unwrap();

        let results = manager
            .cancel_compact_tasks(
                vec![
                    left_task.task_id,
                    u64::MAX,
                    right_task.task_id,
                    left_task.task_id,
                ],
                TaskStatus::ManualCanceled,
            )
            .await
            .unwrap();
        assert_eq!(results, vec![true, false, true, false]);
        for (receiver, task_id, context_id) in [
            (&mut left_receiver, left_task.task_id, left_context_id),
            (&mut right_receiver, right_task.task_id, right_context_id),
        ] {
            let event = receiver.try_recv().unwrap().unwrap().event.unwrap();
            let ResponseEvent::CancelCompactTask(cancel) = event else {
                panic!("expected compactor cancellation, got {event:?}");
            };
            assert_eq!(cancel.task_id, task_id);
            assert_eq!(cancel.context_id, context_id);
            assert!(receiver.try_recv().is_err());
        }
        assert!(manager.compactor_manager.get_progress().is_empty());
    }

    #[tokio::test]
    async fn test_failed_cancellation_notification_retries_after_reconnect() {
        let (manager, _) = fixture(false).await;
        let context_id = 99.into();
        let receiver = manager.compactor_manager.add_compactor(context_id);
        let task = manager
            .get_compact_task(context_id, 2.into(), &mut default_compaction_selector())
            .await
            .unwrap()
            .unwrap();
        drop(receiver);

        // Losing the session must not prevent meta from committing the cancellation.
        assert!(
            manager
                .cancel_compact_task(task.task_id, TaskStatus::ManualCanceled)
                .await
                .unwrap()
        );
        assert!(manager.compactor_manager.get_progress().is_empty());

        let mut receiver = manager.compactor_manager.add_compactor(context_id);
        assert!(
            HummockCompactionEventHandler::new(manager)
                .handle_heartbeat(
                    context_id,
                    vec![CompactTaskProgress {
                        task_id: task.task_id,
                        ..Default::default()
                    }],
                )
                .await
        );
        let event = receiver.try_recv().unwrap().unwrap().event.unwrap();
        let ResponseEvent::CancelCompactTask(cancel) = event else {
            panic!("expected compactor cancellation, got {event:?}");
        };
        assert_eq!(cancel.task_id, task.task_id);
        assert_eq!(cancel.context_id, context_id);
        assert!(receiver.try_recv().is_err());
    }
}
