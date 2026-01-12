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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem::take;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::util::epoch::EpochPair;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::id::{ActorId, PartialGraphId, TableId, WorkerId};
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use risingwave_pb::stream_service::streaming_control_stream_response::{
    ResetPartialGraphResponse, Response,
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::barrier::context::GlobalBarrierWorkerContext;
use crate::barrier::info::BarrierInfo;
use crate::barrier::rpc::{ControlStreamManager, WorkerNodeEvent};
use crate::barrier::utils::{NodeToCollect, is_valid_after_worker_err};
use crate::manager::MetaSrvEnv;
use crate::model::StreamJobActorsToCreate;
use crate::{MetaError, MetaResult};

#[derive(Debug)]
struct PartialGraphInflightBarrier {
    epoch: EpochPair,
    node_to_collect: NodeToCollect,
    resps: HashMap<WorkerId, BarrierCompleteResponse>,
}

#[derive(Debug)]
struct PartialGraphRunningState {
    /// `prev_epoch` -> barrier
    inflight_barriers: BTreeMap<u64, PartialGraphInflightBarrier>,
}

impl PartialGraphRunningState {
    fn new() -> Self {
        Self {
            inflight_barriers: Default::default(),
        }
    }

    fn enqueue(&mut self, epoch: EpochPair, node_to_collect: NodeToCollect) {
        if let Some((last_prev_epoch, last_barrier)) = self.inflight_barriers.last_key_value() {
            assert_eq!(last_barrier.epoch.curr, epoch.prev);
            assert!(*last_prev_epoch < epoch.prev);
        }
        self.inflight_barriers
            .try_insert(
                epoch.prev,
                PartialGraphInflightBarrier {
                    epoch,
                    node_to_collect,
                    resps: Default::default(),
                },
            )
            .expect("non-duplicated");
    }

    fn collect(&mut self, resp: BarrierCompleteResponse) {
        debug!(
            epoch = resp.epoch,
            worker_id = %resp.worker_id,
            partial_graph_id = %resp.partial_graph_id,
            "collect barrier from worker"
        );
        let inflight_barrier = self
            .inflight_barriers
            .get_mut(&resp.epoch)
            .expect("should exist");
        assert!(inflight_barrier.node_to_collect.remove(&resp.worker_id));
        inflight_barrier
            .resps
            .try_insert(resp.worker_id, resp)
            .expect("non-duplicate");
    }

    fn barrier_collected(&mut self) -> Option<CollectedBarrier> {
        if let Some(entry) = self.inflight_barriers.first_entry()
            && entry.get().node_to_collect.is_empty()
        {
            let PartialGraphInflightBarrier { epoch, resps, .. } = entry.remove();
            Some(CollectedBarrier { epoch, resps })
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct ResetPartialGraphCollector {
    remaining_workers: HashSet<WorkerId>,
    reset_resps: HashMap<WorkerId, ResetPartialGraphResponse>,
}

impl ResetPartialGraphCollector {
    fn collect(&mut self, worker_id: WorkerId, resp: ResetPartialGraphResponse) -> bool {
        assert!(self.remaining_workers.remove(&worker_id));
        self.reset_resps
            .try_insert(worker_id, resp)
            .expect("non-duplicate");
        self.remaining_workers.is_empty()
    }
}

#[derive(Debug)]
enum PartialGraphStatus {
    Running(PartialGraphRunningState),
    Resetting(ResetPartialGraphCollector),
    Initializing {
        epoch: EpochPair,
        node_to_collect: NodeToCollect,
    },
}

impl PartialGraphStatus {
    fn collect(
        &mut self,
        worker_id: WorkerId,
        resp: BarrierCompleteResponse,
    ) -> Option<PartialGraphEvent> {
        assert_eq!(worker_id, resp.worker_id);
        match self {
            PartialGraphStatus::Running(state) => {
                state.collect(resp);
                state
                    .barrier_collected()
                    .map(PartialGraphEvent::BarrierCollected)
            }
            PartialGraphStatus::Resetting(_) => None,
            PartialGraphStatus::Initializing {
                epoch,
                node_to_collect,
            } => {
                assert_eq!(epoch.prev, resp.epoch);
                assert!(node_to_collect.remove(&worker_id));
                if node_to_collect.is_empty() {
                    *self = PartialGraphStatus::Running(PartialGraphRunningState::new());
                    Some(PartialGraphEvent::Initialized)
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct CollectedBarrier {
    pub epoch: EpochPair,
    pub resps: HashMap<WorkerId, BarrierCompleteResponse>,
}

pub(super) enum PartialGraphEvent {
    BarrierCollected(CollectedBarrier),
    Reset(HashMap<WorkerId, ResetPartialGraphResponse>),
    Initialized,
    Error(WorkerId),
}

pub(super) enum WorkerEvent {
    WorkerError {
        err: MetaError,
        affected_partial_graphs: HashSet<PartialGraphId>,
    },
    WorkerConnected,
}

fn existing_graphs(
    graphs: &HashMap<PartialGraphId, PartialGraphStatus>,
) -> impl Iterator<Item = PartialGraphId> + '_ {
    graphs
        .iter()
        .filter_map(|(partial_graph_id, status)| match status {
            PartialGraphStatus::Running(_) | PartialGraphStatus::Initializing { .. } => {
                Some(*partial_graph_id)
            }
            PartialGraphStatus::Resetting(_) => None,
        })
}

pub(super) struct PartialGraphManager {
    control_stream_manager: ControlStreamManager,
    term_id: String,
    graphs: HashMap<PartialGraphId, PartialGraphStatus>,
}

impl PartialGraphManager {
    pub(super) fn uninitialized(env: MetaSrvEnv) -> Self {
        Self {
            control_stream_manager: ControlStreamManager::new(env),
            term_id: "uninitialized".to_owned(),
            graphs: HashMap::new(),
        }
    }

    pub(super) async fn recover(
        env: MetaSrvEnv,
        nodes: &HashMap<WorkerId, WorkerNode>,
        context: Arc<impl GlobalBarrierWorkerContext>,
    ) -> Self {
        let term_id = Uuid::new_v4().to_string();
        let control_stream_manager =
            ControlStreamManager::recover(env, nodes, &term_id, context).await;
        Self {
            control_stream_manager,
            term_id,
            graphs: Default::default(),
        }
    }

    pub(super) fn control_stream_manager(&self) -> &ControlStreamManager {
        &self.control_stream_manager
    }

    pub(super) async fn add_worker(
        &mut self,
        node: WorkerNode,
        context: &impl GlobalBarrierWorkerContext,
    ) {
        self.control_stream_manager
            .add_worker(node, existing_graphs(&self.graphs), &self.term_id, context)
            .await
    }

    pub(super) fn remove_worker(&mut self, node: WorkerNode) {
        self.control_stream_manager.remove_worker(node);
    }

    pub(super) fn clear_worker(&mut self) {
        self.control_stream_manager.clear();
    }
}

#[must_use]
pub(super) struct PartialGraphAdder<'a> {
    partial_graph_id: PartialGraphId,
    manager: &'a mut PartialGraphManager,
    consumed: bool,
}

impl PartialGraphAdder<'_> {
    pub(super) fn added(mut self) {
        self.consumed = true;
    }

    pub(super) fn failed(mut self) {
        self.manager.reset_partial_graphs([self.partial_graph_id]);
        self.consumed = true;
    }

    pub(super) fn manager(&mut self) -> &mut PartialGraphManager {
        self.manager
    }
}

impl Drop for PartialGraphAdder<'_> {
    fn drop(&mut self) {
        debug_assert!(self.consumed, "unconsumed graph adder");
        if !self.consumed {
            warn!(partial_graph_id = %self.partial_graph_id, "unconsumed graph adder");
        }
    }
}

impl PartialGraphManager {
    pub(super) fn add_partial_graph(
        &mut self,
        partial_graph_id: PartialGraphId,
    ) -> PartialGraphAdder<'_> {
        self.graphs
            .try_insert(
                partial_graph_id,
                PartialGraphStatus::Running(PartialGraphRunningState::new()),
            )
            .expect("non-duplicated");
        self.control_stream_manager
            .add_partial_graph(partial_graph_id);
        PartialGraphAdder {
            partial_graph_id,
            manager: self,
            consumed: false,
        }
    }

    pub(super) fn remove_partial_graphs(&mut self, partial_graphs: Vec<PartialGraphId>) {
        for partial_graph_id in &partial_graphs {
            let graph = self.graphs.remove(partial_graph_id).expect("should exist");
            let PartialGraphStatus::Running(state) = graph else {
                panic!("graph to be explicitly removed should be running");
            };
            assert!(state.inflight_barriers.is_empty());
        }
        self.control_stream_manager
            .remove_partial_graphs(partial_graphs);
    }

    pub(super) fn reset_partial_graphs(
        &mut self,
        partial_graph_ids: impl IntoIterator<Item = PartialGraphId>,
    ) {
        let partial_graph_ids = partial_graph_ids.into_iter().collect_vec();
        let remaining_workers = self
            .control_stream_manager
            .reset_partial_graphs(partial_graph_ids.clone());
        let new_collector = || ResetPartialGraphCollector {
            remaining_workers: remaining_workers.clone(),
            reset_resps: Default::default(),
        };
        for partial_graph_id in partial_graph_ids {
            match self.graphs.entry(partial_graph_id) {
                Entry::Vacant(entry) => {
                    entry.insert(PartialGraphStatus::Resetting(new_collector()));
                }
                Entry::Occupied(mut entry) => {
                    let graph = entry.get_mut();
                    match graph {
                        PartialGraphStatus::Resetting(_) => {
                            unreachable!("should not reset again")
                        }
                        PartialGraphStatus::Running(_)
                        | PartialGraphStatus::Initializing { .. } => {
                            *graph = PartialGraphStatus::Resetting(new_collector());
                        }
                    }
                }
            }
        }
    }

    pub(super) fn assert_resetting(&self, partial_graph_id: PartialGraphId) {
        let graph = self.graphs.get(&partial_graph_id).expect("should exist");
        let PartialGraphStatus::Resetting(..) = graph else {
            panic!("should be at resetting but at {:?}", graph);
        };
    }

    pub(super) fn inject_barrier(
        &mut self,
        partial_graph_id: PartialGraphId,
        mutation: Option<Mutation>,
        barrier_info: &BarrierInfo,
        node_actors: &HashMap<WorkerId, HashSet<ActorId>>,
        table_ids_to_sync: impl Iterator<Item = TableId>,
        nodes_to_sync_table: impl Iterator<Item = WorkerId>,
        new_actors: Option<StreamJobActorsToCreate>,
    ) -> MetaResult<()> {
        let graph = self
            .graphs
            .get_mut(&partial_graph_id)
            .expect("should exist");
        let node_to_collect = self.control_stream_manager.inject_barrier(
            partial_graph_id,
            mutation,
            barrier_info,
            node_actors,
            table_ids_to_sync,
            nodes_to_sync_table,
            new_actors,
        )?;
        let PartialGraphStatus::Running(state) = graph else {
            panic!("should not inject barrier on non-running status: {graph:?}")
        };
        state.enqueue(barrier_info.epoch(), node_to_collect);
        Ok(())
    }

    pub(super) fn start_recover(&mut self) -> PartialGraphRecoverer<'_> {
        PartialGraphRecoverer {
            added_partial_graphs: Default::default(),
            manager: self,
            consumed: false,
        }
    }
}

#[must_use]
pub(super) struct PartialGraphRecoverer<'a> {
    added_partial_graphs: HashSet<PartialGraphId>,
    manager: &'a mut PartialGraphManager,
    consumed: bool,
}

impl PartialGraphRecoverer<'_> {
    pub(super) fn recover_graph(
        &mut self,
        partial_graph_id: PartialGraphId,
        mutation: Mutation,
        barrier_info: &BarrierInfo,
        node_actors: &HashMap<WorkerId, HashSet<ActorId>>,
        table_ids_to_sync: impl Iterator<Item = TableId>,
        new_actors: StreamJobActorsToCreate,
    ) -> MetaResult<()> {
        assert!(
            self.added_partial_graphs.insert(partial_graph_id),
            "duplicated recover graph {partial_graph_id}"
        );
        self.manager
            .control_stream_manager
            .add_partial_graph(partial_graph_id);
        assert!(barrier_info.kind.is_initial());
        let node_to_collect = self.manager.control_stream_manager.inject_barrier(
            partial_graph_id,
            Some(mutation),
            barrier_info,
            node_actors,
            table_ids_to_sync,
            node_actors.keys().copied(),
            Some(new_actors),
        )?;
        self.manager
            .graphs
            .try_insert(
                partial_graph_id,
                PartialGraphStatus::Initializing {
                    epoch: barrier_info.epoch(),
                    node_to_collect,
                },
            )
            .expect("non-duplicated");
        Ok(())
    }

    pub(super) fn control_stream_manager(&self) -> &ControlStreamManager {
        &self.manager.control_stream_manager
    }

    pub(super) fn all_initializing(mut self) -> HashSet<PartialGraphId> {
        self.consumed = true;
        take(&mut self.added_partial_graphs)
    }

    pub(super) fn failed(mut self) -> HashSet<PartialGraphId> {
        self.manager
            .reset_partial_graphs(self.added_partial_graphs.iter().copied());
        self.consumed = true;
        take(&mut self.added_partial_graphs)
    }
}

impl Drop for PartialGraphRecoverer<'_> {
    fn drop(&mut self) {
        debug_assert!(self.consumed, "unconsumed graph recoverer");
        if !self.consumed {
            warn!(partial_graph_ids = ?self.added_partial_graphs, "unconsumed graph recoverer");
        }
    }
}

#[must_use]
pub(super) enum PartialGraphManagerEvent {
    PartialGraph(PartialGraphId, PartialGraphEvent),
    Worker(WorkerId, WorkerEvent),
}

impl PartialGraphManager {
    pub(super) async fn next_event(
        &mut self,
        context: &Arc<impl GlobalBarrierWorkerContext>,
    ) -> PartialGraphManagerEvent {
        for (&partial_graph_id, graph) in &mut self.graphs {
            match graph {
                PartialGraphStatus::Running(state) => {
                    if let Some(collected) = state.barrier_collected() {
                        return PartialGraphManagerEvent::PartialGraph(
                            partial_graph_id,
                            PartialGraphEvent::BarrierCollected(collected),
                        );
                    }
                }
                PartialGraphStatus::Resetting(collector) => {
                    if collector.remaining_workers.is_empty() {
                        let resps = take(&mut collector.reset_resps);
                        self.graphs.remove(&partial_graph_id);
                        return PartialGraphManagerEvent::PartialGraph(
                            partial_graph_id,
                            PartialGraphEvent::Reset(resps),
                        );
                    }
                }
                PartialGraphStatus::Initializing {
                    node_to_collect, ..
                } => {
                    if node_to_collect.is_empty() {
                        *graph = PartialGraphStatus::Running(PartialGraphRunningState::new());
                        return PartialGraphManagerEvent::PartialGraph(
                            partial_graph_id,
                            PartialGraphEvent::Initialized,
                        );
                    }
                }
            }
        }
        loop {
            let (worker_id, event) = self
                .control_stream_manager
                .next_event(&self.term_id, context)
                .await;
            match event {
                WorkerNodeEvent::Response(result) => match result {
                    Ok(resp) => match resp {
                        Response::CompleteBarrier(resp) => {
                            let partial_graph_id = resp.partial_graph_id;
                            if let Some(event) = self
                                .graphs
                                .get_mut(&partial_graph_id)
                                .expect("should exist")
                                .collect(worker_id, resp)
                            {
                                return PartialGraphManagerEvent::PartialGraph(
                                    partial_graph_id,
                                    event,
                                );
                            }
                        }
                        Response::ReportPartialGraphFailure(resp) => {
                            let partial_graph_id = resp.partial_graph_id;
                            let graph = self
                                .graphs
                                .get_mut(&partial_graph_id)
                                .expect("should exist");
                            match graph {
                                PartialGraphStatus::Resetting(_) => {
                                    // ignore reported error when resetting
                                }
                                PartialGraphStatus::Running(_)
                                | PartialGraphStatus::Initializing { .. } => {
                                    return PartialGraphManagerEvent::PartialGraph(
                                        partial_graph_id,
                                        PartialGraphEvent::Error(worker_id),
                                    );
                                }
                            }
                        }
                        Response::ResetPartialGraph(resp) => {
                            let partial_graph_id = resp.partial_graph_id;
                            let graph = self
                                .graphs
                                .get_mut(&partial_graph_id)
                                .expect("should exist");
                            match graph {
                                PartialGraphStatus::Running(_)
                                | PartialGraphStatus::Initializing { .. } => {
                                    if cfg!(debug_assertions) {
                                        unreachable!(
                                            "should not have reset request when not in resetting state"
                                        )
                                    } else {
                                        warn!(
                                            ?resp,
                                            "ignore reset resp when not in Resetting state"
                                        );
                                    }
                                }
                                PartialGraphStatus::Resetting(collector) => {
                                    if collector.collect(worker_id, resp) {
                                        let resps = take(&mut collector.reset_resps);
                                        self.graphs.remove(&partial_graph_id);
                                        return PartialGraphManagerEvent::PartialGraph(
                                            partial_graph_id,
                                            PartialGraphEvent::Reset(resps),
                                        );
                                    }
                                }
                            }
                        }
                        Response::Init(_) | Response::Shutdown(_) => {
                            unreachable!("should be handled in control stream manager")
                        }
                    },
                    Err(error) => {
                        let affected_partial_graphs = self
                            .graphs
                            .iter_mut()
                            .filter_map(|(partial_graph_id, graph)| match graph {
                                PartialGraphStatus::Running(state) => state
                                    .inflight_barriers
                                    .values()
                                    .any(|inflight_barrier| {
                                        !is_valid_after_worker_err(
                                            &inflight_barrier.node_to_collect,
                                            worker_id,
                                        )
                                    })
                                    .then_some(*partial_graph_id),
                                PartialGraphStatus::Resetting(collector) => {
                                    collector.remaining_workers.remove(&worker_id);
                                    None
                                }
                                PartialGraphStatus::Initializing {
                                    node_to_collect, ..
                                } => (!is_valid_after_worker_err(node_to_collect, worker_id))
                                    .then_some(*partial_graph_id),
                            })
                            .collect();
                        return PartialGraphManagerEvent::Worker(
                            worker_id,
                            WorkerEvent::WorkerError {
                                err: error,
                                affected_partial_graphs,
                            },
                        );
                    }
                },
                WorkerNodeEvent::Connected(connected) => {
                    connected.initialize(existing_graphs(&self.graphs));
                    return PartialGraphManagerEvent::Worker(
                        worker_id,
                        WorkerEvent::WorkerConnected,
                    );
                }
            }
        }
    }
}
