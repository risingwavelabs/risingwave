// Copyright 2023 RisingWave Labs
// Licensed under the Apache License, Version 2.0 (the "License");
//
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

use std::collections::HashMap;
use std::default::Default;
use std::fmt::{Debug, Formatter};
use std::mem;
use std::sync::Arc;

use anyhow::anyhow;
use futures::executor::block_on;
use petgraph::dot::{Config, Dot};
use petgraph::Graph;
use pgwire::pg_server::SessionId;
use risingwave_common::array::DataChunk;
use risingwave_pb::batch_plan::{TaskId as TaskIdPb, TaskOutputId as TaskOutputIdPb};
use risingwave_pb::common::HostAddress;
use risingwave_rpc_client::ComputeClientPoolRef;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{oneshot, RwLock};
use tracing::{debug, error, info, warn};

use super::{DistributedQueryMetrics, QueryExecutionInfoRef, QueryResultFetcher, StageEvent};
use crate::catalog::catalog_service::CatalogReader;
use crate::scheduler::distributed::query::QueryMessage::Stage;
use crate::scheduler::distributed::stage::StageEvent::ScheduledRoot;
use crate::scheduler::distributed::StageEvent::Scheduled;
use crate::scheduler::distributed::StageExecution;
use crate::scheduler::plan_fragmenter::{Query, StageId, ROOT_TASK_ID, ROOT_TASK_OUTPUT_ID};
use crate::scheduler::worker_node_manager::WorkerNodeSelector;
use crate::scheduler::{
    ExecutionContextRef, PinnedHummockSnapshot, SchedulerError, SchedulerResult,
};

/// Message sent to a `QueryRunner` to control its execution.
#[derive(Debug)]
pub enum QueryMessage {
    /// Events passed running execution.
    Stage(StageEvent),
    CancelQuery,
}

enum QueryState {
    /// Not scheduled yet.
    ///
    /// We put `msg_receiver` in `Pending` state to avoid holding it in `QueryExecution`.
    Pending {
        msg_receiver: Receiver<QueryMessage>,
    },

    /// Running
    Running,

    /// Failed
    Failed,
}

pub struct QueryExecution {
    query: Arc<Query>,
    state: RwLock<QueryState>,
    shutdown_tx: Sender<QueryMessage>,
    /// Identified by process_id, secret_key. Query in the same session should have same key.
    pub session_id: SessionId,
}

struct QueryRunner {
    query: Arc<Query>,
    stage_executions: HashMap<StageId, Arc<StageExecution>>,
    scheduled_stages_count: usize,
    /// Query messages receiver. For example, stage state change events, query commands.
    msg_receiver: Receiver<QueryMessage>,

    /// Will be set to `None` after all stage scheduled.
    root_stage_sender: Option<oneshot::Sender<SchedulerResult<QueryResultFetcher>>>,

    // Used for cleaning up `QueryExecution` after execution.
    query_execution_info: QueryExecutionInfoRef,

    query_metrics: Arc<DistributedQueryMetrics>,
}

impl QueryExecution {
    #[allow(clippy::too_many_arguments)]
    pub fn new(query: Query, session_id: SessionId) -> Self {
        let query = Arc::new(query);
        let (sender, receiver) = channel(100);
        let state = QueryState::Pending {
            msg_receiver: receiver,
        };

        Self {
            query,
            state: RwLock::new(state),
            shutdown_tx: sender,
            session_id,
        }
    }

    /// Start execution of this query.
    /// Note the two shutdown channel sender and receivers are not dual.
    /// One is used for propagate error to `QueryResultFetcher`, one is used for listening on
    /// cancel request (from ctrl-c, cli, ui etc).
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        &self,
        context: ExecutionContextRef,
        worker_node_manager: WorkerNodeSelector,
        pinned_snapshot: PinnedHummockSnapshot,
        compute_client_pool: ComputeClientPoolRef,
        catalog_reader: CatalogReader,
        query_execution_info: QueryExecutionInfoRef,
        query_metrics: Arc<DistributedQueryMetrics>,
    ) -> SchedulerResult<QueryResultFetcher> {
        let mut state = self.state.write().await;
        let cur_state = mem::replace(&mut *state, QueryState::Failed);

        // Because the snapshot may be released before all stages are scheduled, we only pass a
        // reference of `pinned_snapshot`. Its ownership will be moved into `QueryRunner` so that it
        // can control when to release the snapshot.
        let stage_executions = self.gen_stage_executions(
            &pinned_snapshot,
            context,
            worker_node_manager,
            compute_client_pool.clone(),
            catalog_reader,
        );

        match cur_state {
            QueryState::Pending { msg_receiver } => {
                *state = QueryState::Running;

                // Create a oneshot channel for QueryResultFetcher to get failed event.
                let (root_stage_sender, root_stage_receiver) =
                    oneshot::channel::<SchedulerResult<QueryResultFetcher>>();

                let runner = QueryRunner {
                    query: self.query.clone(),
                    stage_executions,
                    msg_receiver,
                    root_stage_sender: Some(root_stage_sender),
                    scheduled_stages_count: 0,
                    query_execution_info,
                    query_metrics,
                };

                tracing::trace!("Starting query: {:?}", self.query.query_id);

                // Not trace the error here, it will be processed in scheduler.
                tokio::spawn(async move { runner.run(pinned_snapshot).await });

                let root_stage = root_stage_receiver
                    .await
                    .map_err(|e| anyhow!("Starting query execution failed: {:?}", e))??;

                tracing::trace!(
                    "Received root stage query result fetcher: {:?}, query id: {:?}",
                    root_stage,
                    self.query.query_id
                );

                tracing::trace!("Query {:?} started.", self.query.query_id);
                Ok(root_stage)
            }
            _ => {
                unreachable!("The query runner should not be scheduled twice");
            }
        }
    }

    /// Cancel execution of this query.
    pub async fn abort(self: Arc<Self>) {
        if self
            .shutdown_tx
            .send(QueryMessage::CancelQuery)
            .await
            .is_err()
        {
            warn!("Send cancel query request failed: the query has ended");
        } else {
            info!("Send cancel request to query-{:?}", self.query.query_id);
        };
    }

    fn gen_stage_executions(
        &self,
        pinned_snapshot: &PinnedHummockSnapshot,
        context: ExecutionContextRef,
        worker_node_manager: WorkerNodeSelector,
        compute_client_pool: ComputeClientPoolRef,
        catalog_reader: CatalogReader,
    ) -> HashMap<StageId, Arc<StageExecution>> {
        let mut stage_executions: HashMap<StageId, Arc<StageExecution>> =
            HashMap::with_capacity(self.query.stage_graph.stages.len());

        for stage_id in self.query.stage_graph.stage_ids_by_topo_order() {
            let children_stages = self
                .query
                .stage_graph
                .get_child_stages_unchecked(&stage_id)
                .iter()
                .map(|s| stage_executions[s].clone())
                .collect::<Vec<Arc<StageExecution>>>();

            let stage_exec = Arc::new(StageExecution::new(
                pinned_snapshot.get_batch_query_epoch(),
                self.query.stage_graph.stages[&stage_id].clone(),
                worker_node_manager.clone(),
                self.shutdown_tx.clone(),
                children_stages,
                compute_client_pool.clone(),
                catalog_reader.clone(),
                context.clone(),
            ));
            stage_executions.insert(stage_id, stage_exec);
        }
        stage_executions
    }
}

impl Drop for QueryRunner {
    fn drop(&mut self) {
        self.query_metrics.running_query_num.dec();
    }
}

impl Debug for QueryRunner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut graph = Graph::<String, String>::new();
        let mut stage_id_to_node_id = HashMap::new();
        for stage in &self.stage_executions {
            let node_id = graph.add_node(format!("{} {}", stage.0, block_on(stage.1.state())));
            stage_id_to_node_id.insert(stage.0, node_id);
        }

        for stage in &self.stage_executions {
            let stage_id = stage.0;
            if let Some(child_stages) = self.query.stage_graph.get_child_stages(stage_id) {
                for child_stage in child_stages {
                    graph.add_edge(
                        *stage_id_to_node_id.get(stage_id).unwrap(),
                        *stage_id_to_node_id.get(child_stage).unwrap(),
                        "".to_string(),
                    );
                }
            }
        }

        // Visit https://dreampuf.github.io/GraphvizOnline/ to display the result
        writeln!(f, "{}", Dot::with_config(&graph, &[Config::EdgeNoLabel]))
    }
}

impl QueryRunner {
    async fn run(mut self, pinned_snapshot: PinnedHummockSnapshot) {
        self.query_metrics.running_query_num.inc();
        // Start leaf stages.
        let leaf_stages = self.query.leaf_stages();
        for stage_id in &leaf_stages {
            self.stage_executions[stage_id].start().await;
            tracing::trace!(
                "Query stage {:?}-{:?} started.",
                self.query.query_id,
                stage_id
            );
        }
        let mut stages_with_table_scan = self.query.stages_with_table_scan();
        let has_lookup_join_stage = self.query.has_lookup_join_stage();
        // To convince the compiler that `pinned_snapshot` will only be dropped once.
        let mut pinned_snapshot_to_drop = Some(pinned_snapshot);

        let mut finished_stage_cnt = 0usize;
        while let Some(msg_inner) = self.msg_receiver.recv().await {
            match msg_inner {
                Stage(Scheduled(stage_id)) => {
                    tracing::trace!(
                        "Query stage {:?}-{:?} scheduled.",
                        self.query.query_id,
                        stage_id
                    );
                    self.scheduled_stages_count += 1;
                    stages_with_table_scan.remove(&stage_id);
                    // If query contains lookup join we need to delay epoch unpin util the end of
                    // the query.
                    if !has_lookup_join_stage && stages_with_table_scan.is_empty() {
                        // We can be sure here that all the Hummock iterators have been created,
                        // thus they all successfully pinned a HummockVersion.
                        // So we can now unpin their epoch.
                        tracing::trace!("Query {:?} has scheduled all of its stages that have table scan (iterator creation).", self.query.query_id);
                        pinned_snapshot_to_drop.take();
                    }

                    // For root stage, we execute in frontend local. We will pass the root fragment
                    // to QueryResultFetcher and execute to get a Chunk stream.
                    for parent in self.query.get_parents(&stage_id) {
                        if self.all_children_scheduled(parent).await
                                // Do not schedule same stage twice.
                                && self.stage_executions[parent].is_pending().await
                        {
                            self.stage_executions[parent].start().await;
                        }
                    }
                }
                Stage(ScheduledRoot(receiver)) => {
                    // We already schedule the root fragment, therefore we can notify query result
                    // fetcher.
                    self.send_root_stage_info(receiver);
                }
                Stage(StageEvent::Failed { id, reason }) => {
                    error!(
                        "Query stage {:?}-{:?} failed: {:?}.",
                        self.query.query_id, id, reason
                    );

                    self.clean_all_stages(Some(reason)).await;
                    // One stage failed, not necessary to execute schedule stages.
                    break;
                }
                Stage(StageEvent::Completed(_)) => {
                    finished_stage_cnt += 1;
                    assert!(finished_stage_cnt <= self.stage_executions.len());
                    if finished_stage_cnt == self.stage_executions.len() {
                        tracing::trace!(
                            "Query {:?} completed, starting to clean stage tasks.",
                            &self.query.query_id
                        );
                        // Now all stages completed, we should remove all
                        self.clean_all_stages(None).await;
                        break;
                    }
                }
                QueryMessage::CancelQuery => {
                    self.clean_all_stages(Some(SchedulerError::QueryCancelError))
                        .await;
                    // One stage failed, not necessary to execute schedule stages.
                    break;
                }
            }
        }
    }

    /// The `shutdown_tx` will only be Some if the stage is 1. In that case, we should keep the life
    /// of shutdown sender so that shutdown receiver won't be triggered.
    fn send_root_stage_info(&mut self, chunk_rx: Receiver<SchedulerResult<DataChunk>>) {
        let root_task_output_id = {
            let root_task_id_prost = TaskIdPb {
                query_id: self.query.query_id.clone().id,
                stage_id: self.query.root_stage_id(),
                task_id: ROOT_TASK_ID,
            };

            TaskOutputIdPb {
                task_id: Some(root_task_id_prost),
                output_id: ROOT_TASK_OUTPUT_ID,
            }
        };

        let root_stage_result = QueryResultFetcher::new(
            root_task_output_id,
            // Execute in local, so no need to fill meaningful address.
            HostAddress::default(),
            chunk_rx,
            self.query.query_id.clone(),
            self.query_execution_info.clone(),
        );

        // Consume sender here.
        let root_stage_sender = mem::take(&mut self.root_stage_sender);

        if let Err(e) = root_stage_sender.unwrap().send(Ok(root_stage_result)) {
            warn!("Query execution dropped: {:?}", e);
        } else {
            debug!("Root stage for {:?} sent.", self.query.query_id);
        }
    }

    async fn all_children_scheduled(&self, stage_id: &StageId) -> bool {
        for child in self.query.stage_graph.get_child_stages_unchecked(stage_id) {
            if !self.stage_executions[child].is_scheduled().await {
                return false;
            }
        }
        true
    }

    /// Handle ctrl-c query or failed execution. Should stop all executions and send error to query
    /// result fetcher.
    async fn clean_all_stages(&mut self, error: Option<SchedulerError>) {
        let error_msg = error.as_ref().map(|e| e.to_string());
        if let Some(reason) = error {
            // Consume sender here and send error to root stage.
            let root_stage_sender = mem::take(&mut self.root_stage_sender);
            // It's possible we receive stage failed event message multi times and the
            // sender has been consumed in first failed event.
            if let Some(sender) = root_stage_sender {
                if let Err(e) = sender.send(Err(reason)) {
                    warn!("Query execution dropped: {:?}", e);
                } else {
                    debug!(
                        "Root stage failure event for {:?} sent.",
                        self.query.query_id
                    );
                }
            }

            // If root stage has been taken (None), then root stage is responsible for send error to
            // Query Result Fetcher.
        }

        tracing::trace!("Cleaning stages in query [{:?}]", self.query.query_id);
        // Stop all running stages.
        for stage_execution in self.stage_executions.values() {
            // The stop is return immediately so no need to spawn tasks.
            stage_execution.stop(error_msg.clone()).await;
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::rc::Rc;
    use std::sync::{Arc, RwLock};

    use fixedbitset::FixedBitSet;
    use risingwave_common::catalog::{ColumnDesc, TableDesc};
    use risingwave_common::constants::hummock::TABLE_OPTION_DUMMY_RETENTION_SECOND;
    use risingwave_common::hash::ParallelUnitMapping;
    use risingwave_common::types::DataType;
    use risingwave_pb::common::worker_node::Property;
    use risingwave_pb::common::{HostAddress, ParallelUnit, WorkerNode, WorkerType};
    use risingwave_pb::plan_common::JoinType;
    use risingwave_rpc_client::ComputeClientPool;

    use crate::catalog::catalog_service::CatalogReader;
    use crate::catalog::root_catalog::Catalog;
    use crate::expr::InputRef;
    use crate::optimizer::plan_node::{
        generic, BatchExchange, BatchFilter, BatchHashJoin, EqJoinPredicate, LogicalScan, ToBatch,
    };
    use crate::optimizer::property::{Distribution, Order};
    use crate::optimizer::{OptimizerContext, PlanRef};
    use crate::scheduler::distributed::QueryExecution;
    use crate::scheduler::plan_fragmenter::{BatchPlanFragmenter, Query};
    use crate::scheduler::worker_node_manager::{WorkerNodeManager, WorkerNodeSelector};
    use crate::scheduler::{
        DistributedQueryMetrics, ExecutionContext, HummockSnapshotManager, PinnedHummockSnapshot,
        QueryExecutionInfo,
    };
    use crate::session::SessionImpl;
    use crate::test_utils::MockFrontendMetaClient;
    use crate::utils::Condition;

    #[tokio::test]
    async fn test_query_should_not_hang_with_empty_worker() {
        let worker_node_manager = Arc::new(WorkerNodeManager::mock(vec![]));
        let worker_node_selector = WorkerNodeSelector::new(worker_node_manager.clone(), false);
        let compute_client_pool = Arc::new(ComputeClientPool::default());
        let hummock_snapshot_manager = Arc::new(HummockSnapshotManager::new(Arc::new(
            MockFrontendMetaClient {},
        )));
        let catalog_reader =
            CatalogReader::new(Arc::new(parking_lot::RwLock::new(Catalog::default())));
        let query = create_query().await;
        let query_id = query.query_id().clone();
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        let query_execution = Arc::new(QueryExecution::new(query, (0, 0)));
        let query_execution_info = Arc::new(RwLock::new(QueryExecutionInfo::new_from_map(
            HashMap::from([(query_id, query_execution.clone())]),
        )));

        assert!(query_execution
            .start(
                ExecutionContext::new(SessionImpl::mock().into()).into(),
                worker_node_selector,
                PinnedHummockSnapshot::FrontendPinned(pinned_snapshot, true),
                compute_client_pool,
                catalog_reader,
                query_execution_info,
                Arc::new(DistributedQueryMetrics::for_test()),
            )
            .await
            .is_err());
    }

    pub async fn create_query() -> Query {
        // Construct a Hash Join with Exchange node.
        // Logical plan:
        //
        //    HashJoin
        //     /    \
        //   Scan  Scan
        //
        let ctx = OptimizerContext::mock().await;
        let table_id = 0.into();
        let batch_plan_node: PlanRef = LogicalScan::create(
            "".to_string(),
            false,
            Rc::new(TableDesc {
                table_id,
                stream_key: vec![],
                pk: vec![],
                columns: vec![
                    ColumnDesc::new_atomic(DataType::Int32, "a", 0),
                    ColumnDesc::new_atomic(DataType::Float64, "b", 1),
                    ColumnDesc::new_atomic(DataType::Int64, "c", 2),
                ],
                distribution_key: vec![],
                append_only: false,
                retention_seconds: TABLE_OPTION_DUMMY_RETENTION_SECOND,
                value_indices: vec![0, 1, 2],
                read_prefix_len_hint: 0,
                watermark_columns: FixedBitSet::with_capacity(3),
                versioned: false,
            }),
            vec![],
            ctx,
            false,
        )
        .to_batch()
        .unwrap()
        .to_distributed()
        .unwrap();
        let batch_filter = BatchFilter::new(generic::Filter::new(
            Condition {
                conjunctions: vec![],
            },
            batch_plan_node.clone(),
        ))
        .into();
        let batch_exchange_node1: PlanRef = BatchExchange::new(
            batch_plan_node.clone(),
            Order::default(),
            Distribution::HashShard(vec![0, 1]),
        )
        .into();
        let batch_exchange_node2: PlanRef = BatchExchange::new(
            batch_filter,
            Order::default(),
            Distribution::HashShard(vec![0, 1]),
        )
        .into();
        let logical_join_node = generic::Join::with_full_output(
            batch_exchange_node1.clone(),
            batch_exchange_node2.clone(),
            JoinType::Inner,
            Condition::true_cond(),
        );
        let eq_key_1 = (
            InputRef {
                index: 0,
                data_type: DataType::Int32,
            },
            InputRef {
                index: 2,
                data_type: DataType::Int32,
            },
            false,
        );
        let eq_key_2 = (
            InputRef {
                index: 1,
                data_type: DataType::Float64,
            },
            InputRef {
                index: 3,
                data_type: DataType::Float64,
            },
            false,
        );
        let eq_join_predicate =
            EqJoinPredicate::new(Condition::true_cond(), vec![eq_key_1, eq_key_2], 2, 2);
        let hash_join_node: PlanRef =
            BatchHashJoin::new(logical_join_node, eq_join_predicate).into();
        let batch_exchange_node: PlanRef = BatchExchange::new(
            hash_join_node.clone(),
            Order::default(),
            Distribution::Single,
        )
        .into();

        let worker1 = WorkerNode {
            id: 0,
            r#type: WorkerType::ComputeNode as i32,
            host: Some(HostAddress {
                host: "127.0.0.1".to_string(),
                port: 5687,
            }),
            state: risingwave_pb::common::worker_node::State::Running as i32,
            parallel_units: generate_parallel_units(0, 0),
            property: Some(Property {
                is_streaming: true,
                is_serving: true,
            }),
        };
        let worker2 = WorkerNode {
            id: 1,
            r#type: WorkerType::ComputeNode as i32,
            host: Some(HostAddress {
                host: "127.0.0.1".to_string(),
                port: 5688,
            }),
            state: risingwave_pb::common::worker_node::State::Running as i32,
            parallel_units: generate_parallel_units(8, 1),
            property: Some(Property {
                is_streaming: true,
                is_serving: true,
            }),
        };
        let worker3 = WorkerNode {
            id: 2,
            r#type: WorkerType::ComputeNode as i32,
            host: Some(HostAddress {
                host: "127.0.0.1".to_string(),
                port: 5689,
            }),
            state: risingwave_pb::common::worker_node::State::Running as i32,
            parallel_units: generate_parallel_units(16, 2),
            property: Some(Property {
                is_streaming: true,
                is_serving: true,
            }),
        };
        let workers = vec![worker1, worker2, worker3];
        let worker_node_manager = Arc::new(WorkerNodeManager::mock(workers));
        let worker_node_selector = WorkerNodeSelector::new(worker_node_manager.clone(), false);
        worker_node_manager
            .insert_streaming_fragment_mapping(0, ParallelUnitMapping::new_single(0));
        worker_node_manager.set_serving_fragment_mapping(
            vec![(0, ParallelUnitMapping::new_single(0))]
                .into_iter()
                .collect(),
        );
        let catalog = Arc::new(parking_lot::RwLock::new(Catalog::default()));
        catalog.write().insert_table_id_mapping(table_id, 0);
        let catalog_reader = CatalogReader::new(catalog);
        // Break the plan node into fragments.
        let fragmenter = BatchPlanFragmenter::new(
            worker_node_selector,
            catalog_reader,
            None,
            batch_exchange_node.clone(),
        )
        .unwrap();
        fragmenter.generate_complete_query().await.unwrap()
    }

    fn generate_parallel_units(start_id: u32, node_id: u32) -> Vec<ParallelUnit> {
        let parallel_degree = 8;
        (start_id..start_id + parallel_degree)
            .map(|id| ParallelUnit {
                id,
                worker_node_id: node_id,
            })
            .collect()
    }
}
