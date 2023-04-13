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

//! Local execution for batch query.
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use pgwire::pg_server::BoxedError;
use rand::seq::SliceRandom;
use risingwave_batch::executor::{BoxedDataChunkStream, ExecutorBuilder};
use risingwave_batch::task::TaskId;
use risingwave_common::array::DataChunk;
use risingwave_common::bail;
use risingwave_common::error::RwError;
use risingwave_common::hash::ParallelUnitMapping;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::stream_cancel::{cancellable_stream, Tripwire};
use risingwave_connector::source::SplitMetaData;
use risingwave_pb::batch_plan::exchange_info::DistributionMode;
use risingwave_pb::batch_plan::exchange_source::LocalExecutePlan::Plan;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{
    ExchangeInfo, ExchangeSource, LocalExecutePlan, PbTaskId, PlanFragment, PlanNode as PlanNodePb,
    TaskOutputId,
};
use risingwave_pb::common::WorkerNode;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;
use uuid::Uuid;

use super::plan_fragmenter::{PartitionInfo, QueryStage, QueryStageRef};
use crate::catalog::{FragmentId, TableId};
use crate::optimizer::plan_node::PlanNodeType;
use crate::scheduler::plan_fragmenter::{ExecutionPlanNode, Query, StageId};
use crate::scheduler::task_context::FrontendBatchTaskContext;
use crate::scheduler::{PinnedHummockSnapshot, SchedulerError, SchedulerResult};
use crate::session::{AuthContext, FrontendEnv};

pub type LocalQueryStream = ReceiverStream<Result<DataChunk, BoxedError>>;

pub struct LocalQueryExecution {
    sql: String,
    query: Query,
    front_env: FrontendEnv,
    // The snapshot will be released when LocalQueryExecution is dropped.
    snapshot: PinnedHummockSnapshot,
    auth_context: Arc<AuthContext>,
    cancel_flag: Option<Tripwire<Result<DataChunk, BoxedError>>>,
}

impl LocalQueryExecution {
    pub fn new<S: Into<String>>(
        query: Query,
        front_env: FrontendEnv,
        sql: S,
        snapshot: PinnedHummockSnapshot,
        auth_context: Arc<AuthContext>,
        cancel_flag: Tripwire<Result<DataChunk, BoxedError>>,
    ) -> Self {
        Self {
            sql: sql.into(),
            query,
            front_env,
            snapshot,
            auth_context,
            cancel_flag: Some(cancel_flag),
        }
    }

    #[try_stream(ok = DataChunk, error = RwError)]
    pub async fn run_inner(self) {
        debug!(
            "Starting to run query: {:?}, sql: '{}'",
            self.query.query_id, self.sql
        );

        let context =
            FrontendBatchTaskContext::new(self.front_env.clone(), self.auth_context.clone());

        let task_id = TaskId {
            query_id: self.query.query_id.id.clone(),
            stage_id: 0,
            task_id: 0,
        };

        let plan_fragment = self.create_plan_fragment()?;
        let plan_node = plan_fragment.root.unwrap();
        let executor = ExecutorBuilder::new(
            &plan_node,
            &task_id,
            context,
            self.snapshot.get_batch_query_epoch(),
        );
        let executor = executor.build().await?;

        #[for_await]
        for chunk in executor.execute() {
            yield chunk?;
        }
    }

    pub fn run(self) -> BoxedDataChunkStream {
        Box::pin(self.run_inner())
    }

    pub fn stream_rows(mut self) -> LocalQueryStream {
        let (sender, receiver) = mpsc::channel(10);
        let tripwire = self.cancel_flag.take().unwrap();

        let mut data_stream = {
            let s = self.run().map(|r| r.map_err(|e| Box::new(e) as BoxedError));
            Box::pin(cancellable_stream(s, tripwire))
        };

        let future = async move {
            while let Some(r) = data_stream.next().await {
                if (sender.send(r).await).is_err() {
                    tracing::info!("Receiver closed.");
                }
            }
        };

        #[cfg(madsim)]
        tokio::spawn(future);
        #[cfg(not(madsim))]
        tokio::task::spawn_blocking(move || futures::executor::block_on(future));

        ReceiverStream::new(receiver)
    }

    /// Convert query to plan fragment.
    ///
    /// We can convert a query to plan fragment since in local execution mode, there are at most
    /// two layers, e.g. root stage and its optional input stage. If it does have input stage, it
    /// will be embedded in exchange source, so we can always convert a query into a plan fragment.
    ///
    /// We remark that the boundary to determine which part should be executed on the frontend and
    /// which part should be executed on the backend is `the first exchange operator` when looking
    /// from the the root of the plan to the leaves. The first exchange operator contains
    /// the pushed-down plan fragment.
    fn create_plan_fragment(&self) -> SchedulerResult<PlanFragment> {
        let root_stage_id = self.query.root_stage_id();
        let root_stage = self.query.stage_graph.stages.get(&root_stage_id).unwrap();
        assert_eq!(root_stage.parallelism.unwrap(), 1);
        let second_stage_id = self.query.stage_graph.get_child_stages(&root_stage_id);
        let plan_node_prost = match second_stage_id {
            None => {
                debug!("Local execution mode converts a plan with a single stage");
                self.convert_plan_node(&root_stage.root, &mut None, None)?
            }
            Some(second_stage_ids) => {
                debug!("Local execution mode converts a plan with two stages");
                if second_stage_ids.is_empty() {
                    // This branch is defensive programming. The semantics should be the same as
                    // `None`.
                    self.convert_plan_node(&root_stage.root, &mut None, None)?
                } else {
                    let mut second_stages = HashMap::new();
                    for second_stage_id in second_stage_ids {
                        let second_stage =
                            self.query.stage_graph.stages.get(second_stage_id).unwrap();
                        second_stages.insert(*second_stage_id, second_stage.clone());
                    }
                    let mut stage_id_to_plan = Some(second_stages);
                    let res =
                        self.convert_plan_node(&root_stage.root, &mut stage_id_to_plan, None)?;
                    assert!(
                        stage_id_to_plan.as_ref().unwrap().is_empty(),
                        "We expect that all the child stage plan fragments have been used"
                    );
                    res
                }
            }
        };

        Ok(PlanFragment {
            root: Some(plan_node_prost),
            // Intentionally leave this as `None` as this is the last stage for the frontend
            // to really get the output of computation, which is single distribution
            // but we do not need to explicitly specify this.
            exchange_info: None,
        })
    }

    fn convert_plan_node(
        &self,
        execution_plan_node: &ExecutionPlanNode,
        second_stages: &mut Option<HashMap<StageId, QueryStageRef>>,
        partition: Option<PartitionInfo>,
    ) -> SchedulerResult<PlanNodePb> {
        match execution_plan_node.plan_node_type {
            PlanNodeType::BatchExchange => {
                let exchange_source_stage_id = execution_plan_node
                    .source_stage_id
                    .expect("We expect stage id for Exchange Operator");
                let Some(second_stages) = second_stages.as_mut() else {
                    bail!("Unexpected exchange detected. We are either converting a single stage plan or converting the second stage of the plan.")
                };
                let second_stage = second_stages.remove(&exchange_source_stage_id).expect(
                    "We expect child stage fragment for Exchange Operator running in the frontend",
                );
                let mut node_body = execution_plan_node.node.clone();
                let sources = match &mut node_body {
                    NodeBody::Exchange(exchange_node) => &mut exchange_node.sources,
                    NodeBody::MergeSortExchange(merge_sort_exchange_node) => {
                        &mut merge_sort_exchange_node
                            .exchange
                            .as_mut()
                            .expect("MergeSortExchangeNode must have a exchange node")
                            .sources
                    }
                    _ => unreachable!(),
                };
                assert!(sources.is_empty());

                if let Some(table_scan_info) = second_stage.table_scan_info.clone() && let Some(vnode_bitmaps) = table_scan_info.partitions() {
                    // Similar to the distributed case (StageRunner::schedule_tasks).
                    // Set `vnode_ranges` of the scan node in `local_execute_plan` of each
                    // `exchange_source`.
                    let (parallel_unit_ids, vnode_bitmaps): (Vec<_>, Vec<_>) =
                        vnode_bitmaps.clone().into_iter().unzip();
                    let workers = self.front_env.worker_node_manager().get_workers_by_parallel_unit_ids(&parallel_unit_ids)?;

                    for (idx, (worker_node, partition)) in
                        (workers.into_iter().zip_eq_fast(vnode_bitmaps.into_iter())).enumerate()
                    {
                        let second_stage_plan_node = self.convert_plan_node(
                            &second_stage.root,
                            &mut None,
                            Some(PartitionInfo::Table(partition)),
                        )?;
                        let second_stage_plan_fragment = PlanFragment {
                            root: Some(second_stage_plan_node),
                            exchange_info: Some(ExchangeInfo {
                                mode: DistributionMode::Single as i32,
                                ..Default::default()
                            }),
                        };
                        let local_execute_plan = LocalExecutePlan {
                            plan: Some(second_stage_plan_fragment),
                            epoch: Some(self.snapshot.get_batch_query_epoch()),
                        };
                        let exchange_source = ExchangeSource {
                            task_output_id: Some(TaskOutputId {
                                task_id: Some(PbTaskId {
                                    task_id: idx as u32,
                                    stage_id: exchange_source_stage_id,
                                    query_id: self.query.query_id.id.clone(),
                                }),
                                output_id: 0,
                            }),
                            host: Some(worker_node.host.as_ref().unwrap().clone()),
                            local_execute_plan: Some(Plan(local_execute_plan)),
                        };
                        sources.push(exchange_source);
                    }
                } else if let Some(source_info) = &second_stage.source_info {
                    for (id,split) in source_info.split_info().unwrap().iter().enumerate() {
                        let second_stage_plan_node = self.convert_plan_node(
                            &second_stage.root,
                            &mut None,
                            Some(PartitionInfo::Source(split.clone())),
                        )?;
                        let second_stage_plan_fragment = PlanFragment {
                            root: Some(second_stage_plan_node),
                            exchange_info: Some(ExchangeInfo {
                                mode: DistributionMode::Single as i32,
                                ..Default::default()
                            }),
                        };
                        let local_execute_plan = LocalExecutePlan {
                            plan: Some(second_stage_plan_fragment),
                            epoch: Some(self.snapshot.get_batch_query_epoch()),
                        };
                        // NOTE: select a random work node here.
                        let worker_node = self.front_env.worker_node_manager().next_random()?;
                        let exchange_source = ExchangeSource {
                            task_output_id: Some(TaskOutputId {
                                task_id: Some(PbTaskId {
                                    task_id: id as u32,
                                    stage_id: exchange_source_stage_id,
                                    query_id: self.query.query_id.id.clone(),
                                }),
                                output_id: 0,
                            }),
                            host: Some(worker_node.host.as_ref().unwrap().clone()),
                            local_execute_plan: Some(Plan(local_execute_plan)),
                        };
                        sources.push(exchange_source);
                    }
                }
                else {
                    let second_stage_plan_node =
                        self.convert_plan_node(&second_stage.root, &mut None, None)?;
                    let second_stage_plan_fragment = PlanFragment {
                        root: Some(second_stage_plan_node),
                        exchange_info: Some(ExchangeInfo {
                            mode: DistributionMode::Single as i32,
                            ..Default::default()
                        }),
                    };

                    let local_execute_plan = LocalExecutePlan {
                        plan: Some(second_stage_plan_fragment),
                        epoch: Some(self.snapshot.get_batch_query_epoch()),
                    };

                    let workers = self.choose_worker(&second_stage)?;
                    *sources = workers
                        .iter()
                        .enumerate()
                        .map(|(idx, worker_node)| {
                            let exchange_source = ExchangeSource {
                                task_output_id: Some(TaskOutputId {
                                    task_id: Some(PbTaskId {
                                        task_id: idx as u32,
                                        stage_id: exchange_source_stage_id,
                                        query_id: self.query.query_id.id.clone(),
                                    }),
                                    output_id: 0,
                                }),
                                host: Some(worker_node.host.as_ref().unwrap().clone()),
                                local_execute_plan: Some(Plan(local_execute_plan.clone())),
                            };
                            exchange_source
                        })
                        .collect();
                }

                Ok(PlanNodePb {
                    /// Since all the rest plan is embedded into the exchange node,
                    /// there is no children any more.
                    children: vec![],
                    identity: Uuid::new_v4().to_string(),
                    node_body: Some(node_body),
                })
            }
            PlanNodeType::BatchSeqScan => {
                let mut node_body = execution_plan_node.node.clone();
                match &mut node_body {
                    NodeBody::RowSeqScan(ref mut scan_node) => {
                        if let Some(partition) = partition {
                            let partition = partition
                                .into_table()
                                .expect("PartitionInfo should be TablePartitionInfo here");
                            scan_node.vnode_bitmap = Some(partition.vnode_bitmap);
                            scan_node.scan_ranges = partition.scan_ranges;
                        }
                    }
                    NodeBody::SysRowSeqScan(_) => {}
                    _ => unreachable!(),
                }

                Ok(PlanNodePb {
                    children: vec![],
                    // TODO: Generate meaningful identify
                    identity: Uuid::new_v4().to_string(),
                    node_body: Some(node_body),
                })
            }
            PlanNodeType::BatchSource => {
                let mut node_body = execution_plan_node.node.clone();
                match &mut node_body {
                    NodeBody::Source(ref mut source_node) => {
                        if let Some(partition) = partition {
                            let partition = partition
                                .into_source()
                                .expect("PartitionInfo should be SourcePartitionInfo here");
                            source_node.split = partition.encode_to_bytes().into();
                        }
                    }
                    _ => unreachable!(),
                }

                Ok(PlanNodePb {
                    children: vec![],
                    // TODO: Generate meaningful identify
                    identity: Uuid::new_v4().to_string(),
                    node_body: Some(node_body),
                })
            }
            PlanNodeType::BatchLookupJoin => {
                let mut node_body = execution_plan_node.node.clone();
                match &mut node_body {
                    NodeBody::LocalLookupJoin(node) => {
                        let side_table_desc = node
                            .inner_side_table_desc
                            .as_ref()
                            .expect("no side table desc");
                        let mapping = self.front_env.worker_node_manager().serving_vnode_mapping(
                            self.get_fragment_id(&side_table_desc.table_id.into())?,
                        )?;

                        // TODO: should we use `pb::ParallelUnitMapping` here?
                        node.inner_side_vnode_mapping = mapping.to_expanded();
                        node.worker_nodes =
                            self.front_env.worker_node_manager().list_worker_nodes();
                    }
                    _ => unreachable!(),
                }

                let left_child = self.convert_plan_node(
                    &execution_plan_node.children[0],
                    second_stages,
                    partition,
                )?;

                Ok(PlanNodePb {
                    children: vec![left_child],
                    identity: Uuid::new_v4().to_string(),
                    node_body: Some(node_body),
                })
            }
            _ => {
                let children = execution_plan_node
                    .children
                    .iter()
                    .map(|e| self.convert_plan_node(e, second_stages, partition.clone()))
                    .collect::<SchedulerResult<Vec<PlanNodePb>>>()?;

                Ok(PlanNodePb {
                    children,
                    // TODO: Generate meaningful identify
                    identity: Uuid::new_v4().to_string(),
                    node_body: Some(execution_plan_node.node.clone()),
                })
            }
        }
    }

    #[inline(always)]
    fn get_fragment_id(&self, table_id: &TableId) -> SchedulerResult<FragmentId> {
        let reader = self.front_env.catalog_reader().read_guard();
        reader
            .get_table_by_id(table_id)
            .map(|table| table.fragment_id)
            .map_err(|e| SchedulerError::Internal(anyhow!(e)))
    }

    #[inline(always)]
    fn get_vnode_mapping(&self, table_id: &TableId) -> Option<ParallelUnitMapping> {
        let reader = self.front_env.catalog_reader().read_guard();
        reader
            .get_table_by_id(table_id)
            .map(|table| {
                self.front_env
                    .worker_node_manager()
                    .get_fragment_mapping(&table.fragment_id)
            })
            .ok()
            .flatten()
    }

    fn choose_worker(&self, stage: &Arc<QueryStage>) -> SchedulerResult<Vec<WorkerNode>> {
        if let Some(table_id) = stage.dml_table_id.as_ref() {
            // dml should use streaming vnode mapping
            let vnode_mapping = self
                .get_vnode_mapping(table_id)
                .ok_or_else(|| SchedulerError::EmptyWorkerNodes)?;
            let worker_node = {
                let parallel_unit_ids = vnode_mapping.iter_unique().collect_vec();
                let candidates = self
                    .front_env
                    .worker_node_manager()
                    .get_workers_by_parallel_unit_ids(&parallel_unit_ids)?;
                candidates.choose(&mut rand::thread_rng()).unwrap().clone()
            };
            Ok(vec![worker_node])
        } else {
            let mut workers = Vec::with_capacity(stage.parallelism.unwrap() as usize);
            for _ in 0..stage.parallelism.unwrap() {
                workers.push(self.front_env.worker_node_manager().next_random()?);
            }
            Ok(workers)
        }
    }
}
