// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Local execution for batch query.

use std::collections::HashMap;

use futures_async_stream::try_stream;
use risingwave_batch::executor::ExecutorBuilder;
use risingwave_batch::task::TaskId;
use risingwave_common::array::DataChunk;
use risingwave_common::error::{internal_error, Result, RwError};
use risingwave_common::try_match_expand;
use risingwave_pb::batch_plan::exchange_info::DistributionMode;
use risingwave_pb::batch_plan::exchange_source::LocalExecutePlan::Plan;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{
    ExchangeInfo, ExchangeSource, LocalExecutePlan, PlanFragment, PlanNode as PlanNodeProst,
    TaskId as ProstTaskId, TaskOutputId,
};
use tracing::debug;
use uuid::Uuid;

use crate::optimizer::plan_node::PlanNodeType;
use crate::scheduler::plan_fragmenter::{ExecutionPlanNode, Query, StageId};
use crate::scheduler::task_context::FrontendBatchTaskContext;
use crate::session::FrontendEnv;

pub struct LocalQueryExecution {
    sql: String,
    query: Query,
    front_env: FrontendEnv,
    epoch: Option<u64>,
}

impl LocalQueryExecution {
    pub fn new<S: Into<String>>(query: Query, front_env: FrontendEnv, sql: S) -> Self {
        Self {
            sql: sql.into(),
            query,
            front_env,
            epoch: None,
        }
    }

    #[try_stream(ok = DataChunk, error = RwError)]
    pub async fn run(mut self) {
        debug!(
            "Starting to run query: {:?}, sql: '{}'",
            self.query.query_id, self.sql
        );

        let context = FrontendBatchTaskContext::new(self.front_env.clone());

        let query_id = self.query.query_id().clone();

        let task_id = TaskId {
            query_id: self.query.query_id.id.clone(),
            stage_id: 0,
            task_id: 0,
        };

        let epoch = self
            .front_env
            .hummock_snapshot_manager()
            .get_epoch(query_id)
            .await?;
        self.epoch = Some(epoch);
        let plan_fragment = self.create_plan_fragment()?;
        let plan_node = plan_fragment.root.unwrap();
        let executor = ExecutorBuilder::new(&plan_node, &task_id, context, epoch);
        let executor = executor.build().await?;

        #[for_await]
        for chunk in executor.execute() {
            yield chunk?;
        }
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
    ///
    /// We also remark that there are at most two stages during local execution mode, meaning that
    /// `the first exchange operator` is the only possible exchange operator in the whole plan.
    /// And it is possible that there is no exchange operator at all, e.g. `select 1+2;`
    fn create_plan_fragment(&self) -> Result<PlanFragment> {
        let root_stage_id = self.query.root_stage_id();
        let root_stage = self.query.stage_graph.stages.get(&root_stage_id).unwrap();
        let second_stage_id = self.query.stage_graph.get_child_stages(&root_stage_id);
        let plan_node_prost = match second_stage_id {
            None => {
                debug!("Local execution mode converts a plan with a single stage");
                self.convert_plan_node(&*root_stage.root, &mut None)?
            }
            Some(second_stage_ids) => {
                debug!("Local execution mode converts a plan with two stages");
                if second_stage_ids.is_empty() {
                    // This branch is defensive programming. The semantics should be the same as
                    // `None`.
                    self.convert_plan_node(&*root_stage.root, &mut None)?
                } else {
                    let mut stage_id_to_plan = HashMap::new();
                    for second_stage_id in second_stage_ids {
                        let second_stage =
                            self.query.stage_graph.stages.get(second_stage_id).unwrap();
                        let second_stage_plan_node =
                            self.convert_plan_node(&*second_stage.root, &mut None)?;
                        let second_stage_plan_fragment = PlanFragment {
                            root: Some(second_stage_plan_node),
                            exchange_info: Some(ExchangeInfo {
                                mode: DistributionMode::Single as i32,
                                ..Default::default()
                            }),
                        };
                        stage_id_to_plan.insert(*second_stage_id, second_stage_plan_fragment);
                    }
                    let mut stage_id_to_plan = Some(stage_id_to_plan);
                    let res = self.convert_plan_node(&*root_stage.root, &mut stage_id_to_plan)?;
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
        second_stage_plan: &mut Option<HashMap<StageId, PlanFragment>>,
    ) -> Result<PlanNodeProst> {
        match execution_plan_node.plan_node_type {
            PlanNodeType::BatchExchange => {
                let exchange_from_stage_id = execution_plan_node
                    .stage_id
                    .expect("We expect stage id for Exchange Operator");
                match second_stage_plan.as_mut() {
                    Some(second_stage_plan) => {
                        let second_stage_plan_fragment = second_stage_plan.remove(&exchange_from_stage_id).expect("We expect child stage fragment for Exchange Operator running in the frontend");
                        let mut exchange_node =
                            try_match_expand!(execution_plan_node.node.clone(), NodeBody::Exchange)?;
                        let local_execute_plan = LocalExecutePlan {
                            plan: Some(second_stage_plan_fragment),
                            epoch: self.epoch.expect("Local execution mode has not acquired the epoch when generating the plan.")
                        };
                        exchange_node.sources.extend(self.front_env.worker_node_manager().list_worker_nodes().iter().enumerate().map(|(idx, worker_node)| {
                            let exchange_source = ExchangeSource {
                                task_output_id: Some(TaskOutputId {
                                    task_id: Some(ProstTaskId {
                                        // We remark that `RowSeqScanExecutor` relies on the task_id
                                        // to differentiate which one is primary and
                                        // should really read all the data.
                                        task_id: idx as u32,
                                        stage_id: 0,
                                        query_id: "local execution mode".to_string(),
                                    }),
                                    output_id: 0,
                                }),
                                host: Some(worker_node.host.as_ref().unwrap().clone()),
                                local_execute_plan: Some(Plan(local_execute_plan.clone())),
                            };
                            exchange_source
                        }));
                        Ok(PlanNodeProst {
                            /// Since all the rest plan is embedded into the exchange node,
                            /// there is no children any more.
                            children: vec![],
                            identity: Uuid::new_v4().to_string(),
                            node_body: Some(NodeBody::Exchange(exchange_node)),
                        })
                    }
                    None => {
                        Err(internal_error("Unexpected exchange detected. We are either converting a single stage plan or converting the second stage of the plan."))
                    }
                }
            }
            _ => {
                let children = execution_plan_node
                    .children
                    .iter()
                    .map(|e| self.convert_plan_node(e, second_stage_plan))
                    .collect::<Result<Vec<PlanNodeProst>>>()?;

                Ok(PlanNodeProst {
                    children,
                    // TODO: Generate meaningful identify
                    identity: Uuid::new_v4().to_string(),
                    node_body: Some(execution_plan_node.node.clone()),
                })
            }
        }
    }
}
