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

use futures_async_stream::try_stream;
use risingwave_batch::executor::ExecutorBuilder;
use risingwave_batch::task::TaskId;
use risingwave_common::array::DataChunk;
use risingwave_common::error::{internal_error, Result, RwError};
use risingwave_pb::batch_plan::{PlanFragment, PlanNode as PlanNodeProst};
use tracing::debug;
use uuid::Uuid;

use crate::optimizer::plan_node::PlanNodeType;
use crate::scheduler::plan_fragmenter::{ExecutionPlanNode, Query};
use crate::scheduler::task_context::FrontendBatchTaskContext;
use crate::scheduler::HummockSnapshotManagerRef;

pub struct LocalQueryExecution {
    sql: String,
    query: Query,
    hummock_snapshot_manager: HummockSnapshotManagerRef,
}

impl LocalQueryExecution {
    pub fn new<S: Into<String>>(
        query: Query,
        hummock_snapshot_manager: HummockSnapshotManagerRef,
        sql: S,
    ) -> Self {
        Self {
            sql: sql.into(),
            query,
            hummock_snapshot_manager,
        }
    }

    #[try_stream(ok = DataChunk, error = RwError)]
    pub async fn run(self) {
        debug!(
            "Starting to run query: {:?}, sql: '{}'",
            self.query.query_id, self.sql
        );

        let plan_fragment = self.create_plan_fragment()?;

        let context = FrontendBatchTaskContext::default();

        let query_id = self.query.query_id().clone();

        let task_id = TaskId {
            query_id: self.query.query_id.id,
            stage_id: 0,
            task_id: 0,
        };

        let epoch = self.hummock_snapshot_manager.get_epoch(query_id).await?;
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
    /// which part should be executed on the backend is the first exchange operator when looking
    /// from the the root of the plan to the leaves.
    fn create_plan_fragment(&self) -> Result<PlanFragment> {
        let stage = self
            .query
            .stage_graph
            .stages
            .get(&self.query.root_stage_id())
            .unwrap();
        let plan_node_prost = self.convert_plan_node(&*stage.root)?;

        Ok(PlanFragment {
            root: Some(plan_node_prost),
            // Intentionally leave this as `None` as this is the last stage for the frontend
            // to really get the output of computation, which is single distribution
            // but we do not need to explicitly specify this.
            exchange_info: None,
        })
    }

    fn convert_plan_node(&self, execution_plan_node: &ExecutionPlanNode) -> Result<PlanNodeProst> {
        match execution_plan_node.plan_node_type {
            PlanNodeType::BatchExchange => {
                Err(internal_error("Exchange not supported in local mode yet!"))
            }
            _ => {
                let children = execution_plan_node
                    .children
                    .iter()
                    .map(|e| self.convert_plan_node(&*e))
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
