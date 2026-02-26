// Copyright 2026 RisingWave Labs
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

use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_common::Result as DFResult;

use crate::datafusion::{ExpandPlanner, ProjectSetPlanner};

#[derive(Debug)]
pub struct RwCustomQueryPlanner;

impl RwCustomQueryPlanner {
    pub fn new() -> Arc<Self> {
        static INSTANCE: LazyLock<Arc<RwCustomQueryPlanner>> =
            LazyLock::new(|| Arc::new(RwCustomQueryPlanner));
        INSTANCE.clone()
    }
}

#[async_trait]
impl QueryPlanner for RwCustomQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        static PLANNER: LazyLock<Arc<DefaultPhysicalPlanner>> = LazyLock::new(|| {
            Arc::new(DefaultPhysicalPlanner::with_extension_planners(vec![
                Arc::new(ExpandPlanner),
                Arc::new(ProjectSetPlanner),
            ]))
        });

        let planner = PLANNER.clone();
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
