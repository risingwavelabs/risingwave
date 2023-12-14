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

use risingwave_pb::catalog::PbCreateType;
use risingwave_pb::stream_plan::PbStreamFragmentGraph;

use crate::controller::catalog::CatalogController;
use crate::manager::{NotificationVersion, StreamingJob};
use crate::MetaResult;

impl CatalogController {
    pub async fn create_streaming_job(
        &self,
        _job: StreamingJob,
        _graph: PbStreamFragmentGraph,
        _create_type: PbCreateType,
    ) -> MetaResult<NotificationVersion> {
        let _catalog = self.inner.write().await;
        // let version = catalog.create_streaming_job(&mut job, graph, create_type)?;
        // Ok(version)

        // 1. build fragment graph.
        // 2. build streaming job: CompleteStreamFragmentGraph, ActorGraphBuilder,
        // 3.

        todo!()
    }
}
