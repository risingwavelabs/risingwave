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

use risingwave_common::catalog::TableId;
use risingwave_common::error::tonic_err;
use risingwave_pb::common::ParallelUnitType;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerService;
use risingwave_pb::meta::*;
use tonic::{Request, Response, Status};

use crate::cluster::ClusterManagerRef;
use crate::manager::MetaSrvEnv;
use crate::model::TableFragments;
use crate::storage::MetaStore;
use crate::stream::{FragmentManagerRef, GlobalStreamManagerRef, StreamFragmenter};

pub type TonicResponse<T> = Result<Response<T>, Status>;

#[derive(Clone)]
pub struct StreamServiceImpl<S>
where
    S: MetaStore,
{
    env: MetaSrvEnv<S>,

    global_stream_manager: GlobalStreamManagerRef<S>,
    fragment_manager: FragmentManagerRef<S>,
    cluster_manager: ClusterManagerRef<S>,
}

impl<S> StreamServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        global_stream_manager: GlobalStreamManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        cluster_manager: ClusterManagerRef<S>,
    ) -> Self {
        StreamServiceImpl {
            env,
            global_stream_manager,
            fragment_manager,
            cluster_manager,
        }
    }
}

#[async_trait::async_trait]
impl<S> StreamManagerService for StreamServiceImpl<S>
where
    S: MetaStore,
{
    #[cfg_attr(coverage, no_coverage)]
    async fn create_materialized_view(
        &self,
        request: Request<CreateMaterializedViewRequest>,
    ) -> TonicResponse<CreateMaterializedViewResponse> {
        use crate::stream::CreateMaterializedViewContext;

        let req = request.into_inner();

        tracing::trace!(
            target: "events::meta::create_mv_v1",
            plan = serde_json::to_string(&req).unwrap().as_str(),
            "create materialized view"
        );

        let hash_mapping = self.cluster_manager.get_hash_mapping().await;
        let parallel_degree = self
            .cluster_manager
            .get_parallel_unit_count(Some(ParallelUnitType::Hash))
            .await;
        let mut ctx = CreateMaterializedViewContext {
            is_legacy_frontend: true,
            hash_mapping,
            ..Default::default()
        };

        let graph = StreamFragmenter::generate_graph(
            self.env.id_gen_manager_ref(),
            self.fragment_manager.clone(),
            parallel_degree as u32,
            true,
            req.get_stream_node().map_err(tonic_err)?,
            &mut ctx,
        )
        .await
        .map_err(|e| e.to_grpc_status())?;

        let table_fragments = TableFragments::new(TableId::from(&req.table_ref_id), graph);
        match self
            .global_stream_manager
            .create_materialized_view(table_fragments, ctx)
            .await
        {
            Ok(()) => Ok(Response::new(CreateMaterializedViewResponse {
                status: None,
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn drop_materialized_view(
        &self,
        request: Request<DropMaterializedViewRequest>,
    ) -> TonicResponse<DropMaterializedViewResponse> {
        let req = request.into_inner();

        match self
            .global_stream_manager
            .drop_materialized_view(&TableId::from(&req.table_ref_id))
            .await
        {
            Ok(()) => Ok(Response::new(DropMaterializedViewResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn flush(&self, request: Request<FlushRequest>) -> TonicResponse<FlushResponse> {
        let _req = request.into_inner();

        self.global_stream_manager
            .flush()
            .await
            .map_err(|e| e.to_grpc_status())?;
        Ok(Response::new(FlushResponse { status: None }))
    }
}
