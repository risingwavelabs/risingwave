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
//
use risingwave_common::catalog::TableId;
use risingwave_common::error::tonic_err;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerService;
use risingwave_pb::meta::*;
use tonic::{Request, Response, Status};

use crate::cluster::StoredClusterManagerRef;
use crate::cluster::StoredClusterManager;
use crate::manager::{EpochGeneratorRef, IdGeneratorManagerRef, MetaSrvEnv};
use crate::model::TableFragments;
use crate::storage::MetaStore;
use crate::stream::{CreateSourceContext, DropSourceContext, FragmentManagerRef, SourceManagerRef, StreamFragmenter, StreamManagerRef};

pub type TonicResponse<T> = Result<Response<T>, Status>;

#[derive(Clone)]
pub struct StreamServiceImpl<S>
    where
        S: MetaStore,
{
    sm: StreamManagerRef<S>,

    id_gen_manager_ref: IdGeneratorManagerRef<S>,
    fragment_manager_ref: FragmentManagerRef<S>,
    cluster_manager: StoredClusterManagerRef<S>,

    #[allow(dead_code)]
    epoch_generator: EpochGeneratorRef,
}

impl<S> StreamServiceImpl<S>
    where
        S: MetaStore,
{
    pub fn new(
        sm: StreamManagerRef<S>,
        fragment_manager_ref: FragmentManagerRef<S>,
        cluster_manager: StoredClusterManagerRef<S>,
        source_manager_ref: SourceManagerRef<S>,
        env: MetaSrvEnv<S>,
    ) -> Self {
        StreamServiceImpl {
            sm,
            fragment_manager_ref,
            id_gen_manager_ref: env.id_gen_manager_ref(),
            cluster_manager,
            epoch_generator: env.epoch_generator_ref(),
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

        tracing::debug!(
            plan = serde_json::to_string(&req).unwrap().as_str(),
            "create materialized view"
        );

        let hash_mapping = self.cluster_manager.get_hash_mapping().await;
        let mut ctx = CreateMaterializedViewContext::default();

        let mut fragmenter = StreamFragmenter::new(
            self.id_gen_manager_ref.clone(),
            self.fragment_manager_ref.clone(),
            hash_mapping,
        );
        let graph = fragmenter
            .generate_graph(req.get_stream_node().map_err(tonic_err)?, &mut ctx)
            .await
            .map_err(|e| e.to_grpc_status())?;

        let table_fragments = TableFragments::new(TableId::from(&req.table_ref_id), graph);
        match self.sm.create_materialized_view(table_fragments, ctx).await {
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
            .sm
            .drop_materialized_view(req.get_table_ref_id().map_err(tonic_err)?)
            .await
        {
            Ok(()) => Ok(Response::new(DropMaterializedViewResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }


    #[cfg(not(tarpaulin_include))]
    async fn flush(&self, request: Request<FlushRequest>) -> TonicResponse<FlushResponse> {
        let _req = request.into_inner();

        self.sm.flush().await.map_err(|e| e.to_grpc_status())?;
        Ok(Response::new(FlushResponse { status: None }))
    }
}
