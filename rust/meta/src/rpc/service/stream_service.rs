use std::sync::Arc;

use risingwave_pb::meta::stream_manager_service_server::StreamManagerService;
use risingwave_pb::meta::{
    AddFragmentsToNodeRequest, AddFragmentsToNodeResponse, CreateMaterializedViewRequest,
    CreateMaterializedViewResponse, DropMaterializedViewRequest, DropMaterializedViewResponse,
    LoadAllFragmentsRequest, LoadAllFragmentsResponse,
};
use tonic::{Request, Response, Status};

use crate::cluster::StoredClusterManager;
use crate::manager::{IdGeneratorManagerRef, MetaSrvEnv};
use crate::stream::{StreamFragmenter, StreamManagerRef, StreamMetaManagerRef};

#[derive(Clone)]
pub struct StreamServiceImpl {
    // TODO: merge together.
    smm: StreamMetaManagerRef,
    sm: StreamManagerRef,

    id_gen_manager_ref: IdGeneratorManagerRef,
    cluster_manager: Arc<StoredClusterManager>,
}

impl StreamServiceImpl {
    pub fn new(
        smm: StreamMetaManagerRef,
        sm: StreamManagerRef,
        cluster_manager: Arc<StoredClusterManager>,
        env: MetaSrvEnv,
    ) -> Self {
        StreamServiceImpl {
            smm,
            sm,
            id_gen_manager_ref: env.id_gen_manager_ref(),
            cluster_manager,
        }
    }
}

#[async_trait::async_trait]
impl StreamManagerService for StreamServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn add_fragments_to_node(
        &self,
        request: Request<AddFragmentsToNodeRequest>,
    ) -> Result<Response<AddFragmentsToNodeResponse>, Status> {
        let req = request.into_inner();
        let result = self.smm.add_fragments_to_node(req.get_location()).await;
        match result {
            Ok(()) => Ok(Response::new(AddFragmentsToNodeResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn load_all_fragments(
        &self,
        request: Request<LoadAllFragmentsRequest>,
    ) -> Result<Response<LoadAllFragmentsResponse>, Status> {
        let _req = request.into_inner();
        Ok(Response::new(LoadAllFragmentsResponse {
            status: None,
            locations: self
                .smm
                .load_all_fragments()
                .await
                .map_err(|e| e.to_grpc_status())?,
        }))
    }

    async fn create_materialized_view(
        &self,
        request: Request<CreateMaterializedViewRequest>,
    ) -> Result<Response<CreateMaterializedViewResponse>, Status> {
        let req = request.into_inner();
        let mut fragmenter = StreamFragmenter::new(
            self.id_gen_manager_ref.clone(),
            self.cluster_manager.clone(),
        );
        let graph = fragmenter
            .generate_graph(req.get_stream_node())
            .await
            .map_err(|e| e.to_grpc_status())?;

        match self
            .sm
            .create_materialized_view(req.get_table_ref_id(), &graph)
            .await
        {
            Ok(()) => Ok(Response::new(CreateMaterializedViewResponse {
                status: None,
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    async fn drop_materialized_view(
        &self,
        _request: Request<DropMaterializedViewRequest>,
    ) -> Result<Response<DropMaterializedViewResponse>, Status> {
        todo!()
    }
}
