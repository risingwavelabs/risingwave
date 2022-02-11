use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_common::error::tonic_err;
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerService;
use risingwave_pb::meta::*;
use tonic::{Request, Response, Status};

use crate::cluster::StoredClusterManager;
use crate::manager::{EpochGeneratorRef, IdGeneratorManagerRef, MetaSrvEnv};
use crate::model::TableFragments;
use crate::stream::{StreamFragmenter, StreamManagerRef};

pub type TonicResponse<T> = Result<Response<T>, Status>;

#[derive(Clone)]
pub struct StreamServiceImpl {
    sm: StreamManagerRef,

    id_gen_manager_ref: IdGeneratorManagerRef,
    cluster_manager: Arc<StoredClusterManager>,

    #[allow(dead_code)]
    epoch_generator: EpochGeneratorRef,
}

impl StreamServiceImpl {
    pub fn new(
        sm: StreamManagerRef,
        cluster_manager: Arc<StoredClusterManager>,
        env: MetaSrvEnv,
    ) -> Self {
        StreamServiceImpl {
            sm,
            id_gen_manager_ref: env.id_gen_manager_ref(),
            cluster_manager,
            epoch_generator: env.epoch_generator_ref(),
        }
    }
}

#[async_trait::async_trait]
impl StreamManagerService for StreamServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn create_materialized_view(
        &self,
        request: Request<CreateMaterializedViewRequest>,
    ) -> TonicResponse<CreateMaterializedViewResponse> {
        let req = request.into_inner();
        let worker_count = self
            .cluster_manager
            .get_worker_count(WorkerType::ComputeNode);

        let mut fragmenter = StreamFragmenter::new(self.id_gen_manager_ref.clone(), worker_count as u32);
        let graph = fragmenter
            .generate_graph(req.get_stream_node().map_err(tonic_err)?)
            .await
            .map_err(|e| e.to_grpc_status())?;

        let table_fragments = TableFragments::new(TableId::from(&req.table_ref_id), graph);
        match self.sm.create_materialized_view(table_fragments).await {
            Ok(()) => Ok(Response::new(CreateMaterializedViewResponse {
                status: None,
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn drop_materialized_view(
        &self,
        request: Request<DropMaterializedViewRequest>,
    ) -> TonicResponse<DropMaterializedViewResponse> {
        let _req = request.into_inner();

        // FIXME: We can't handle drop mv on mv now. Since TABLE_V2 is enabled, dropping
        // materialized view on backend is temporarily disabled.
        return Ok(Response::new(DropMaterializedViewResponse { status: None }));

        #[allow(unreachable_code)]
        match self
            .sm
            .drop_materialized_view(_req.get_table_ref_id().map_err(tonic_err)?)
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
