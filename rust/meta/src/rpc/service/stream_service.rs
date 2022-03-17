use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_common::error::tonic_err;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerService;
use risingwave_pb::meta::*;
use tonic::{Request, Response, Status};

use crate::cluster::StoredClusterManager;
use crate::manager::{EpochGeneratorRef, IdGeneratorManagerRef, MetaSrvEnv};
use crate::model::TableFragments;
use crate::storage::MetaStore;
use crate::stream::{FragmentManagerRef, StreamFragmenter, StreamManagerRef};

pub type TonicResponse<T> = Result<Response<T>, Status>;

#[derive(Clone)]
pub struct StreamServiceImpl<S>
where
    S: MetaStore,
{
    sm: StreamManagerRef<S>,

    id_gen_manager_ref: IdGeneratorManagerRef<S>,
    fragment_manager_ref: FragmentManagerRef<S>,
    cluster_manager: Arc<StoredClusterManager<S>>,

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
        cluster_manager: Arc<StoredClusterManager<S>>,
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
    #[cfg(not(tarpaulin_include))]
    async fn create_materialized_view(
        &self,
        request: Request<CreateMaterializedViewRequest>,
    ) -> TonicResponse<CreateMaterializedViewResponse> {
        use risingwave_pb::common::ParallelUnitType;

        use crate::stream::CreateMaterializedViewContext;

        let req = request.into_inner();

        tracing::debug!(
            plan = serde_json::to_string_pretty(&req).unwrap().as_str(),
            "create materialized view"
        );

        let hash_parallel_count = self
            .cluster_manager
            .get_parallel_unit_count(Some(ParallelUnitType::Hash))
            .await;
        let mut ctx = CreateMaterializedViewContext::default();

        let mut fragmenter = StreamFragmenter::new(
            self.id_gen_manager_ref.clone(),
            self.fragment_manager_ref.clone(),
            hash_parallel_count as u32,
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

    #[cfg(not(tarpaulin_include))]
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
