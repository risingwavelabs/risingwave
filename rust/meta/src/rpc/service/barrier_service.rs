use async_trait::async_trait;
use risingwave_pb::meta::barrier_service_server::BarrierService;
use risingwave_pb::meta::{CollectBarrierRequest, CollectBarrierResponse};
use tonic::{Request, Response, Status};

use crate::barrier::BarrierManagerRef;

/// [`BarrierServiceImpl`] collects finished barriers from all compute nodes, and trigger new
/// barriers in `BarrierManager`.
#[derive(Clone)]
pub struct BarrierServiceImpl {
    manager: BarrierManagerRef,
}

impl BarrierServiceImpl {
    pub fn new(manager: BarrierManagerRef) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl BarrierService for BarrierServiceImpl {
    async fn collect_barrier(
        &self,
        _request: Request<CollectBarrierRequest>,
    ) -> Result<Response<CollectBarrierResponse>, Status> {
        todo!()
    }
}
