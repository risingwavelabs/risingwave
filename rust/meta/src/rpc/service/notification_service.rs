use risingwave_common::error::tonic_err;
use risingwave_common::try_match_expand;
use risingwave_pb::meta::notification_service_server::NotificationService;
use risingwave_pb::meta::SubscribeRequest;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::manager::{Notification, NotificationManagerRef};

pub struct NotificationServiceImpl {
    nm: NotificationManagerRef,
}

impl NotificationServiceImpl {
    pub fn new(nm: NotificationManagerRef) -> Self {
        Self { nm }
    }
}

#[async_trait::async_trait]
impl NotificationService for NotificationServiceImpl {
    type SubscribeStream = ReceiverStream<Notification>;

    #[cfg(not(tarpaulin_include))]
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let worker_type = req.get_worker_type().map_err(tonic_err)?;
        let host_address = try_match_expand!(req.host, Some, "SubscribeRequest::host is empty")
            .map_err(|e| e.to_grpc_status())?;

        let rx = self
            .nm
            .subscribe(host_address, worker_type)
            .map_err(|e| e.to_grpc_status())?;

        Ok(Response::new(rx))
    }
}
