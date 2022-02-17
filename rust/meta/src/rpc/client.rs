use std::time::Duration;

use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::common::HostAddress;
use risingwave_pb::meta::info_notify_request::{Info, Operation};
use risingwave_pb::meta::info_notify_service_client::InfoNotifyServiceClient;
use risingwave_pb::meta::{InfoNotifyRequest};
use tonic::transport::{Channel, Endpoint};

use crate::model::{MetadataModel, Worker};

/// Client used to notify meta information.
pub struct InfoNotifyClient(InfoNotifyServiceClient<Channel>);

impl InfoNotifyClient {
    pub async fn new(host_address: &HostAddress) -> Result<Self> {
        let addr = host_address.to_socket_addr()?;
        let channel = Endpoint::from_shared(addr.to_string())
            .map_err(|e| InternalError(format!("{}", e)))?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .to_rw_result_with(format!("failed to connect to {}", addr))?;
        Ok(Self(InfoNotifyServiceClient::new(channel)))
    }

    pub async fn refresh_compute_addr(&self, worker: &Worker) -> Result<()> {
        let request = InfoNotifyRequest {
            operation: Operation::Add as i32,
            info: Some(Info::Node(worker.to_protobuf())),
        };

        let _resp = self
            .0
            .to_owned()
            .notify(request)
            .await
            .to_rw_result()?
            .into_inner();

        Ok(())
    }
}
