use std::net::SocketAddr;
use std::time::Duration;

use risingwave_common::array::InternalError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::task_service::exchange_service_client::ExchangeServiceClient;
use tonic::transport::{Channel, Endpoint};

pub struct ComputeClient {
    pub channel: ExchangeServiceClient<Channel>,
}

impl ComputeClient {
    pub async fn new(addr: &SocketAddr) -> Result<Self> {
        let client = ExchangeServiceClient::new(
            Endpoint::from_shared(format!("http://{}", *addr))
                .map_err(|e| InternalError(format!("{}", e)))?
                .connect_timeout(Duration::from_secs(5))
                .connect()
                .await
                .to_rw_result_with(format!("failed to connect to {}", *addr))?,
        );
        Ok(Self { channel: client })
    }

    pub fn get_channel(&self) -> ExchangeServiceClient<Channel> {
        self.channel.clone()
    }
}
