use std::net::SocketAddr;

use risingwave_common::error::ErrorCode::MetaError;
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_pb::common::HostAddress;
use risingwave_pb::meta::cluster_service_client::ClusterServiceClient;
use risingwave_pb::meta::heartbeat_service_client::HeartbeatServiceClient;
use risingwave_pb::meta::{AddWorkerNodeRequest, ClusterType, HeartbeatRequest};
use tonic::transport::Channel;

pub struct MetaClient {
    pub cluster_client: ClusterServiceClient<Channel>,
    pub heartbeat_client: HeartbeatServiceClient<Channel>,
}

impl MetaClient {
    pub async fn new(endpoint: &str) -> Result<Self> {
        let cluster_client = ClusterServiceClient::connect(endpoint.to_owned())
            .await
            .to_rw_result()?;
        let heartbeat_client = HeartbeatServiceClient::connect(endpoint.to_owned())
            .await
            .to_rw_result()?;
        // TODO: add some mechanism on connection overtime.

        Ok(Self {
            cluster_client,
            heartbeat_client,
        })
    }

    /// Register this cluster and return the corresponding worker id.
    pub async fn register(&self, addr: SocketAddr) -> Result<u32> {
        let host_address = HostAddress {
            host: addr.ip().to_string(),
            port: addr.port() as i32,
        };
        let request = AddWorkerNodeRequest {
            cluster_type: ClusterType::ComputeNode as i32,
            host: Some(host_address),
        };
        let res = self
            .cluster_client
            .to_owned()
            .add_worker_node(request)
            .await;
        match res {
            Ok(res_inner) => match res_inner.into_inner().node {
                Some(worker_node) => Ok(worker_node.id),
                _ => Err(RwError::from(MetaError("".to_string()))),
            },
            _ => Err(RwError::from(MetaError(
                "worker node already registered".to_string(),
            ))),
        }
    }

    /// Send heartbeat signal to meta service.
    pub async fn send_heartbeat(&self, node_id: u32) -> Result<()> {
        let request = HeartbeatRequest {
            node_id,
            cluster_type: ClusterType::ComputeNode as i32,
        };
        let res = self.heartbeat_client.to_owned().heartbeat(request).await;
        match res {
            Ok(_res_inner) => Ok(()),
            _ => Err(RwError::from(MetaError("".to_string()))),
        }
    }
}
