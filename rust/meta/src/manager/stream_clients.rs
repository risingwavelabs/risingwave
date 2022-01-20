use std::sync::Arc;
use std::time::Duration;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_common::util::addr::get_host_port;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::stream_service::stream_service_client::StreamServiceClient;
use tonic::transport::{Channel, Endpoint};

/// [`StreamClients`] maintains stream service clients to known compute nodes.
#[derive(Default)]
pub struct StreamClients {
    /// Stores the [`StreamServiceClient`] mapping: `node_id` => client.
    clients: DashMap<u32, StreamServiceClient<Channel>>,
}

impl StreamClients {
    /// Get the stream service client for the given node. If the connection is not established, a
    /// new client will be created and returned.
    pub async fn get(&self, node: &WorkerNode) -> Result<StreamServiceClient<Channel>> {
        let client = match self.clients.entry(node.id) {
            Entry::Occupied(o) => o.get().to_owned(),
            Entry::Vacant(v) => {
                let addr = get_host_port(
                    format!(
                        "{}:{}",
                        node.get_host().get_host(),
                        node.get_host().get_port()
                    )
                    .as_str(),
                )
                .unwrap();
                let endpoint = Endpoint::from_shared(format!("http://{}", addr));
                let client = StreamServiceClient::new(
                    endpoint
                        .map_err(|e| InternalError(e.to_string()))?
                        .connect_timeout(Duration::from_secs(5))
                        .connect()
                        .await
                        .to_rw_result_with(format!("failed to connect to {}", node.get_id()))?,
                );
                v.insert(client).to_owned()
            }
        };

        Ok(client)
    }
}

pub type StreamClientsRef = Arc<StreamClients>;
