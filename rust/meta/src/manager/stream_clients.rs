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
use std::sync::Arc;
use std::time::Duration;

use moka::future::Cache;
use risingwave_common::error::ErrorCode::{self, InternalError};
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::stream_service::stream_service_client::StreamServiceClient;
use tonic::transport::{Channel, Endpoint};

use crate::cluster::NodeId;

pub type StreamClient = StreamServiceClient<Channel>;

/// [`StreamClients`] maintains stream service clients to known compute nodes.
pub struct StreamClients {
    /// Stores the [`StreamClient`] mapping: `node_id` => client.
    clients: Cache<NodeId, StreamClient>,
}

impl Default for StreamClients {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamClients {
    pub fn new() -> Self {
        Self {
            clients: Cache::new(u64::MAX),
        }
    }

    /// Get the stream service client for the given node. If the connection is not established, a
    /// new client will be created and returned.
    pub async fn get(&self, node: &WorkerNode) -> Result<StreamServiceClient<Channel>> {
        self.clients
            .get_or_try_insert_with(node.id, async {
                let addr = node.get_host()?.to_socket_addr()?;
                let endpoint = Endpoint::from_shared(format!("http://{}", addr));
                let client = StreamServiceClient::new(
                    endpoint
                        .map_err(|e| InternalError(e.to_string()))?
                        .connect_timeout(Duration::from_secs(5))
                        .connect()
                        .await
                        .to_rw_result_with(format!("failed to connect to {}", node.get_id()))?,
                );
                Ok::<_, RwError>(client)
            })
            .await
            .map_err(|e| {
                ErrorCode::InternalError(format!("failed to create compute client: {:?}", e)).into()
            })
    }

    pub fn get_by_node_id(&self, node_id: &NodeId) -> Option<StreamServiceClient<Channel>> {
        self.clients.get(node_id)
    }
}

pub type StreamClientsRef = Arc<StreamClients>;
