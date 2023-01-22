// Copyright 2023 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use risingwave_pb::health::health_server::HealthServer;
use risingwave_pb::leader::leader_service_server::LeaderServiceServer;
use risingwave_pb::meta::MetaLeaderInfo;
use tokio::sync::oneshot::Receiver as OneReceiver;
use tokio::sync::watch::Receiver as WatchReceiver;

use super::intercept::MetricsMiddlewareLayer;
use super::server::AddressInfo;
use super::service::health_service::HealthServiceImpl;
use crate::rpc::metrics::MetaMetrics;
use crate::rpc::server::ElectionClientRef;
use crate::rpc::service::leader_service::LeaderServiceImpl;

/// Starts all services needed for the meta follower node
pub async fn start_follower_srv(
    mut svc_shutdown_rx: WatchReceiver<()>,
    follower_shutdown_rx: OneReceiver<()>,
    address_info: AddressInfo,
    election_client: Option<ElectionClientRef>,
) {
    let leader_srv = LeaderServiceImpl::new(
        election_client,
        MetaLeaderInfo {
            node_address: address_info.listen_addr.to_string(),
            lease_id: 0,
        },
    );

    let health_srv = HealthServiceImpl::new();
    tonic::transport::Server::builder()
        .layer(MetricsMiddlewareLayer::new(Arc::new(MetaMetrics::new())))
        .add_service(LeaderServiceServer::new(leader_srv))
        .add_service(HealthServer::new(health_srv))
        .serve_with_shutdown(address_info.listen_addr, async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                // shutdown service if all services should be shut down
                res = svc_shutdown_rx.changed() =>  {
                    match res {
                        Ok(_) => tracing::info!("Shutting down services"),
                        Err(_) => tracing::error!("Service shutdown sender dropped")
                    }
                },
                // shutdown service if follower becomes leader
                res = follower_shutdown_rx =>  {
                    match res {
                        Ok(_) => tracing::info!("Shutting down follower services"),
                        Err(_) => tracing::error!("Follower service shutdown sender dropped")
                    }
                },
            }
        })
        .await
        .unwrap();
}
