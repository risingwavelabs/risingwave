// Copyright 2025 RisingWave Labs
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

#![feature(let_chains)]
#![feature(impl_trait_in_assoc_type)]
#![cfg_attr(coverage, feature(coverage_attribute))]

use risingwave_meta::*;

pub mod backup_service;
pub mod cloud_service;
pub mod cluster_limit_service;
pub mod cluster_service;
pub mod ddl_service;
pub mod event_log_service;
pub mod health_service;
pub mod heartbeat_service;
pub mod hosted_iceberg_catalog_service;
pub mod hummock_service;
pub mod meta_member_service;
pub mod monitor_service;
pub mod notification_service;
pub mod scale_service;
pub mod serving_service;
pub mod session_config;
pub mod sink_coordination_service;
pub mod stream_service;
pub mod system_params_service;
pub mod telemetry_service;
pub mod user_service;

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::MetaError;

/// `RwReceiverStream` is a wrapper around `tokio::sync::mpsc::UnboundedReceiver` that implements
/// Stream. `RwReceiverStream` is similar to `tokio_stream::wrappers::ReceiverStream`, but it
/// maps Result<S, `MetaError`> to Result<S, `tonic::Status`>.
pub struct RwReceiverStream<S: Send + Sync + 'static> {
    inner: UnboundedReceiver<Result<S, MetaError>>,
}

impl<S: Send + Sync + 'static> RwReceiverStream<S> {
    pub fn new(inner: UnboundedReceiver<Result<S, MetaError>>) -> Self {
        Self { inner }
    }
}

impl<S: Send + Sync + 'static> Stream for RwReceiverStream<S> {
    type Item = Result<S, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner
            .poll_recv(cx)
            .map(|opt| opt.map(|res| res.map_err(Into::into)))
    }
}

use std::net::SocketAddr;

#[derive(Clone)]
pub struct AddressInfo {
    pub advertise_addr: String,
    pub listen_addr: SocketAddr,
    pub prometheus_addr: Option<SocketAddr>,
    pub dashboard_addr: Option<SocketAddr>,
}
impl Default for AddressInfo {
    fn default() -> Self {
        Self {
            advertise_addr: "".to_owned(),
            listen_addr: SocketAddr::V4("127.0.0.1:0000".parse().unwrap()),
            prometheus_addr: None,
            dashboard_addr: None,
        }
    }
}
