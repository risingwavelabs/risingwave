// Copyright 2023 RisingWave Labs
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

// TODO: Which of these features do we need?
#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(option_result_contains)]
#![feature(type_alias_impl_trait)]
#![feature(drain_filter)]
#![feature(custom_test_frameworks)]
#![feature(lint_reasons)]
#![feature(map_try_insert)]
#![feature(hash_drain_filter)]
#![feature(is_some_and)]
#![feature(btree_drain_filter)]
#![feature(result_option_inspect)]
#![feature(once_cell)]
#![feature(let_chains)]
#![feature(error_generic_member_access)]
#![feature(provide_any)]
#![feature(assert_matches)]
#![feature(try_blocks)]
#![cfg_attr(coverage, feature(no_coverage))]
#![test_runner(risingwave_test_runner::test_runner::run_failpont_tests)]
#![feature(is_sorted)]

use clap::Parser;

#[derive(Debug, Clone, Parser)]
pub struct MetaProbeOpts {
    /// address at which to probe the meta container. Format is "127.0.0.1:PORT"
    #[clap(long, env = "RW_LISTEN_ADDR", default_value = "127.0.0.1:5690")]
    listen_addr: String,
}

use std::net::SocketAddr;
use std::time::Duration;

use risingwave_common::config::MAX_CONNECTION_WINDOW_SIZE;
use risingwave_pb::meta::heartbeat_request::{extra_info, ExtraInfo};
use risingwave_pb::meta::heartbeat_service_client::HeartbeatServiceClient;
use risingwave_pb::meta::HeartbeatRequest;
use risingwave_rpc_client::error::{Result, RpcError};
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tonic::transport::{Channel, Endpoint};
use tracing::info;

// ,{ heartbeat_client, heartbeat, HeartbeatRequest, HeartbeatResponse }

const CONN_RETRY_MAX_INTERVAL_MS: u64 = 100;
const CONN_RETRY_BASE_INTERVAL_MS: u64 = 20;
const ENDPOINT_KEEP_ALIVE_INTERVAL_SEC: u64 = 1;
const ENDPOINT_KEEP_ALIVE_TIMEOUT_SEC: u64 = 1;
const MAX_RETRIES: usize = 3;

async fn connect_to_endpoint(endpoint: Endpoint) -> Result<Channel> {
    endpoint
        .http2_keep_alive_interval(Duration::from_secs(ENDPOINT_KEEP_ALIVE_INTERVAL_SEC))
        .keep_alive_timeout(Duration::from_secs(ENDPOINT_KEEP_ALIVE_TIMEOUT_SEC))
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .map_err(RpcError::TransportError)
}

fn addr_to_endpoint(addr: String) -> Result<Endpoint> {
    Endpoint::from_shared(addr)
        .map(|endpoint| endpoint.initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE))
        .map_err(RpcError::TransportError)
}

async fn try_build_rpc_channel(addr: String) -> Result<Channel> {
    let endpoint = addr_to_endpoint(addr).expect("Expected that a valid meta address");

    let retry_strategy = ExponentialBackoff::from_millis(CONN_RETRY_BASE_INTERVAL_MS)
        .max_delay(Duration::from_millis(CONN_RETRY_MAX_INTERVAL_MS))
        .map(jitter)
        .take(MAX_RETRIES);

    let channel_res = tokio_retry::Retry::spawn(retry_strategy, || async {
        let endpoint_clone = endpoint.clone();
        match connect_to_endpoint(endpoint_clone).await {
            Ok(channel) => {
                return Ok(channel);
            }
            Err(e) => return Err(e),
        }
    })
    .await;
    match channel_res {
        Ok(channel) => Ok(channel),
        Err(e) => Err(e),
    }
}

// Returns false if heartbeat timed out
async fn heartbeat_ok(request: HeartbeatRequest, addr: String) -> bool {
    let channel = match try_build_rpc_channel(addr).await {
        Ok(c) => c,
        Err(_) => return false,
    };
    let mut heartbeat_client = HeartbeatServiceClient::new(channel.clone());

    match heartbeat_client.heartbeat(request).await {
        Ok(_) => true,
        Err(e) => {
            if e.code() == tonic::Code::DeadlineExceeded {
                false;
            }
            true
        }
    }
}

fn get_dummy_request() -> HeartbeatRequest {
    HeartbeatRequest {
        node_id: u32::MAX,
        info: vec![]
            .into_iter()
            .map(|info| ExtraInfo { info: Some(info) })
            .collect(),
    }
}

/// True if meta node is up
pub async fn ok(opts: MetaProbeOpts) -> bool {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    info!("Starting probe");
    info!("> options: {:?}", opts);
    // TODO: Maybe remove this. We can check somewhere else if this addr is valid
    let listen_addr: SocketAddr = opts
        .listen_addr
        .parse()
        .expect("Expected a valid listen address");
    info!("probing on {}", listen_addr);
    heartbeat_ok(get_dummy_request(), listen_addr.to_string()).await
}
