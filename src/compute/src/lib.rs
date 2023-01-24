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

#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(generators)]
#![feature(type_alias_impl_trait)]
#![feature(let_chains)]
#![feature(result_option_inspect)]
#![feature(lint_reasons)]
#![cfg_attr(coverage, feature(no_coverage))]

#[macro_use]
extern crate tracing;

pub mod memory_management;
pub mod rpc;
pub mod server;

use clap::Parser;
use risingwave_common::config::{
    load_config, AsyncStackTraceOption, ComputeNodeConfig, OverwriteConfig, RwConfig,
};
use risingwave_common::util::resource_util::cpu::total_cpu_available;
use risingwave_common::util::resource_util::memory::total_memory_available_bytes;

/// Command-line arguments for compute-node.
#[derive(Parser, Clone, Debug)]
pub struct ComputeNodeOpts {
    // TODO: rename to listen_address and separate out the port.
    #[clap(long)]
    pub listen_addr: Option<String>,

    #[clap(long)]
    pub client_address: Option<String>,

    #[clap(long)]
    pub state_store: Option<String>,

    #[clap(long)]
    pub prometheus_listener_addr: Option<String>,

    #[clap(long)]
    pub metrics_level: Option<u32>,

    #[clap(long)]
    pub meta_address: Option<String>,

    #[clap(long)]
    pub enable_jaeger_tracing: Option<bool>,

    #[clap(long, arg_enum)]
    pub async_stack_trace: Option<AsyncStackTraceOption>,

    #[clap(long)]
    pub file_cache_dir: Option<String>,

    /// Default value is read from the 'CONNECTOR_RPC_ENDPOINT' environment variable.
    #[clap(long, env = "CONNECTOR_RPC_ENDPOINT")]
    pub connector_rpc_endpoint: Option<String>,

    #[clap(long)]
    pub total_memory_bytes: Option<usize>,

    #[clap(long)]
    pub parallelism: Option<usize>,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    #[clap(long, default_value = "")]
    pub config_path: String,
}

impl OverwriteConfig for ComputeNodeOpts {
    fn overwrite(self, config: &mut RwConfig) {
        let c = &mut config.compute_node;
        if let Some(v) = self.listen_addr {
            c.listen_addr = v;
        }
        if self.client_address.is_some() {
            c.client_address = self.client_address;
        }
        if let Some(v) = self.state_store {
            c.state_store = v;
        }
        if let Some(v) = self.prometheus_listener_addr {
            c.prometheus_listener_addr = v;
        }
        if let Some(v) = self.metrics_level {
            c.metrics_level = v;
        }
        if let Some(v) = self.meta_address {
            c.meta_address = v;
        }
        if let Some(v) = self.enable_jaeger_tracing {
            c.enable_jaeger_tracing = v;
        }
        if let Some(v) = self.async_stack_trace {
            c.async_stack_trace = v;
        }
        if let Some(v) = self.file_cache_dir {
            c.file_cache_dir = v;
        }
        if self.connector_rpc_endpoint.is_some() {
            c.connector_rpc_endpoint = self.connector_rpc_endpoint;
        }
        if let Some(v) = self.total_memory_bytes {
            c.total_memory_bytes = v;
        }
        if let Some(v) = self.parallelism {
            c.parallelism = v;
        }
    }
}

fn validate_config(config: &ComputeNodeConfig) {
    let total_memory_available_bytes = total_memory_available_bytes();
    if config.total_memory_bytes > total_memory_available_bytes {
        let error_msg = format!("total_memory_bytes {} is larger than the total memory available bytes {} that can be acquired.", config.total_memory_bytes, total_memory_available_bytes);
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }
    if config.parallelism == 0 {
        let error_msg = "parallelism should not be zero";
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }
    let total_cpu_available = total_cpu_available().ceil() as usize;
    if config.parallelism > total_cpu_available {
        let error_msg = format!(
            "parallelism {} is larger than the total cpu available {} that can be acquired.",
            config.parallelism, total_cpu_available
        );
        tracing::warn!(error_msg);
    }
}

use std::future::Future;
use std::pin::Pin;

use crate::server::compute_node_serve;

/// Start compute node
pub fn start(opts: ComputeNodeOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        tracing::info!("Compute node options: {:?}", opts);

        let config = load_config(&opts.config_path.clone(), Some(opts));
        validate_config(&config.compute_node);

        let listen_address = config.compute_node.listen_addr.parse().unwrap();
        tracing::info!("Server Listening at {}", listen_address);

        let client_address = config
            .compute_node
            .client_address
            .as_ref()
            .unwrap_or_else(|| {
                tracing::warn!("Client address is not specified, defaulting to client address");
                &config.compute_node.listen_addr
            })
            .parse()
            .unwrap();
        tracing::info!("Client address is {}", client_address);

        let (join_handle_vec, _shutdown_send) =
            compute_node_serve(listen_address, client_address, config).await;

        for join_handle in join_handle_vec {
            join_handle.await.unwrap();
        }
    })
}
