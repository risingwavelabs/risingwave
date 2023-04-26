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
pub mod observer;
pub mod rpc;
pub mod server;
pub mod telemetry;

use clap::{Parser, ValueEnum};
use risingwave_common::config::AsyncStackTraceOption;
use risingwave_common::util::resource_util::cpu::total_cpu_available;
use risingwave_common::util::resource_util::memory::total_memory_available_bytes;
use risingwave_common_proc_macro::OverrideConfig;
use serde::{Deserialize, Serialize};

/// Command-line arguments for compute-node.
#[derive(Parser, Clone, Debug)]
pub struct ComputeNodeOpts {
    // TODO: rename to listen_addr and separate out the port.
    /// The address that this service listens to.
    /// Usually the localhost + desired port.
    #[clap(long, env = "RW_LISTEN_ADDR", default_value = "127.0.0.1:5688")]
    pub listen_addr: String,

    /// The address for contacting this instance of the service.
    /// This would be synonymous with the service's "public address"
    /// or "identifying address".
    /// Optional, we will use listen_addr if not specified.
    #[clap(long, env = "RW_ADVERTISE_ADDR", long)]
    pub advertise_addr: Option<String>,

    #[clap(
        long,
        env = "RW_PROMETHEUS_LISTENER_ADDR",
        default_value = "127.0.0.1:1222"
    )]
    pub prometheus_listener_addr: String,

    #[clap(long, env = "RW_META_ADDR", default_value = "http://127.0.0.1:5690")]
    pub meta_address: String,

    /// Endpoint of the connector node
    #[clap(long, env = "RW_CONNECTOR_RPC_ENDPOINT")]
    pub connector_rpc_endpoint: Option<String>,

    /// Payload format of connector sink rpc
    #[clap(long, env = "RW_CONNECTOR_RPC_SINK_PAYLOAD_FORMAT")]
    pub connector_rpc_sink_payload_format: Option<String>,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    #[clap(long, env = "RW_CONFIG_PATH", default_value = "")]
    pub config_path: String,

    /// Total available memory for the compute node in bytes. Used by both computing and storage.
    #[clap(long, env = "RW_TOTAL_MEMORY_BYTES", default_value_t = default_total_memory_bytes())]
    pub total_memory_bytes: usize,

    /// The parallelism that the compute node will register to the scheduler of the meta service.
    #[clap(long, env = "RW_PARALLELISM", default_value_t = default_parallelism())]
    pub parallelism: usize,

    /// Decides whether the compute node can be used for streaming and serving.
    #[clap(long, env = "RW_COMPUTE_NODE_ROLE", value_enum, default_value_t = default_role())]
    pub role: Role,

    #[clap(flatten)]
    override_config: OverrideConfigOpts,
}

/// Command-line arguments for compute-node that overrides the config file.
#[derive(Parser, Clone, Debug, OverrideConfig)]
struct OverrideConfigOpts {
    /// Used for control the metrics level, similar to log level.
    /// 0 = close metrics
    /// >0 = open metrics
    #[clap(long, env = "RW_METRICS_LEVEL")]
    #[override_opts(path = server.metrics_level)]
    pub metrics_level: Option<u32>,

    /// Path to file cache data directory.
    /// Left empty to disable file cache.
    #[clap(long, env = "RW_FILE_CACHE_DIR")]
    #[override_opts(path = storage.file_cache.dir)]
    pub file_cache_dir: Option<String>,

    /// Enable reporting tracing information to jaeger.
    #[clap(long, env = "RW_ENABLE_JAEGER_TRACING", default_missing_value = None)]
    #[override_opts(path = streaming.enable_jaeger_tracing)]
    pub enable_jaeger_tracing: Option<bool>,

    /// Enable async stack tracing through `await-tree` for risectl.
    #[clap(long, env = "RW_ASYNC_STACK_TRACE", value_enum)]
    #[override_opts(path = streaming.async_stack_trace)]
    pub async_stack_trace: Option<AsyncStackTraceOption>,
}

#[derive(Copy, Clone, Debug, Default, ValueEnum, Serialize, Deserialize)]
pub enum Role {
    Serving,
    Streaming,
    #[default]
    Both,
}

impl Role {
    pub fn for_streaming(&self) -> bool {
        match self {
            Role::Serving => false,
            Role::Streaming => true,
            Role::Both => true,
        }
    }

    pub fn for_serving(&self) -> bool {
        match self {
            Role::Serving => true,
            Role::Streaming => false,
            Role::Both => true,
        }
    }
}

fn validate_opts(opts: &ComputeNodeOpts) {
    let total_memory_available_bytes = total_memory_available_bytes();
    if opts.total_memory_bytes > total_memory_available_bytes {
        let error_msg = format!("total_memory_bytes {} is larger than the total memory available bytes {} that can be acquired.", opts.total_memory_bytes, total_memory_available_bytes);
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }
    if opts.parallelism == 0 {
        let error_msg = "parallelism should not be zero";
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }
    let total_cpu_available = total_cpu_available().ceil() as usize;
    if opts.parallelism > total_cpu_available {
        let error_msg = format!(
            "parallelism {} is larger than the total cpu available {} that can be acquired.",
            opts.parallelism, total_cpu_available
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
        tracing::info!("options: {:?}", opts);
        validate_opts(&opts);

        let listen_addr = opts.listen_addr.parse().unwrap();
        tracing::info!("Server Listening at {}", listen_addr);

        let advertise_addr = opts
            .advertise_addr
            .as_ref()
            .unwrap_or_else(|| {
                tracing::warn!("advertise addr is not specified, defaulting to listen_addr");
                &opts.listen_addr
            })
            .parse()
            .unwrap();
        tracing::info!("advertise addr is {}", advertise_addr);

        let (join_handle_vec, _shutdown_send) =
            compute_node_serve(listen_addr, advertise_addr, opts).await;

        for join_handle in join_handle_vec {
            join_handle.await.unwrap();
        }
    })
}

fn default_total_memory_bytes() -> usize {
    total_memory_available_bytes()
}

fn default_parallelism() -> usize {
    total_cpu_available().ceil() as usize
}

fn default_role() -> Role {
    Role::Both
}
