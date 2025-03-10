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

mod compactor_observer;
mod rpc;
pub mod server;
mod telemetry;

use clap::Parser;
use risingwave_common::config::{
    AsyncStackTraceOption, CompactorMode, MetricLevel, OverrideConfig,
};
use risingwave_common::util::meta_addr::MetaAddressStrategy;
use risingwave_common::util::resource_util::memory::system_memory_available_bytes;
use risingwave_common::util::tokio_util::sync::CancellationToken;

use crate::server::{compactor_serve, shared_compactor_serve};

/// Command-line arguments for compactor-node.
#[derive(Parser, Clone, Debug, OverrideConfig)]
#[command(
    version,
    about = "The stateless worker node that compacts data for the storage engine"
)]
pub struct CompactorOpts {
    // TODO: rename to listen_addr and separate out the port.
    /// The address that this service listens to.
    /// Usually the localhost + desired port.
    #[clap(long, env = "RW_LISTEN_ADDR", default_value = "127.0.0.1:6660")]
    pub listen_addr: String,

    /// The address for contacting this instance of the service.
    /// This would be synonymous with the service's "public address"
    /// or "identifying address".
    /// Optional, we will use `listen_addr` if not specified.
    #[clap(long, env = "RW_ADVERTISE_ADDR")]
    pub advertise_addr: Option<String>,

    // TODO(eric): remove me
    // TODO: This is currently unused.
    #[clap(long, env = "RW_PORT")]
    pub port: Option<u16>,

    /// We will start a http server at this address via `MetricsManager`.
    /// Then the prometheus instance will poll the metrics from this address.
    #[clap(
        long,
        env = "RW_PROMETHEUS_LISTENER_ADDR",
        default_value = "127.0.0.1:1260"
    )]
    pub prometheus_listener_addr: String,

    #[clap(long, env = "RW_META_ADDR", default_value = "http://127.0.0.1:5690")]
    pub meta_address: MetaAddressStrategy,

    #[clap(long, env = "RW_COMPACTION_WORKER_THREADS_NUMBER")]
    pub compaction_worker_threads_number: Option<usize>,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    #[clap(long, env = "RW_CONFIG_PATH", default_value = "")]
    pub config_path: String,

    /// Used for control the metrics level, similar to log level.
    #[clap(long, hide = true, env = "RW_METRICS_LEVEL")]
    #[override_opts(path = server.metrics_level)]
    pub metrics_level: Option<MetricLevel>,

    /// Enable async stack tracing through `await-tree` for risectl.
    #[clap(long, hide = true, env = "RW_ASYNC_STACK_TRACE", value_enum)]
    #[override_opts(path = streaming.async_stack_trace)]
    pub async_stack_trace: Option<AsyncStackTraceOption>,

    /// Enable heap profile dump when memory usage is high.
    #[clap(long, hide = true, env = "RW_HEAP_PROFILING_DIR")]
    #[override_opts(path = server.heap_profiling.dir)]
    pub heap_profiling_dir: Option<String>,

    #[clap(long, env = "RW_COMPACTOR_MODE", value_enum)]
    pub compactor_mode: Option<CompactorMode>,

    #[clap(long, hide = true, env = "RW_PROXY_RPC_ENDPOINT", default_value = "")]
    pub proxy_rpc_endpoint: String,

    /// Total available memory for the frontend node in bytes. Used by compactor.
    #[clap(long, env = "RW_COMPACTOR_TOTAL_MEMORY_BYTES", default_value_t = default_compactor_total_memory_bytes())]
    pub compactor_total_memory_bytes: usize,

    #[clap(long, env = "RW_COMPACTOR_META_CACHE_MEMORY_BYTES", default_value_t = default_compactor_meta_cache_memory_bytes())]
    pub compactor_meta_cache_memory_bytes: usize,
}

impl risingwave_common::opts::Opts for CompactorOpts {
    fn name() -> &'static str {
        "compactor"
    }

    fn meta_addr(&self) -> MetaAddressStrategy {
        self.meta_address.clone()
    }
}

use std::future::Future;
use std::pin::Pin;

pub fn start(
    opts: CompactorOpts,
    shutdown: CancellationToken,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    match opts.compactor_mode {
        Some(CompactorMode::Shared) => Box::pin(async move {
            tracing::info!("Shared compactor pod options: {:?}", opts);
            tracing::info!("Proxy rpc endpoint: {}", opts.proxy_rpc_endpoint.clone());

            let listen_addr = opts.listen_addr.parse().unwrap();

            shared_compactor_serve(listen_addr, opts, shutdown).await;
        }),
        None | Some(CompactorMode::Dedicated) => Box::pin(async move {
            tracing::info!("Compactor node options: {:?}", opts);
            tracing::info!("meta address: {}", opts.meta_address.clone());

            let listen_addr = opts.listen_addr.parse().unwrap();

            let advertise_addr = opts
                .advertise_addr
                .as_ref()
                .unwrap_or_else(|| {
                    tracing::warn!("advertise addr is not specified, defaulting to listen address");
                    &opts.listen_addr
                })
                .parse()
                .unwrap();
            tracing::info!(" address is {}", advertise_addr);

            compactor_serve(listen_addr, advertise_addr, opts, shutdown).await;
        }),
    }
}

pub fn default_compactor_total_memory_bytes() -> usize {
    system_memory_available_bytes()
}

pub fn default_compactor_meta_cache_memory_bytes() -> usize {
    128 * 1024 * 1024 // 128MB
}
