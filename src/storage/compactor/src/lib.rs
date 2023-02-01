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

mod compactor_observer;
mod rpc;
mod server;

use clap::Parser;
use risingwave_common_proc_macro::OverrideConfig;

use crate::server::compactor_serve;

/// Command-line arguments for compute-node.
#[derive(Parser, Clone, Debug)]
pub struct CompactorOpts {
    // TODO: rename to listen_address and separate out the port.
    #[clap(long, default_value = "127.0.0.1:6660")]
    pub host: String,

    // Optional, we will use listen_address if not specified.
    #[clap(long)]
    pub client_address: Option<String>,

    // TODO: This is currently unused.
    #[clap(long)]
    pub port: Option<u16>,

    #[clap(long, default_value = "127.0.0.1:1260")]
    pub prometheus_listener_addr: String,

    #[clap(long, default_value = "http://127.0.0.1:5690")]
    pub meta_address: String,

    #[clap(long)]
    pub compaction_worker_threads_number: Option<usize>,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    #[clap(long, default_value = "")]
    pub config_path: String,

    #[clap(flatten)]
    override_config: OverrideConfigOpts,
}

/// Command-line arguments for compactor-node that overrides the config file.
#[derive(Parser, Clone, Debug, OverrideConfig)]
struct OverrideConfigOpts {
    /// Of the form `hummock+{object_store}` where `object_store`
    /// is one of `s3://{path}`, `s3-compatible://{path}`, `minio://{path}`, `disk://{path}`,
    /// `memory` or `memory-shared`.
    #[clap(long)]
    #[override_opts(path = storage.state_store)]
    pub state_store: Option<String>,

    /// Used for control the metrics level, similar to log level.
    /// 0 = close metrics
    /// >0 = open metrics
    #[clap(long)]
    #[override_opts(path = server.metrics_level)]
    pub metrics_level: Option<u32>,

    /// It's a hint used by meta node.
    #[clap(long)]
    #[override_opts(path = storage.max_concurrent_compaction_task_number)]
    pub max_concurrent_task_number: Option<u64>,
}

use std::future::Future;
use std::pin::Pin;

pub fn start(opts: CompactorOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        tracing::info!("Compactor node options: {:?}", opts);
        tracing::info!("meta address: {}", opts.meta_address.clone());

        let listen_address = opts.host.parse().unwrap();
        tracing::info!("Server Listening at {}", listen_address);

        let client_address = opts
            .client_address
            .as_ref()
            .unwrap_or_else(|| {
                tracing::warn!("Client address is not specified, defaulting to host address");
                &opts.host
            })
            .parse()
            .unwrap();
        tracing::info!("Client address is {}", client_address);

        let (join_handle, observer_join_handle, _shutdown_sender) =
            compactor_serve(listen_address, client_address, opts).await;

        join_handle.await.unwrap();
        observer_join_handle.await.unwrap();
    })
}
