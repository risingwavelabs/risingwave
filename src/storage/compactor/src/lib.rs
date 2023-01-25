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

mod compactor_observer;
mod rpc;
mod server;

use clap::Parser;

use crate::server::compactor_serve;

/// Command-line arguments for compute-node.
#[derive(Parser, Clone, Debug)]
pub struct CompactorOpts {
    // TODO: rename to listen_address and separate out the port.
    /// The address for this service to listen to locally
    #[clap(long, default_value = "127.0.0.1:6660")]
    pub listen_address: String,

    /// The address for contacting this instance of the frontend service.
    /// Optional, we will use listen_address if not specified.
    #[clap(long)]
    pub contact_address: Option<String>,

    // TODO: This is currently unused.
    #[clap(long)]
    pub port: Option<u16>,

    #[clap(long, default_value = "")]
    pub state_store: String,

    #[clap(long, default_value = "127.0.0.1:1260")]
    pub prometheus_listener_addr: String,

    #[clap(long, default_value = "0")]
    pub metrics_level: u32,

    #[clap(long, default_value = "http://127.0.0.1:5690")]
    pub meta_address: String,

    /// It's a hint used by meta node.
    #[clap(long, default_value = "16")]
    pub max_concurrent_task_number: u64,

    #[clap(long)]
    pub compaction_worker_threads_number: Option<usize>,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    ///
    /// Note that internal system parameters should be defined in the configuration file at
    /// [`risingwave_common::config`] instead of command line arguments.
    #[clap(long, default_value = "")]
    pub config_path: String,
}

use std::future::Future;
use std::pin::Pin;

pub fn start(opts: CompactorOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        tracing::info!("meta address: {}", opts.meta_address.clone());

        let listen_address = opts.listen_address.parse().unwrap();
        tracing::info!("Server Listening at {}", listen_address);

        let contact_address = opts
            .contact_address
            .as_ref()
            .unwrap_or_else(|| {
                tracing::warn!("Contact address is not specified, defaulting to listen address");
                &opts.listen_address
            })
            .parse()
            .unwrap();
        tracing::info!(" address is {}", contact_address);

        let (join_handle, observer_join_handle, _shutdown_sender) =
            compactor_serve(listen_address, contact_address, opts).await;

        join_handle.await.unwrap();
        observer_join_handle.await.unwrap();
    })
}
