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
use risingwave_common::config::{load_config, OverwriteConfig, RwConfig};

use crate::server::compactor_serve;

/// CLI argguments received by meta node. Overwrites fields in
/// [`risingwave_common::config::CompactorConfig`].
#[derive(Parser, Clone, Debug)]
pub struct CompactorOpts {
    #[clap(long = "host")]
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

    /// It's a hint used by meta node.
    #[clap(long)]
    pub max_concurrent_task_number: Option<u64>,

    #[clap(long)]
    pub compaction_worker_threads_number: Option<usize>,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    #[clap(long, default_value = "")]
    pub config_path: String,
}

impl OverwriteConfig for CompactorOpts {
    fn overwrite(self, config: &mut RwConfig) {
        let mut c = &mut config.compactor;
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
        if let Some(v) = self.max_concurrent_task_number {
            c.max_concurrent_task_number = v;
        }
        if self.compaction_worker_threads_number.is_some() {
            c.compaction_worker_threads_number = self.compaction_worker_threads_number;
        }
    }
}

use std::future::Future;
use std::pin::Pin;

pub fn start(opts: CompactorOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        let config = load_config(&opts.config_path.clone(), Some(opts));
        tracing::info!("meta address: {}", config.compactor.meta_address);

        let listen_address = config.compactor.listen_addr.parse().unwrap();
        tracing::info!("Server Listening at {}", listen_address);

        let client_address = config
            .compactor
            .client_address
            .as_ref()
            .unwrap_or_else(|| {
                tracing::warn!("Client address is not specified, defaulting to listen address");
                &config.compactor.listen_addr
            })
            .parse()
            .unwrap();
        tracing::info!("Client address is {}", client_address);

        let (join_handle, observer_join_handle, _shutdown_sender) =
            compactor_serve(listen_address, client_address, config).await;

        join_handle.await.unwrap();
        observer_join_handle.await.unwrap();
    })
}
