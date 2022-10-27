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

#![warn(clippy::dbg_macro)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::unused_async)]
#![warn(clippy::map_flatten)]
#![warn(clippy::no_effect_underscore_binding)]
#![warn(clippy::await_holding_lock)]
#![deny(rustdoc::broken_intra_doc_links)]
#![feature(let_else)]

mod server;

use clap::Parser;
use risingwave_common::config::{ServerConfig, StorageConfig};
use serde::{Deserialize, Serialize};

use crate::server::compaction_test_serve;

/// Command-line arguments for compute-node.
#[derive(Parser, Debug)]
pub struct CompactionTestOpts {
    // TODO: rename to listen_address and separate out the port.
    #[clap(long, default_value = "127.0.0.1:6660")]
    pub host: String,

    // Optional, we will use listen_address if not specified.
    #[clap(long)]
    pub client_address: Option<String>,

    /// The state store string e.g. hummock+s3://test-bucket
    #[clap(short, long)]
    pub state_store: String,

    #[clap(long, default_value = "http://127.0.0.1:5690")]
    pub meta_address: String,

    /// No given `config_path` means to use default config.
    #[clap(long, default_value = "")]
    pub config_path: String,

    /// The data of this table will be checked after compaction
    #[clap(short, long)]
    pub table_id: u32,

    /// The number of version deltas needed to be replayed before triggering a compaction
    #[clap(short, long, default_value = "10")]
    pub compaction_trigger_frequency: u64,

    /// The number of rounds to trigger compactions
    #[clap(short, long, default_value = "5")]
    pub compaction_trigger_rounds: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct TestToolConfig {
    // For connection
    #[serde(default)]
    pub server: ServerConfig,

    // Below for Hummock.
    #[serde(default)]
    pub storage: StorageConfig,
}

use std::future::Future;
use std::pin::Pin;

pub fn start(opts: CompactionTestOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        tracing::info!("Compaction test start with options {:?}", opts);
        let prefix = opts.state_store.strip_prefix("hummock+");
        match prefix {
            Some(s) => {
                assert!(s.starts_with("s3://"), "Only support S3 object store");
            }
            None => {
                panic!("Invalid state store");
            }
        }
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

        let ret = compaction_test_serve(listen_address, client_address, opts).await;
        match ret {
            Ok(_) => {
                tracing::info!("Success");
            }
            Err(e) => {
                tracing::error!("Failure {}", e);
            }
        }
    })
}
