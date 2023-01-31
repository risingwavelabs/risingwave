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

mod compaction_test_runner;
mod delete_range_runner;

use clap::Parser;
pub use delete_range_runner::start_delete_range;

use crate::compaction_test_runner::compaction_test_main;

/// Command-line arguments for compute-node.
#[derive(Parser, Debug)]
pub struct CompactionTestOpts {
    #[clap(long, default_value = "127.0.0.1:6660")]
    pub host: String,

    // Optional, we will use listen_addr if not specified.
    #[clap(long)]
    pub client_address: Option<String>,

    /// The state store string e.g. hummock+s3://test-bucket
    #[clap(short, long)]
    pub state_store: String,

    #[clap(long, default_value = "http://127.0.0.1:5790")]
    pub meta_address: String,

    /// The data of this table will be checked after compaction
    #[clap(short, long, default_value = "0")]
    pub table_id: u32,

    /// Whether runs in the CI environment
    #[clap(long, default_value = "false")]
    pub ci_mode: bool,

    /// The number of version deltas needed to be replayed before triggering a compaction
    #[clap(long, default_value = "10")]
    pub num_trigger_frequency: u64,

    /// The number of rounds to trigger compactions
    #[clap(long, default_value = "5")]
    pub num_trigger_rounds: u32,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    ///
    /// Note that internal system parameters should be defined in the configuration file at
    /// [`risingwave_common::config`] instead of command line arguments.
    #[clap(long, default_value = "")]
    pub config_path: String,

    /// The path to the configuration file used for the embedded meta node.
    #[clap(long, default_value = "src/config/ci-compaction-test-meta.toml")]
    pub config_path_for_meta: String,
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
                assert!(
                    s.starts_with("s3://") || s.starts_with("minio://"),
                    "Only support S3 and MinIO object store"
                );
            }
            None => {
                panic!("Invalid state store");
            }
        }
        let listen_addr = opts.host.parse().unwrap();
        tracing::info!("Server Listening at {}", listen_addr);

        let client_address = opts
            .client_address
            .as_ref()
            .unwrap_or_else(|| {
                tracing::warn!("Client address is not specified, defaulting to host address");
                &opts.host
            })
            .parse()
            .unwrap();

        let ret = compaction_test_main(listen_addr, client_address, opts).await;
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
