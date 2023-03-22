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

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use tracing::info;

/// Start meta node
pub fn start(opts: MetaProbeOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        info!("Starting probe 2");
        info!("> options: {:?}", opts);
        let listen_addr: SocketAddr = opts.listen_addr.parse().unwrap();
        info!("probing on {}", listen_addr);
    })
}
