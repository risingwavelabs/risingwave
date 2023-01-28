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

use std::future::Future;
use std::pin::Pin;

use risingwave_common::config::{load_config, AsyncStackTraceOption, ComputeNodeConfig};
use risingwave_common::util::resource_util::cpu::total_cpu_available;
use risingwave_common::util::resource_util::memory::total_memory_available_bytes;

use crate::server::compute_node_serve;

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

/// Start compute node
pub fn start(opts: ComputeNodeConfig) -> Pin<Box<dyn Future<Output = ()> + Send>> {
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
                tracing::warn!("Client address is not specified, defaulting to listen address");
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
