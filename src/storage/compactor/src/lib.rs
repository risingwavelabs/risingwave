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

use std::future::Future;
use std::pin::Pin;

use risingwave_common::config::{load_config, CompactorConfig};

use crate::server::compactor_serve;

pub fn start(opts: CompactorConfig) -> Pin<Box<dyn Future<Output = ()> + Send>> {
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
