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

use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::manager::MetaOpts;
use crate::rpc::server::MetaStoreBackend;
use crate::AddressInfo;

pub struct LocalMeta {
    port: u16,
    join_handle: JoinHandle<()>,
    shutdown_sender: Sender<()>,
}

impl LocalMeta {
    fn meta_addr_inner(port: u16) -> String {
        format!("127.0.0.1:{}", port)
    }

    /// Start a local meta node in the background.
    pub async fn start(port: u16) -> Self {
        let addr = Self::meta_addr_inner(port);
        let listen_addr: SocketAddr = addr.parse().unwrap();
        let address_info = AddressInfo {
            addr,
            listen_addr,
            ..Default::default()
        };
        let (join_handle, shutdown_sender) = crate::rpc::server::rpc_serve(
            address_info,
            MetaStoreBackend::Mem,
            Duration::from_secs(3600),
            10,
            MetaOpts::default(),
        )
        .await
        .unwrap();
        Self {
            port,
            join_handle,
            shutdown_sender,
        }
    }

    pub async fn stop(self) {
        self.shutdown_sender.send(()).unwrap();
        self.join_handle.await.unwrap();
    }

    pub fn meta_addr(&self) -> String {
        Self::meta_addr_inner(self.port)
    }
}
