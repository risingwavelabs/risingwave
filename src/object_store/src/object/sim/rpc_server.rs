// Copyright 2024 RisingWave Labs
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

use std::io::Result;
use std::net::SocketAddr;
use std::sync::Arc;

use madsim::net::{Endpoint, Payload};

use super::service::{Request, SimService};

/// A simulated ObjectStore server.
#[derive(Default, Clone)]
pub struct SimServer {}

impl SimServer {
    pub fn builder() -> Self {
        SimServer::default()
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        let ep = Endpoint::bind(addr).await?;
        let service = SimService::new();
        let service = Arc::new(service);

        loop {
            let (tx, mut rx, _) = ep.accept1().await?;
            let service = service.clone();
            madsim::task::spawn(async move {
                let request = *rx.recv().await?.downcast::<Request>().unwrap();
                use super::service::Request::*;

                let response: Payload = match request {
                    Upload { path, obj } => Box::new(service.upload(path, obj).await),
                    Read { path } => Box::new(service.read(path).await),
                    Delete { path } => Box::new(service.delete(path).await),
                    DeleteObjects { paths } => Box::new(service.delete_objects(paths).await),
                    List { path } => Box::new(service.list(path).await),
                    Metadata { path } => Box::new(service.metadata(path).await),
                };
                tx.send(response).await?;
                Ok(()) as Result<()>
            });
        }
    }
}
