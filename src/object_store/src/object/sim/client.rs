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

use std::fmt::Debug;
use std::net::SocketAddr;

use madsim::net::{Endpoint, Payload};

use super::service::{Request, Response};
use crate::object::error::ObjectResult as Result;

#[derive(Debug, Clone)]
pub struct Client {
    addr: SocketAddr,
}

impl Client {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub(crate) async fn send_request(&self, req: Request) -> Result<Response> {
        let resp = self.send_request_io(req).await?;
        let resp = *resp
            .downcast::<Result<Response>>()
            .expect("failed to downcast");
        resp
    }

    async fn send_request_io(&self, req: Request) -> std::io::Result<Payload> {
        let addr = self.addr;
        let ep = Endpoint::connect(addr).await?;
        let (tx, mut rx) = ep.connect1(addr).await?;
        tx.send(Box::new(req)).await?;
        rx.recv().await
    }
}
