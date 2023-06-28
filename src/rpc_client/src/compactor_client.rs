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

use std::time::Duration;

use risingwave_common::util::addr::HostAddr;
use risingwave_pb::monitor_service::monitor_service_client::MonitorServiceClient;
use risingwave_pb::monitor_service::{StackTraceRequest, StackTraceResponse};
use tonic::transport::{Channel, Endpoint};

use crate::error::Result;

#[derive(Clone)]
pub struct CompactorClient {
    pub monitor_client: MonitorServiceClient<Channel>,
}

impl CompactorClient {
    pub async fn new(host_addr: HostAddr) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", &host_addr))?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await?;
        Ok(Self {
            monitor_client: MonitorServiceClient::new(channel),
        })
    }

    pub async fn stack_trace(&self) -> Result<StackTraceResponse> {
        Ok(self
            .monitor_client
            .to_owned()
            .stack_trace(StackTraceRequest::default())
            .await?
            .into_inner())
    }
}
