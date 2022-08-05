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

use std::sync::Arc;

use risingwave_common::util::addr::HostAddr;
use risingwave_pb::stream_service::stream_service_client::StreamServiceClient;

use crate::{Channel, RpcClient, RpcClientPool};

pub type StreamClient = StreamServiceClient<Channel>;

impl RpcClient for StreamClient {
    fn new_client(_host_addr: HostAddr, channel: Channel) -> Self {
        Self::new(channel)
    }
}

pub type StreamClientPool = RpcClientPool<StreamClient>;
pub type StreamClientPoolRef = Arc<StreamClientPool>;
