// Copyright 2025 RisingWave Labs
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

use std::env;

use anyhow::{Result, bail};
use risingwave_common::config::MetaConfig;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerType;
use risingwave_pb::common::worker_node::Property;
use risingwave_rpc_client::MetaClient;

pub struct MetaServiceOpts {
    pub meta_addr: String,
}

impl MetaServiceOpts {
    /// Recover meta service options from env variable
    ///
    /// Currently, we will read these variables for meta:
    ///
    /// * `RW_META_ADDR`: meta service address
    pub fn from_env() -> Result<Self> {
        let meta_addr = match env::var("RW_META_ADDR") {
            Ok(url) => {
                tracing::info!("using meta addr from `RW_META_ADDR`: {}", url);
                url
            }
            Err(_) => {
                const MESSAGE: &str = "env variable `RW_META_ADDR` not found.

For `./risedev d` use cases, please do the following:
* use `./risedev ctl` to use risectl.

For production use cases,
* please set `RW_META_ADDR` to the address of the meta node.

Note: the default value of `RW_META_ADDR` is 'http://127.0.0.1:5690'.";
                bail!(MESSAGE);
            }
        };
        Ok(Self { meta_addr })
    }

    /// Create meta client from options, and register as rise-ctl worker
    pub async fn create_meta_client(&self) -> Result<MetaClient> {
        let (client, _) = MetaClient::register_new(
            self.meta_addr.parse()?,
            WorkerType::RiseCtl,
            &get_new_ctl_identity(),
            Property::default(),
            &MetaConfig::default(),
        )
        .await;
        let worker_id = client.worker_id();
        tracing::info!("registered as RiseCtl worker, worker_id = {}", worker_id);
        Ok(client)
    }
}

fn get_new_ctl_identity() -> HostAddr {
    HostAddr {
        host: format!("risectl-{}", uuid::Uuid::new_v4()),
        port: 0,
    }
}
