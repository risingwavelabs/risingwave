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

use std::env;

use anyhow::{bail, Result};
use risingwave_pb::common::WorkerType;
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
* use `./risedev d for-ctl` to start the cluster.
* use `./risedev ctl` to use risectl.

For `./risedev apply-compose-deploy` users,
* `RW_META_ADDR` will be printed out when deploying. Please copy the bash exports to your console.

risectl requires a full persistent cluster to operate. Please make sure you're not running in minimum mode.";
                bail!(MESSAGE);
            }
        };
        Ok(Self { meta_addr })
    }

    /// Create meta client from options, and register as rise-ctl worker
    pub async fn create_meta_client(&self) -> Result<MetaClient> {
        let client = MetaClient::register_new(
            &self.meta_addr,
            WorkerType::RiseCtl,
            &"127.0.0.1:2333".parse().unwrap(),
            0,
        )
        .await?;
        // FIXME: don't use 127.0.0.1 for ctl
        let worker_id = client.worker_id();
        tracing::info!("registered as RiseCtl worker, worker_id = {}", worker_id);
        // TODO: remove worker node
        Ok(client)
    }
}
