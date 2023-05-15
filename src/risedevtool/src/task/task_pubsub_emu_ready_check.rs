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

use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Result};
use google_cloud_pubsub::client::Client;

use crate::{ExecuteContext, PubsubConfig, Task};

pub struct PubsubReadyTaskCheck {
    config: PubsubConfig,
}

impl PubsubReadyTaskCheck {
    pub fn new(config: PubsubConfig) -> Result<Self> {
        Ok(Self { config })
    }
}

// #[async_tr]
impl Task for PubsubReadyTaskCheck {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.pb.set_message("waiting for online...");

        // environment variables to use the pubsub emulator
        std::env::set_var(
            "PUBSUB_EMULATOR_HOST",
            format!("{}:{}", self.config.address, self.config.port),
        );

        thread::sleep(Duration::from_secs(5));
        let async_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()?;

        let client = async_runtime.block_on(Client::new(Default::default()))?;

        ctx.wait(|| {
            async_runtime
                .block_on(client.get_subscriptions(None))
                .map_err(|e| anyhow!(e))?;
            Ok(())
        })?;

        ctx.complete_spin();

        Ok(())
    }
}
