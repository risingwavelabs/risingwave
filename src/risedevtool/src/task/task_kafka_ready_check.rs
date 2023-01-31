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

use anyhow::{anyhow, Result};
use kafka::client::KafkaClient;

use crate::{ExecuteContext, KafkaConfig, Task};

pub struct KafkaReadyCheckTask {
    config: KafkaConfig,
}

impl KafkaReadyCheckTask {
    pub fn new(config: KafkaConfig) -> Result<Self> {
        Ok(Self { config })
    }
}

impl Task for KafkaReadyCheckTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.pb.set_message("waiting for online...");

        let mut client = KafkaClient::new(vec![format!(
            "{}:{}",
            self.config.address, self.config.port
        )]);

        ctx.wait(|| {
            client.load_metadata_all().map_err(|e| anyhow!("{}", e))?;
            Ok(())
        })?;

        ctx.complete_spin();

        Ok(())
    }
}
