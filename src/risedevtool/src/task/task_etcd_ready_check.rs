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

use anyhow::Result;
use serde::Deserialize;

use crate::{EtcdConfig, ExecuteContext, Task};

#[derive(Deserialize)]
struct HealthResponse {
    health: String,
    #[expect(dead_code)]
    reason: String,
}

pub struct EtcdReadyCheckTask {
    config: EtcdConfig,
}

impl EtcdReadyCheckTask {
    pub fn new(config: EtcdConfig) -> Result<Self> {
        Ok(Self { config })
    }
}

impl Task for EtcdReadyCheckTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.pb.set_message("waiting for online...");
        let health_check_addr =
            format!("http://{}:{}/health", self.config.address, self.config.port);
        let online_cb = |body: &str| -> bool {
            let response: HealthResponse = serde_json::from_str(body).unwrap();
            response.health == "true"
        };

        ctx.wait_http_with_cb(health_check_addr, online_cb)?;
        ctx.pb
            .set_message(format!("api {}:{}", self.config.address, self.config.port));
        ctx.complete_spin();

        Ok(())
    }
}
