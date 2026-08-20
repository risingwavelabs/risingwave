// Copyright 2026 RisingWave Labs
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

use std::io::Write;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use reqwest::blocking::Client;

use crate::{ClickHouseConfig, ExecuteContext, Task};

pub struct ClickHouseReadyCheckTask {
    config: ClickHouseConfig,
}

impl ClickHouseReadyCheckTask {
    pub fn new(config: ClickHouseConfig) -> Self {
        Self { config }
    }
}

impl Task for ClickHouseReadyCheckTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl Write>) -> Result<()> {
        ctx.pb.set_message("waiting for online...");
        let url = format!(
            "http://{}:{}/?query=SELECT%201",
            self.config.address, self.config.http_port
        );
        let client = Client::builder().timeout(Duration::from_secs(1)).build()?;

        ctx.wait(|| {
            let response = client
                .get(&url)
                .basic_auth(&self.config.user, Some(&self.config.password))
                .send()
                .context("failed to query ClickHouse")?;
            ensure!(
                response.status().is_success(),
                "ClickHouse returned HTTP status {}",
                response.status()
            );
            ensure!(
                response.text()?.trim() == "1",
                "unexpected ClickHouse response"
            );
            Ok(())
        })?;

        ctx.complete_spin();
        Ok(())
    }
}
