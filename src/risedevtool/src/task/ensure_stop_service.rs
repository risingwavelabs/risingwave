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

use anyhow::Result;

use super::{ExecuteContext, Task};

pub struct EnsureStopService {
    /// `(port, id, user_managed)`
    ports: Vec<(u16, String, bool)>,
}

impl EnsureStopService {
    pub fn new(ports: Vec<(u16, String, bool)>) -> Result<Self> {
        Ok(Self { ports })
    }
}

impl Task for EnsureStopService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);

        for (port, service_id, user_managed) in &self.ports {
            // Do not require stopping user-managed services
            if *user_managed {
                continue;
            }
            let address = format!("127.0.0.1:{}", port);

            ctx.pb.set_message(format!(
                "waiting for port close - {} (will be used by {})",
                address, service_id
            ));
            ctx.wait_tcp_close(&address)?;
        }

        ctx.pb
            .set_message("all previous services have been stopped");

        ctx.complete_spin();

        Ok(())
    }

    fn id(&self) -> String {
        "prepare".into()
    }
}
