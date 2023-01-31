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

use super::{ExecuteContext, Task};

pub struct ConfigureGrpcNodeTask {
    advertise_address: String,
    port: u16,
    user_managed: bool,
}

impl ConfigureGrpcNodeTask {
    pub fn new(advertise_address: String, port: u16, user_managed: bool) -> Result<Self> {
        Ok(Self {
            advertise_address,
            port,
            user_managed,
        })
    }
}

impl Task for ConfigureGrpcNodeTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        let address = format!("{}:{}", self.advertise_address, self.port);

        if self.user_managed {
            ctx.pb.set_message(
                "waiting for user-managed service online... (see `risedev.log` for cli args)",
            );
            ctx.wait_tcp_user(&address)?;
        } else {
            ctx.pb.set_message("waiting for online...");
            ctx.wait_tcp(&address)?;
        }

        ctx.complete_spin();

        Ok(())
    }
}
