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

use std::time::Duration;

use anyhow::Context as _;
use sqlx::{ConnectOptions, Connection as _};

use super::{ExecuteContext, Task};
use crate::wait::wait;

/// Check if the database is ready to use.
pub struct DbReadyCheckTask<O> {
    options: O,
}

impl<O> DbReadyCheckTask<O> {
    pub fn new(options: O) -> Self {
        Self { options }
    }
}

impl<O> Task for DbReadyCheckTask<O>
where
    O: ConnectOptions,
    O::Connection: Sized,
{
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        let Some(id) = ctx.id.clone() else {
            panic!("Service should be set before executing DbReadyCheckTask");
        };

        ctx.pb.set_message("waiting for ready...");

        wait(
            || {
                let options = self.options.clone();

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;

                rt.block_on(async move {
                    let mut conn = options
                        .connect()
                        .await
                        .context("failed to connect to database")?;
                    conn.ping().await.context("failed to ping database")?;
                    Ok(())
                })
            },
            &mut ctx.log,
            ctx.status_file.as_ref().unwrap(),
            &id,
            Some(Duration::from_secs(20)),
            true,
        )
        .with_context(|| format!("failed to wait for service `{id}` to be ready"))?;

        ctx.complete_spin();

        Ok(())
    }
}
