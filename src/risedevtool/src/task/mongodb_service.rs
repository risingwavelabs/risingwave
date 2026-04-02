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

use std::process::Command;

use anyhow::{Context, Result, anyhow};

use super::docker_service::{DockerService, DockerServiceConfig};
use crate::MongoDbConfig;
use crate::task::{ExecuteContext, Task};

const REPLICA_SET_NAME: &str = "rs0";

impl DockerServiceConfig for MongoDbConfig {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn is_user_managed(&self) -> bool {
        self.user_managed
    }

    fn image(&self) -> String {
        self.image.clone()
    }

    fn args(&self) -> Vec<String> {
        vec![
            "--replSet".to_owned(),
            REPLICA_SET_NAME.to_owned(),
            "--bind_ip_all".to_owned(),
            "--oplogSize".to_owned(),
            "128".to_owned(),
        ]
    }

    fn ports(&self) -> Vec<(String, String)> {
        vec![(self.port.to_string(), "27017".to_owned())]
    }

    fn data_path(&self) -> Option<String> {
        self.persist_data.then(|| "/data/db".to_owned())
    }
}

pub type MongoDbService = DockerService<MongoDbConfig>;

pub struct MongoDbSetupTask {
    config: MongoDbConfig,
}

impl MongoDbSetupTask {
    pub fn new(config: MongoDbConfig) -> Self {
        Self { config }
    }

    fn container_name(&self) -> String {
        format!("risedev-{}", self.config.id)
    }

    fn mongosh_eval(&self, script: &str) -> Result<std::process::Output> {
        let output = Command::new("docker")
            .arg("exec")
            .arg(self.container_name())
            .arg("mongosh")
            .arg("--quiet")
            .arg("--eval")
            .arg(script)
            .output()
            .context("failed to run mongosh in mongodb container")?;
        Ok(output)
    }

    fn wait_replica_set_ready(&self, ctx: &mut ExecuteContext<impl std::io::Write>) -> Result<()> {
        ctx.pb.set_message("waiting for replica set ready...");
        ctx.wait(|| {
            let output = self.mongosh_eval(
                r#"
try {
  const status = rs.status();
  if (status.ok === 1) {
    quit(0);
  }
  quit(1);
} catch (err) {
  if (err.codeName === "NotYetInitialized") {
    quit(2);
  }
  print(err);
  quit(3);
}
"#,
            )?;
            match output.status.code() {
                Some(0) => Ok(()),
                Some(1) | Some(2) => Err(anyhow!("mongodb replica set is not ready yet")),
                _ => {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    Err(anyhow!(
                        "failed to check mongodb replica set status: {}{}",
                        stdout.trim(),
                        stderr.trim()
                    ))
                }
            }
        })?;
        Ok(())
    }
}

impl Task for MongoDbSetupTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> Result<()> {
        if self.config.user_managed {
            return Ok(());
        }

        let member = format!("{}:{}", self.config.address, self.config.port);
        let init_script = format!(
            r#"
try {{
  const status = rs.status();
  if (status.ok === 1) {{
    quit(0);
  }}
}} catch (err) {{
  if (err.codeName !== "NotYetInitialized") {{
    print(err);
    quit(2);
  }}
}}

const result = rs.initiate({{
  _id: "{REPLICA_SET_NAME}",
  members: [{{ _id: 0, host: "{member}" }}]
}});

if (result.ok === 1) {{
  quit(0);
}}

printjson(result);
quit(1);
"#
        );

        ctx.pb.set_message("initializing replica set...");
        let output = self.mongosh_eval(&init_script)?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(anyhow!(
                "failed to initialize mongodb replica set: {}{}",
                stdout.trim(),
                stderr.trim()
            ));
        }

        self.wait_replica_set_ready(ctx)?;
        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
