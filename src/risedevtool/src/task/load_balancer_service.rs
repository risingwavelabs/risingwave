// Copyright 2023 Singularity Data
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
use std::env::temp_dir;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Result};
use uuid::Uuid;

use crate::{ExecuteContext, LoadBalancerConfig, Task};

pub struct LoadBalancerService {
    pub config: LoadBalancerConfig,
}

impl LoadBalancerService {
    pub fn new(config: LoadBalancerConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn nginx_path(&self) -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin).join("nginx"))
    }

    fn nginx(&self) -> Result<Command> {
        Ok(Command::new(self.nginx_path()?))
    }

    // Creates nginx.conf in tmp dir, based on self.conf.
    // Overwrite if file already exists.
    // Returns tmp file location or panics
    fn create_config_file(&self) -> String {
        // create tmp file
        let mut dir = temp_dir();
        let file_name = format!("{}.conf", Uuid::new_v4());
        dir.push(file_name);
        let dir_clone = dir.clone();
        let mut file = File::create(dir).unwrap();

        let mut file_content = String::new();
        file_content.push_str(
            r#"# This file was created based on the config in risedev.yaml

daemon off;
error_log stderr;

events {
  worker_connections 1024;
}

http {
    upstream loadbalancer {
"#,
        );

        assert!(
            !self.config.target_ports.is_empty(),
            "Please provide target ports for the load-balancer in risedev.yml"
        );

        let target_servers: Vec<_> = self
            .config
            .target_ports
            .iter()
            .map(|port| format!("      server localhost:{};", port))
            .collect();
        file_content.push_str(target_servers.join("\n").as_str());

        file_content.push_str(
            r#"
    }
    server {
"#,
        );

        let tmp = format!("        listen {} http2;", self.config.port);
        file_content.push_str(tmp.as_str());
        file_content.push_str(
            r#"
        server_name localhost;
        location / {
            grpc_pass grpc://loadbalancer;
        }
    }
}
"#,
        );
        file.write_all(file_content.as_bytes()).unwrap();

        format!("{}", dir_clone.as_path().display())
    }
}

impl Task for LoadBalancerService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl Write>) -> Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting");
        let path = self.nginx_path()?;
        if !path.exists() {
            return Err(anyhow!("Nginx binary not found in {:?}\nDid you enable nginx feature in `./risedev configure`?", path));
        }

        let conf_file_path = self.create_config_file();
        let conf_file_path_clone = conf_file_path.clone();

        let mut cmd = self.nginx()?;
        cmd.arg("-c").arg(conf_file_path);

        ctx.run_command(ctx.tmux_run(cmd)?)?;
        ctx.pb.set_message(format!(
            "Using nginx config file at {}",
            conf_file_path_clone
        ));
        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
