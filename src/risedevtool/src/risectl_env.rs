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

use std::fmt::Write;
use std::process::Command;

use anyhow::Result;

use crate::{add_hummock_backend, HummockInMemoryStrategy, ServiceConfig};

pub fn compute_risectl_env(services: &Vec<ServiceConfig>) -> Result<String> {
    // Pick one of the compute node and generate risectl config
    for item in services {
        if let ServiceConfig::ComputeNode(c) = item {
            let mut env = String::new();

            // RW_HUMMOCK_URL
            // If the cluster is launched without a shared storage, we will skip this.
            {
                let mut cmd = Command::new("compute-node");
                if add_hummock_backend(
                    "risectl",
                    c.provide_opendal.as_ref().unwrap(),
                    c.provide_minio.as_ref().unwrap(),
                    c.provide_aws_s3.as_ref().unwrap(),
                    HummockInMemoryStrategy::Disallowed,
                    &mut cmd,
                )
                .is_ok()
                {
                    writeln!(
                        env,
                        "export RW_HUMMOCK_URL=\"{}\"",
                        cmd.get_args().nth(1).unwrap().to_str().unwrap()
                    )
                    .unwrap();
                }
            }

            // RW_META_ADDR
            {
                let meta_node = &c.provide_meta_node.as_ref().unwrap()[0];
                writeln!(
                    env,
                    "export RW_META_ADDR=\"http://{}:{}\"",
                    meta_node.address, meta_node.port
                )
                .unwrap();
            }

            return Ok(env);
        }
    }
    Ok("".into())
}
