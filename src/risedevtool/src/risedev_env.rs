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

#![allow(clippy::doc_markdown)] // RiseDev

use std::fmt::Write;
use std::process::Command;

use crate::{add_hummock_backend, HummockInMemoryStrategy, ServiceConfig};

/// Generate environment variables (put in file `.risingwave/config/risedev-env`)
/// from the given service configurations to be used by future
/// RiseDev commands, like `risedev ctl` or `risedev psql` ().
pub fn generate_risedev_env(services: &Vec<ServiceConfig>) -> String {
    let mut env = String::new();
    for item in services {
        if let ServiceConfig::ComputeNode(c) = item {
            // RW_HUMMOCK_URL
            // If the cluster is launched without a shared storage, we will skip this.
            {
                let mut cmd = Command::new("compute-node");
                if add_hummock_backend(
                    "dummy",
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
                        "RW_HUMMOCK_URL=\"{}\"",
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
                    "RW_META_ADDR=\"http://{}:{}\"",
                    meta_node.address, meta_node.port
                )
                .unwrap();
            }
            break;
        }
    }
    for item in services {
        if let ServiceConfig::Frontend(c) = item {
            let listen_address = &c.listen_address;
            writeln!(env, "RW_FRONTEND_LISTEN_ADDRESS=\"{listen_address}\"",).unwrap();
            let port = &c.port;
            writeln!(env, "RW_FRONTEND_PORT=\"{port}\"",).unwrap();
            break;
        }
    }
    env
}
