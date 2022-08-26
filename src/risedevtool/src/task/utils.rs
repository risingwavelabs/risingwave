// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::process::Command;

use anyhow::{anyhow, Result};

use crate::{AwsS3Config, MetaNodeConfig, MinioConfig};

/// Add a meta node to the parameters
pub fn add_meta_node(provide_meta_node: &[MetaNodeConfig], cmd: &mut Command) -> Result<()> {
    match provide_meta_node {
        [] => {
            return Err(anyhow!(
                "Cannot configure node: no meta node found in this configuration."
            ));
        }
        [meta_node] => {
            cmd.arg("--meta-address")
                .arg(format!("http://{}:{}", meta_node.address, meta_node.port));
        }
        other_meta_nodes => {
            return Err(anyhow!(
                "Cannot configure node: {} meta nodes found in this configuration, but only 1 is needed.",
                other_meta_nodes.len()
            ));
        }
    };

    Ok(())
}

/// Strategy for whether to enable in-memory hummock if no minio and s3 is provided.
pub enum HummockInMemoryStrategy {
    /// Enable isolated in-memory hummock.
    Isolated,
    /// Enable in-memory hummock shared in the process. Used by risedev playground.
    Shared,
    /// Disallow in-memory hummock. Always requires minio or s3.
    Disallowed,
}

/// Add a storage backend to the parameters. Returns whether this is a shared backend.
pub fn add_storage_backend(
    id: &str,
    provide_minio: &[MinioConfig],
    provide_aws_s3: &[AwsS3Config],
    hummock_in_memory_strategy: HummockInMemoryStrategy,
    cmd: &mut Command,
) -> Result<bool> {
    let is_shared_backend = match (provide_minio, provide_aws_s3) {
        ([], []) => {
            match hummock_in_memory_strategy {
                HummockInMemoryStrategy::Isolated => {
                    cmd.arg("--state-store").arg("hummock+memory");
                    false
                }
                HummockInMemoryStrategy::Shared => {
                    cmd.arg("--state-store").arg("hummock+memory-shared");
                    true
                },
                HummockInMemoryStrategy::Disallowed => return Err(anyhow!(
                    "{} is not compatible with in-memory state backend. Need to enable either minio or aws-s3.", id
                )),
            }
        }
        ([minio], []) => {
            cmd.arg("--state-store").arg(format!(
                "hummock+minio://{hummock_user}:{hummock_password}@{minio_addr}:{minio_port}/{hummock_bucket}",
                hummock_user = minio.root_user,
                hummock_password = minio.root_password,
                hummock_bucket = minio.hummock_bucket,
                minio_addr = minio.address,
                minio_port = minio.port,
            ));
            true
        }
        ([], [aws_s3]) => {
            cmd.arg("--state-store")
                .arg(format!("hummock+s3://{}", aws_s3.bucket));
            true
        }
        (other_minio, other_s3) => {
            return Err(anyhow!(
                "{} minio and {} s3 instance found in config, but only 1 is needed",
                other_minio.len(),
                other_s3.len()
            ))
        }
    };

    Ok(is_shared_backend)
}
