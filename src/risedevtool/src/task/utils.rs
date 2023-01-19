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

use std::process::Command;

use anyhow::{anyhow, Result};

use crate::{AwsS3Config, MetaNodeConfig, MinioConfig};

/// Add a meta node to the parameters.
pub fn add_meta_node(provide_meta_node: &[MetaNodeConfig], cmd: &mut Command) -> Result<()> {
    match provide_meta_node {
        [] => {
            return Err(anyhow!(
                "Cannot configure node: no meta node found in this configuration."
            ));
        }
        meta_nodes => {
            // access meta nodes via load-balancer
            match meta_nodes.last().unwrap().lb_port {
                Some(lb_port) => {
                    // All nodes should be addressed by the same LB
                    for mnc in meta_nodes {
                        assert!(
                            mnc.lb_port.is_some(),
                            "if one meta node is addressed by a load-balancer, all should be addressed by an LB");
                        assert!(
                            mnc.lb_port.unwrap() == lb_port,
                            "All meta nodes should use the same LB"
                        );
                    }
                    cmd.arg("--meta-address")
                        .arg(format!("http://127.0.0.1:{}", lb_port));
                }
                None => {
                    // access one meta node directly
                    cmd.arg("--meta-address").arg(format!(
                        "http://{}:{}",
                        meta_nodes.last().unwrap().address,
                        meta_nodes.last().unwrap().port
                    ));
                    if meta_nodes.len() > 1 {
                        eprintln!("WARN: more than 1 meta node instance is detected, only using the last one for meta node.");
                        // According to some heruistics, the last etcd node seems always to be
                        // elected as leader. Therefore we ensure compute
                        // node can start by using the last one.
                        // This is not the case for the meta nodes. The first meta node will get
                        // elected
                    }
                }
            }
        }
    };

    Ok(())
}

/// Strategy for whether to enable in-memory hummock if no minio and s3 is provided.
pub enum HummockInMemoryStrategy {
    /// Enable isolated in-memory hummock. Used by single-node configuration.
    Isolated,
    /// Enable in-memory hummock shared in a single process. Used by risedev playground and
    /// deterministic end-to-end tests.
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
            // if s3-compatible is true, using some s3 compatible object store.
            match aws_s3.s3_compatible{
                true => cmd.arg("--state-store")
                .arg(format!("hummock+s3-compatible://{}", aws_s3.bucket)),
                false => cmd.arg("--state-store")
                .arg(format!("hummock+s3://{}", aws_s3.bucket)),
            };
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
