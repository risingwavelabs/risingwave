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

pub fn add_storage_backend(
    id: &str,
    provide_minio: &[MinioConfig],
    provide_aws_s3: &[AwsS3Config],
    cmd: &mut Command,
) -> Result<()> {
    match gen_object_store_url(provide_minio, provide_aws_s3)? {
        None => Err(anyhow!(
            "{} is not compatible with in-memory state backend. Need to enable either minio or aws-s3.", id
        )),
        Some(url) => {
            cmd.arg("--state-store").arg("hummock+".to_string() + &url);
            Ok(())
        }
    }
}

pub fn gen_object_store_url(
    provide_minio: &[MinioConfig],
    provide_aws_s3: &[AwsS3Config],
) -> Result<Option<String>> {
    match (provide_minio, provide_aws_s3) {
        ([], []) => Ok(None),
        ([minio], []) => Ok(Some(format!(
            "minio://{hummock_user}:{hummock_password}@{minio_addr}:{minio_port}/{hummock_bucket}",
            hummock_user = minio.root_user,
            hummock_password = minio.root_password,
            hummock_bucket = minio.hummock_bucket,
            minio_addr = minio.address,
            minio_port = minio.port,
        ))),
        ([], [aws_s3]) => Ok(Some(format!("s3://{}", aws_s3.bucket))),
        (other_minio, other_s3) => {
            return Err(anyhow!(
                "{} minio and {} s3 instance found in config, but only 1 is needed",
                other_minio.len(),
                other_s3.len()
            ))
        }
    }
}
