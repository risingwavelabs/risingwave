use std::collections::HashMap;
use std::process::Command;

use anyhow::Result;

use crate::{add_storage_backend, ServiceConfig};

pub fn compute_risectl_env(services: &HashMap<String, ServiceConfig>) -> Result<String> {
    // Pick one of the compute node and generate risectl config
    for item in services.values() {
        match item {
            ServiceConfig::ComputeNode(c) => {
                let mut cmd = Command::new("compute-node");
                add_storage_backend(
                    "risectl",
                    c.provide_minio.as_ref().unwrap(),
                    c.provide_aws_s3.as_ref().unwrap(),
                    false,
                    &mut cmd,
                )?;
                let meta_node = &c.provide_meta_node.as_ref().unwrap()[0];
                return Ok(format!(
                    "export RW_HUMMOCK_URL=\"{}\"\nexport RW_META_ADDR=\"http://{}:{}\"",
                    cmd.get_args().nth(1).unwrap().to_str().unwrap(),
                    meta_node.address,
                    meta_node.port
                ));
            }
            _ => {}
        }
    }
    Ok("".into())
}
