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

use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::PathBuf;

use chrono::prelude::Local;
use risingwave_pb::common::WorkerType;
use risingwave_pb::monitor_service::ProfilingResponse;
use risingwave_rpc_client::ComputeClientPool;

use crate::common::MetaServiceOpts;

pub async fn profile(sleep_s: u64) -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;

    let workers = meta_client.get_cluster_info().await?.worker_nodes;
    let compute_nodes = workers
        .into_iter()
        .filter(|w| w.r#type() == WorkerType::ComputeNode);

    let clients = ComputeClientPool::new(u64::MAX);

    let profile_root_path = PathBuf::from(&std::env::var("PREFIX_PROFILING")?);
    let dir_name = Local::now().format("%Y-%m-%d-%H-%M-%S").to_string();
    let dir_path = profile_root_path.join(dir_name);
    create_dir_all(&dir_path)?;

    // FIXME: the compute node may not be accessible directly from risectl, we may let the meta
    // service collect the reports from all compute nodes in the future.
    for cn in compute_nodes {
        let client = clients.get(&cn).await?;
        let response = client.profile(sleep_s).await;
        let host_addr = cn.get_host().expect("Should have host address");
        let node_name = format!(
            "compute-node-{}-{}",
            host_addr.get_host().replace('.', "-"),
            host_addr.get_port()
        );
        let svg_file_name = format!("{}.svg", node_name);
        match response {
            Ok(ProfilingResponse { result }) => {
                let mut file = File::create(dir_path.join(svg_file_name))?;
                file.write_all(&result)?;
            }
            Err(err) => {
                tracing::error! {"Failed to get profiling result from {} with error {}", node_name, err.to_string()};
            }
        }
    }

    println!("Profiling results are saved at {}", dir_path.display());

    Ok(())
}
