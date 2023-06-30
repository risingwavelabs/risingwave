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

use std::io::Write;

use anyhow::Result;
use clap::Parser;
use itertools::Itertools;
use madsim::runtime::Handle;
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration, Session};
use tokio::time::Duration;

fn create_compute_node(cluster: &Cluster, idx: usize, role: &str) {
    let config = cluster.config();
    let opts = risingwave_compute::ComputeNodeOpts::parse_from([
        "compute-node",
        "--config-path",
        config.config_path.as_str(),
        "--listen-addr",
        "0.0.0.0:5688",
        "--advertise-addr",
        &format!("192.168.3.{idx}:5688"),
        "--total-memory-bytes",
        "6979321856",
        "--parallelism",
        &config.compute_node_cores.to_string(),
        "--role",
        role,
    ]);
    cluster
        .handle()
        .create_node()
        .name(format!("compute-{idx}"))
        .ip([192, 168, 3, idx as u8].into())
        .cores(config.compute_node_cores)
        .init(move || risingwave_compute::start(opts.clone(), prometheus::Registry::new()))
        .build();
}

fn cluster_config_no_compute_nodes() -> Configuration {
    let config_path = {
        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp config file");
        file.write_all(
            "\
[meta]
max_heartbeat_interval_secs = 300

[system]
barrier_interval_ms = 1000
checkpoint_frequency = 1

[server]
telemetry_enabled = false
        "
            .as_bytes(),
        )
        .expect("failed to write config file");
        file.into_temp_path()
    };

    Configuration {
        config_path: ConfigPath::Temp(config_path.into()),
        frontend_nodes: 2,
        compute_nodes: 0,
        meta_nodes: 3,
        compactor_nodes: 2,
        compute_node_cores: 2,
        etcd_timeout_rate: 0.0,
        etcd_data_path: None,
    }
}

#[madsim::test]
async fn test_serving_cluster_availability() {
    let config = cluster_config_no_compute_nodes();
    let mut cluster = Cluster::start(config).await.unwrap();
    let num_streaming = 3;
    let num_serving = 2;
    for idx in 1..=num_streaming {
        create_compute_node(&cluster, idx, "streaming");
    }
    for idx in num_streaming + 1..=num_streaming + num_serving {
        create_compute_node(&cluster, idx, "serving");
    }
    // wait for the service to be ready
    tokio::time::sleep(Duration::from_secs(15)).await;

    let mut session = cluster.start_session();
    session
        .run("create table t1 (c int primary key);")
        .await
        .unwrap();
    for i in 0..1000 {
        session
            .run(format!("insert into t1 values ({})", i))
            .await
            .unwrap();
    }
    session.run("flush;").await.unwrap();
    session
        .run("set visibility_mode to checkpoint;")
        .await
        .unwrap();
    session.run("set query_mode to distributed;").await.unwrap();

    let select = "select * from t1 order by c;";
    let query_and_assert = |mut session: Session| async move {
        let result = session.run(select).await.unwrap();
        let rows: Vec<u32> = result
            .split('\n')
            .into_iter()
            .map(|r| r.parse::<u32>().unwrap())
            .collect_vec();
        assert_eq!(rows, (0..1000).collect_vec());
    };
    query_and_assert(session.clone()).await;
    // Leave one serving node, kill all other serving nodes.
    for idx in num_streaming + 1..num_streaming + num_serving {
        cluster.handle().kill(format!("compute-{}", idx));
    }
    // Fail and mask node.
    session.run(select).await.unwrap_err();
    // Succeed as failed node has been masked.
    query_and_assert(session.clone()).await;
    tokio::time::sleep(Duration::from_secs(60)).await;
    // Fail because mask has expired.
    session.run(select).await.unwrap_err();
    // Succeed as failed node has been masked.
    query_and_assert(session.clone()).await;

    // Killed serving node is removed by meta permanently.
    tokio::time::sleep(Duration::from_secs(300)).await;
    query_and_assert(session.clone()).await;

    // kill the remaining serving nodes.
    cluster
        .handle()
        .kill(format!("compute-{}", num_streaming + num_serving));
    // no serving nodes
    session.run(select).await.unwrap_err();
    session.run(select).await.unwrap_err();
    session.run("set visibility_mode to all;").await.unwrap();
    query_and_assert(session.clone()).await;
    session
        .run("set visibility_mode to checkpoint;")
        .await
        .unwrap();
    session.run(select).await.unwrap_err();

    create_compute_node(&cluster, num_streaming + num_serving + 1, "serving");
    // wait for the service to be ready
    tokio::time::sleep(Duration::from_secs(15)).await;
    query_and_assert(session.clone()).await;
    // wait for mask expire
    tokio::time::sleep(Duration::from_secs(30)).await;
    session.run(select).await.unwrap_err();
    // wait for previous nodes expire
    tokio::time::sleep(Duration::from_secs(300)).await;
    query_and_assert(session.clone()).await;
}
