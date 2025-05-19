// Copyright 2025 RisingWave Labs
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

#![cfg(madsim)]

use std::time::Duration;

use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration, Session};

fn cluster_config(interval_sec: usize) -> Configuration {
    use std::io::Write;
    let config_path = {
        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp config file");
        file.write_all(
            format!(
                "\
[meta]
max_heartbeat_interval_secs = 300
periodic_space_reclaim_compaction_interval_sec = {interval_sec}

[system]
barrier_interval_ms = 1000
checkpoint_frequency = 1

[server]
telemetry_enabled = false
metrics_level = \"Disabled\"
        "
            )
            .as_bytes(),
        )
        .expect("failed to write config file");
        file.into_temp_path()
    };

    Configuration {
        config_path: ConfigPath::Temp(config_path.into()),
        frontend_nodes: 1,
        compute_nodes: 1,
        meta_nodes: 1,
        compactor_nodes: 4,
        compute_node_cores: 2,
        ..Default::default()
    }
}

#[tokio::test]
async fn test_vnode_watermark_reclaim() {
    // The vnode watermark reclaim will be triggered, so the SST will be reclaimed.
    let config = crate::compaction::cluster_config(10);
    let mut cluster = Cluster::start(config).await.unwrap();
    // wait for the service to be ready
    tokio::time::sleep(Duration::from_secs(15)).await;
    let mut session = cluster.start_session();

    let compaction_group_id = test_vnode_watermark_reclaim_impl(&mut cluster, &mut session).await;

    assert_compaction_group_sst_count(compaction_group_id, 6, 1, &mut session).await;
    // Need wait longer for reclamation, due to the hard-coded STATE_CLEANING_PERIOD_EPOCH.
    tokio::time::sleep(Duration::from_secs(500)).await;
    assert_compaction_group_sst_count(compaction_group_id, 6, 0, &mut session).await;
}

#[tokio::test]
async fn test_no_vnode_watermark_reclaim() {
    // The vnode watermark reclaim won't be triggered, so the SST won't be reclaimed.
    let config = crate::compaction::cluster_config(3600);
    let mut cluster = Cluster::start(config).await.unwrap();
    // wait for the service to be ready
    tokio::time::sleep(Duration::from_secs(15)).await;
    let mut session = cluster.start_session();

    let compaction_group_id = test_vnode_watermark_reclaim_impl(&mut cluster, &mut session).await;

    assert_compaction_group_sst_count(compaction_group_id, 6, 1, &mut session).await;
    tokio::time::sleep(Duration::from_secs(500)).await;
    assert_compaction_group_sst_count(compaction_group_id, 6, 1, &mut session).await;
}

async fn assert_compaction_group_sst_count(
    compaction_group_id: CompactionGroupId,
    level_id: usize,
    expected: usize,
    session: &mut Session,
) {
    let count = session
        .run(format!("SELECT COUNT(*) FROM rw_hummock_sstables WHERE compaction_group_id={compaction_group_id} and level_id={level_id};"))
        .await
        .unwrap();
    assert_eq!(count.parse::<usize>().unwrap(), expected);
}

async fn test_vnode_watermark_reclaim_impl(
    cluster: &mut Cluster,
    session: &mut Session,
) -> CompactionGroupId {
    session
        .run("CREATE TABLE t2 (ts timestamptz, v INT);")
        .await
        .unwrap();
    session
        .run("CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM t2 WHERE ts > now() - INTERVAL '10s';")
        .await
        .unwrap();

    let table_id = session
        .run("SELECT id FROM rw_internal_tables WHERE name LIKE '%dynamicfilterleft%';")
        .await
        .unwrap()
        .parse::<u64>()
        .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;
    async fn compaction_group_id_by_table_id(session: &mut Session, table_id: u64) -> u64 {
        session
            .run(format!(
                "SELECT id FROM rw_hummock_compaction_group_configs where member_tables @> '[{}]'::jsonb;",
                table_id
            ))
            .await
            .unwrap()
            .parse::<u64>()
            .unwrap()
    }
    let original_compaction_group_id = compaction_group_id_by_table_id(session, table_id).await;
    // Move the table to a dedicated group to prevent its vnode watermark from being reclaimed during the compaction of other tables.
    cluster
        .split_compaction_group(original_compaction_group_id, table_id.into())
        .await
        .unwrap();
    let compaction_group_id = compaction_group_id_by_table_id(session, table_id).await;

    session
        .run("INSERT INTO t2 VALUES (now(), 1);")
        .await
        .unwrap();
    session.run("FLUSH;").await.unwrap();
    assert_compaction_group_sst_count(compaction_group_id, 0, 1, session).await;
    assert_compaction_group_sst_count(compaction_group_id, 6, 0, session).await;
    // Compact data to L6 so that vnode watermark picker can take effects.
    cluster
        .trigger_manual_compaction(compaction_group_id, 0)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_compaction_group_sst_count(compaction_group_id, 0, 0, session).await;
    assert_compaction_group_sst_count(compaction_group_id, 6, 1, session).await;
    compaction_group_id
}
