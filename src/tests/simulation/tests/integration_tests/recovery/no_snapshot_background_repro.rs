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

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Result, anyhow};
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use tokio::time::sleep;

use crate::utils::wait_all_database_recovered;

const MAX_HEARTBEAT_INTERVAL_SEC: u64 = 10;
const DATABASE_RECOVERY_START: &str = "DATABASE_RECOVERY_START";
const DATABASE_RECOVERY_SUCCESS: &str = "DATABASE_RECOVERY_SUCCESS";

async fn cancel_stream_jobs(session: &mut Session) -> Result<Vec<u32>> {
    let ids = session
        .run("select ddl_id from rw_catalog.rw_ddl_progress;")
        .await?;
    let ids = ids.split('\n').collect::<Vec<_>>().join(",");
    let result = session.run(&format!("cancel jobs {};", ids)).await?;
    result
        .split('\n')
        .map(|s| {
            s.parse::<u32>()
                .map_err(|_e| anyhow!("failed to parse {}", s))
        })
        .collect::<Result<Vec<_>>>()
}

async fn wait_until(session: &mut Session, sql: &str, target: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(100), async {
        loop {
            if session.run(sql).await.unwrap() == target {
                return;
            }
            sleep(Duration::from_secs(1)).await;
        }
    })
    .await?;
    Ok(())
}

async fn database_id_mapping(session: &mut Session) -> Result<HashMap<String, u32>> {
    let rows = session.run("select name, id from rw_databases").await?;
    Ok(rows
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let (name, id) = line.rsplit_once(' ').unwrap();
            (name.to_owned(), u32::from_str(id.trim()).unwrap())
        })
        .collect())
}

async fn database_recovery_events(session: &mut Session) -> Result<HashMap<u32, Vec<String>>> {
    let rows = session
        .run(
            "select event_type,
       case event_type
           when 'DATABASE_RECOVERY_START' then info -> 'recovery' -> 'databaseStart' ->> 'databaseId'
           when 'DATABASE_RECOVERY_SUCCESS' then info -> 'recovery' -> 'databaseSuccess' ->> 'databaseId'
           when 'DATABASE_RECOVERY_FAILURE' then info -> 'recovery' -> 'databaseFailure' ->> 'databaseId'
           end as database_id
from rw_catalog.rw_event_logs
where event_type like '%DATABASE_RECOVERY%'
order by timestamp;",
        )
        .await?;
    let mut result = HashMap::new();
    for line in rows.lines().filter(|line| !line.trim().is_empty()) {
        let (event_type, id) = line.rsplit_once(' ').unwrap();
        result
            .entry(u32::from_str(id.trim())?)
            .or_insert_with(Vec::new)
            .push(event_type.to_owned());
    }
    Ok(result)
}

async fn wait_for_database_recovery_state(
    session: &mut Session,
    database_id: u32,
    target: &str,
) -> Result<()> {
    wait_until(
        session,
        &format!(
            "select recovery_state from rw_catalog.rw_recovery_info where database_id = {};",
            database_id
        ),
        target,
    )
    .await
}

async fn member_table_ids(session: &mut Session) -> Result<HashSet<u32>> {
    let rows = session
        .run(
            "SELECT jsonb_array_elements(member_tables)::int FROM rw_hummock_compaction_group_configs;",
        )
        .await?;
    rows.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| Ok(u32::from_str(line.trim())?))
        .collect()
}

async fn wait_for_stream_job_and_member_tables(
    session: &mut Session,
    baseline_member_tables: &HashSet<u32>,
) -> Result<HashSet<u32>> {
    for _ in 0..60 {
        let jobs = session.run("show jobs;").await?;
        if !jobs.trim().is_empty() {
            let current_member_tables = member_table_ids(session).await?;
            let created_member_tables = current_member_tables
                .difference(baseline_member_tables)
                .copied()
                .collect::<HashSet<_>>();
            if !created_member_tables.is_empty() {
                return Ok(created_member_tables);
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
    Err(anyhow!(
        "failed to observe a creating stream job with new member tables"
    ))
}

async fn compaction_group_id_by_table_id(
    session: &mut Session,
    table_id: u32,
) -> Result<CompactionGroupId> {
    Ok(session
        .run(format!(
            "SELECT id FROM rw_hummock_compaction_group_configs WHERE member_tables @> '[{table_id}]'::jsonb;"
        ))
        .await?
        .parse::<CompactionGroupId>()?)
}

async fn sstable_ids_by_table_id(session: &mut Session, table_id: u32) -> Result<Vec<u64>> {
    let rows = session
        .run(format!(
            "SELECT sstable_id FROM rw_hummock_sstables WHERE table_ids @> '[{table_id}]'::jsonb ORDER BY sstable_id;"
        ))
        .await?;
    rows.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| Ok(u64::from_str(line.trim())?))
        .collect()
}

async fn wait_for_sstable_ids_by_table_id(
    session: &mut Session,
    table_id: u32,
) -> Result<Vec<u64>> {
    for _ in 0..60 {
        let sstable_ids = sstable_ids_by_table_id(session, table_id).await?;
        if !sstable_ids.is_empty() {
            return Ok(sstable_ids);
        }
        sleep(Duration::from_secs(1)).await;
    }
    Err(anyhow!("failed to observe sstables for table {}", table_id))
}

async fn sstable_count_by_table_id_and_level(
    session: &mut Session,
    table_id: u32,
    level_id: u32,
) -> Result<usize> {
    Ok(session
        .run(format!(
            "SELECT COUNT(*) FROM rw_hummock_sstables WHERE table_ids @> '[{table_id}]'::jsonb AND level_id = {level_id};"
        ))
        .await?
        .parse::<usize>()?)
}

async fn non_l0_sstable_count_by_table_id(session: &mut Session, table_id: u32) -> Result<usize> {
    Ok(session
        .run(format!(
            "SELECT COUNT(*) FROM rw_hummock_sstables WHERE table_ids @> '[{table_id}]'::jsonb AND level_id > 0;"
        ))
        .await?
        .parse::<usize>()?)
}

async fn wait_for_jobs_cleared(session: &mut Session) -> Result<()> {
    for _ in 0..60 {
        if session.run("show jobs;").await?.trim().is_empty() {
            return Ok(());
        }
        sleep(Duration::from_secs(1)).await;
    }
    Err(anyhow!("streaming jobs are not cleared"))
}

fn init_logger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .try_init();
}

#[tokio::test]
async fn test_no_snapshot_backfill_background_ddl_cancel_database_recovery_manual_compaction() {
    init_logger();
    let mut config = Configuration::for_auto_parallelism(MAX_HEARTBEAT_INTERVAL_SEC, true);
    config.compute_nodes = 3;
    config.compute_node_cores = 2;
    config.compactor_nodes = 2;
    config.compute_resource_groups = HashMap::from([
        (1, "group1".to_owned()),
        (2, "group2".to_owned()),
        (3, "group3".to_owned()),
    ]);
    let mut cluster = Cluster::start(config).await.unwrap();
    let mut session = cluster.start_session();

    session
        .run("create database group1 with resource_group='group1'")
        .await
        .unwrap();
    session
        .run("create database group2 with resource_group='group2'")
        .await
        .unwrap();
    session.run("set rw_implicit_flush = true;").await.unwrap();

    let database_id_mapping = database_id_mapping(&mut session).await.unwrap();
    let group1_database_id = database_id_mapping["group1"];

    session.run("use group1;").await.unwrap();
    session.run("set background_ddl = true;").await.unwrap();
    session
        .run("set streaming_use_snapshot_backfill = false;")
        .await
        .unwrap();
    session.run("set backfill_rate_limit = 1;").await.unwrap();
    session
        .run("create table t (k int primary key, v int);")
        .await
        .unwrap();
    session
        .run("insert into t select i, i from generate_series(1, 10000) as i;")
        .await
        .unwrap();
    session.run("flush;").await.unwrap();

    let baseline_member_tables = member_table_ids(&mut session).await.unwrap();

    let mut create_session = cluster.start_session();
    tokio::spawn(async move {
        create_session.run("use group1;").await.unwrap();
        create_session
            .run("set background_ddl = true;")
            .await
            .unwrap();
        create_session
            .run("set streaming_use_snapshot_backfill = false;")
            .await
            .unwrap();
        create_session
            .run("set backfill_rate_limit = 1;")
            .await
            .unwrap();
        let _ = create_session
            .run("create materialized view mv1 as select * from t;")
            .await;
    });

    let created_member_tables =
        wait_for_stream_job_and_member_tables(&mut session, &baseline_member_tables)
            .await
            .unwrap();
    let candidate_table_id = *created_member_tables.iter().min().unwrap();
    wait_for_sstable_ids_by_table_id(&mut session, candidate_table_id)
        .await
        .unwrap();
    let candidate_compaction_group_id =
        compaction_group_id_by_table_id(&mut session, candidate_table_id)
            .await
            .unwrap();
    println!(
        "REPRO candidate table_id={candidate_table_id}, compaction_group_id={candidate_compaction_group_id}"
    );

    let mut monitor_session = cluster.start_session();
    monitor_session.run("use group2;").await.unwrap();

    cluster.simple_kill_nodes(["compute-1"]).await;
    wait_until(
        &mut monitor_session,
        "select count(*) from rw_catalog.rw_worker_nodes where host = '192.168.3.1';",
        "0",
    )
    .await
    .unwrap();

    wait_for_database_recovery_state(&mut monitor_session, group1_database_id, "RECOVERING")
        .await
        .unwrap();

    let mut cancel_session = cluster.start_session();
    let cancel_handle = tokio::spawn(async move { cancel_stream_jobs(&mut cancel_session).await });

    sleep(Duration::from_millis(100)).await;
    cluster.simple_restart_nodes(["compute-1"]).await;
    wait_all_database_recovered(&mut cluster).await;
    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC)).await;

    let cancel_result = cancel_handle.await.unwrap();
    tracing::info!(
        ?cancel_result,
        "cancel result after concurrent database recovery"
    );

    let database_recovery_events = database_recovery_events(&mut monitor_session)
        .await
        .unwrap();
    let group1_events = database_recovery_events
        .get(&group1_database_id)
        .cloned()
        .unwrap_or_default();
    assert!(
        group1_events.ends_with(&[
            DATABASE_RECOVERY_START.to_owned(),
            DATABASE_RECOVERY_SUCCESS.to_owned(),
        ]),
        "failed to observe database recovery for group1, got events: {group1_events:?}"
    );

    session.run("use group1;").await.unwrap();
    wait_for_jobs_cleared(&mut session).await.unwrap();
    let member_tables_after_recovery = member_table_ids(&mut session).await.unwrap();
    println!(
        "REPRO after recovery: candidate_present={}, member_tables={member_tables_after_recovery:?}",
        member_tables_after_recovery.contains(&candidate_table_id)
    );
    if !member_tables_after_recovery.contains(&candidate_table_id) {
        println!("REPRO path=unregistered_after_recovery candidate_table_id={candidate_table_id}");
        tracing::info!(
            candidate_table_id,
            ?member_tables_after_recovery,
            "candidate table was unregistered after database recovery and cancel; skip manual compaction check"
        );
        return;
    }

    let l0_sstable_count_before =
        sstable_count_by_table_id_and_level(&mut session, candidate_table_id, 0)
            .await
            .unwrap();
    println!(
        "REPRO path=manual_compaction candidate_table_id={candidate_table_id}, compaction_group_id={candidate_compaction_group_id}, l0_before={l0_sstable_count_before}"
    );
    tracing::info!(
        candidate_table_id,
        candidate_compaction_group_id,
        l0_sstable_count_before,
        "candidate table still exists after recovery; trigger manual compaction"
    );
    assert!(
        l0_sstable_count_before > 0,
        "expected candidate table {candidate_table_id} to have L0 SST before manual compaction"
    );

    cluster
        .trigger_manual_compaction(candidate_compaction_group_id, 0)
        .await
        .unwrap();
    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC)).await;

    let l0_sstable_count_after =
        sstable_count_by_table_id_and_level(&mut session, candidate_table_id, 0)
            .await
            .unwrap();
    let non_l0_sstable_count_after =
        non_l0_sstable_count_by_table_id(&mut session, candidate_table_id)
            .await
            .unwrap();
    println!(
        "REPRO manual_compaction_result candidate_table_id={candidate_table_id}, l0_before={l0_sstable_count_before}, l0_after={l0_sstable_count_after}, non_l0_after={non_l0_sstable_count_after}"
    );
    tracing::info!(
        candidate_table_id,
        candidate_compaction_group_id,
        l0_sstable_count_before,
        l0_sstable_count_after,
        non_l0_sstable_count_after,
        "manual compaction result for candidate table"
    );
    assert_eq!(
        l0_sstable_count_after, 0,
        "manual compaction failed to compact candidate table {candidate_table_id} L0 SSTs"
    );
    assert!(
        non_l0_sstable_count_after > 0,
        "manual compaction did not produce non-L0 SSTs for candidate table {candidate_table_id}"
    );
}
