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

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
#[cfg(madsim)]
use clap::Parser;
use itertools::Itertools;
use risingwave_common::config::RwConfig;
use risingwave_common::id::FragmentId;
use risingwave_common::monitor::EndpointExt;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::meta_addr::MetaAddressStrategy;
#[cfg(madsim)]
use risingwave_common::util::tokio_util::sync::CancellationToken;
use risingwave_pb::common::WorkerType;
use risingwave_pb::common::worker_node::Property;
use risingwave_pb::monitor_service::GetTableCacheRefillStatsRequest;
use risingwave_pb::monitor_service::monitor_service_client::MonitorServiceClient;
use risingwave_rpc_client::MetaClient;
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use serde_json::Value;
use tokio::time::sleep;
use tonic::transport::Endpoint;

const DATABASE_RECOVERY_START: &str = "DATABASE_RECOVERY_START";
const DATABASE_RECOVERY_SUCCESS: &str = "DATABASE_RECOVERY_SUCCESS";
const COMPUTE_2_HOST: &str = "192.168.3.2";
const COMPUTE_4_HOST: &str = "192.168.3.4";

async fn assert_no_expected_global_recovery(session: &mut Session) -> Result<()> {
    let global_recovery_events = global_recovery_events(session).await?;

    assert!(
        global_recovery_events
            .iter()
            .all(|(_, reason)| ["adhoc recovery", "bootstrap"].contains(&&**reason)),
        "unexpected recovery reason: {:?}",
        global_recovery_events
    );
    Ok(())
}

const MAX_HEARTBEAT_INTERVAL_SEC: u64 = 10;

#[tokio::test]
async fn test_isolation_simple_two_databases() -> Result<()> {
    let (cluster, mut session) = prepare_isolation_env().await?;

    session.run("use group1").await?;
    session.run("create table t1 (v int);").await?;
    session.run("use group2").await?;
    session.run("create table t2 (v int);").await?;

    cluster.simple_kill_nodes(["compute-1"]).await;

    session.run("use group1").await?;

    let database_mapping = database_id_mapping(&mut session).await?;

    let group1_database_id = database_mapping["group1"];
    let group2_database_id = database_mapping["group2"];

    // should fail
    assert!(
        session
            .run("insert into t1 select * from generate_series(1, 100);")
            .await
            .is_err()
    );

    session.run("use group2").await?;
    session
        .run("insert into t2 select * from generate_series(1, 100);")
        .await?;

    cluster.simple_restart_nodes(["compute-1"]).await;

    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC)).await;

    session.run("use group1").await?;
    session
        .run("insert into t1 select * from generate_series(1, 100);")
        .await?;

    let mut database_recovery_events = database_recovery_events(&mut session).await?;

    assert!(!database_recovery_events.contains_key(&group2_database_id));
    assert_eq!(
        database_recovery_events.remove(&group1_database_id),
        Some(vec![
            DATABASE_RECOVERY_START.to_owned(),
            DATABASE_RECOVERY_SUCCESS.to_owned()
        ])
    );

    assert_no_expected_global_recovery(&mut session).await?;

    Ok(())
}

#[tokio::test]
async fn test_table_cache_refill_runtime_state_after_database_recovery_and_serving_node_change()
-> Result<()> {
    let (cluster, mut session) = prepare_refill_runtime_state_db_recovery_env().await?;

    let database_mapping = database_id_mapping(&mut session).await?;
    let group1_database_id = database_mapping["group1"];
    let group2_database_id = database_mapping["group2"];

    session.run("use group1").await?;
    session.run("create table t1(v1 int, v2 int);").await?;
    session.run("create table t2(v1 int, v3 int);").await?;
    session
        .run(
            "create materialized view mv3 as \
             select t1.v1, count(*) as cnt from t1 join t2 on t1.v1 = t2.v1 \
             group by t1.v1;",
        )
        .await?;
    session
        .run("alter materialized view mv3 set config(streaming.developer.cache_refill_policy = 'both');")
        .await?;

    let result_table_id = session
        .run("select id from rw_catalog.rw_materialized_views where name = 'mv3';")
        .await?
        .trim()
        .parse::<u32>()
        .context("failed to parse mv3 result table id")?;
    let internal_table_ids = session
        .run(format!(
            "select id from rw_catalog.rw_internal_table_info \
             where job_id = {result_table_id} \
             order by id;"
        ))
        .await?
        .lines()
        .map(|line| {
            line.trim()
                .parse::<u32>()
                .with_context(|| format!("failed to parse internal table id from {line:?}"))
        })
        .collect::<Result<Vec<_>>>()?;
    assert!(
        !internal_table_ids.is_empty(),
        "materialized view should have internal state tables"
    );
    let mut refill_table_ids = internal_table_ids.clone();
    refill_table_ids.push(result_table_id);

    wait_refill_policy_on_compute(&cluster, COMPUTE_2_HOST, &refill_table_ids, None).await?;

    session
        .run("insert into t2 select * from generate_series(1, 10);")
        .await?;

    cluster.simple_kill_nodes(["compute-1"]).await;
    wait_until(
        &mut session,
        "select count(*) from rw_catalog.rw_worker_nodes where host = '192.168.3.1';",
        "0",
    )
    .await?;

    wait_until_run_ok(
        &mut session,
        "insert into t1 select generate_series, generate_series from generate_series(1, 10);",
    )
    .await?;

    wait_refill_policy_on_compute(
        &cluster,
        COMPUTE_2_HOST,
        &internal_table_ids,
        Some("streaming"),
    )
    .await?;
    wait_refill_policy_on_compute(
        &cluster,
        COMPUTE_2_HOST,
        &[result_table_id],
        Some("serving"),
    )
    .await?;

    // Adding a serving compute node rebuilds serving vnode mappings and pushes the full
    // replacement to both existing and new serving nodes. The existing serving worker's
    // assignment may stay unchanged for some deterministic seeds, so only require it to
    // converge to the current meta-owned mapping.
    let expected_serving_before_add_node = expected_serving_refill_vnodes_on_compute(
        &cluster,
        &mut session,
        COMPUTE_2_HOST,
        result_table_id,
    )
    .await?;
    wait_serving_refill_on_compute(
        &cluster,
        COMPUTE_2_HOST,
        &refill_table_ids,
        &expected_serving_before_add_node,
    )
    .await?;
    create_compute_node(&cluster, 4, "serving");
    wait_until(
        &mut session,
        "select count(*) from rw_catalog.rw_worker_nodes where host = '192.168.3.4';",
        "1",
    )
    .await?;
    let expected_serving_on_old_worker = expected_serving_refill_vnodes_on_compute(
        &cluster,
        &mut session,
        COMPUTE_2_HOST,
        result_table_id,
    )
    .await?;
    wait_serving_refill_on_compute(
        &cluster,
        COMPUTE_2_HOST,
        &refill_table_ids,
        &expected_serving_on_old_worker,
    )
    .await?;
    wait_refill_policy_on_compute(
        &cluster,
        COMPUTE_4_HOST,
        &internal_table_ids,
        Some("streaming"),
    )
    .await?;
    wait_refill_policy_on_compute(
        &cluster,
        COMPUTE_4_HOST,
        &[result_table_id],
        Some("serving"),
    )
    .await?;
    let expected_serving_on_new_worker = expected_serving_refill_vnodes_on_compute(
        &cluster,
        &mut session,
        COMPUTE_4_HOST,
        result_table_id,
    )
    .await?;
    wait_serving_refill_on_compute(
        &cluster,
        COMPUTE_4_HOST,
        &refill_table_ids,
        &expected_serving_on_new_worker,
    )
    .await?;

    let mut database_recovery_events = database_recovery_events(&mut session).await?;
    assert!(!database_recovery_events.contains_key(&group2_database_id));
    assert_eq!(
        database_recovery_events.remove(&group1_database_id),
        Some(vec![
            DATABASE_RECOVERY_START.to_owned(),
            DATABASE_RECOVERY_SUCCESS.to_owned()
        ])
    );
    assert_no_expected_global_recovery(&mut session).await?;

    Ok(())
}

#[cfg(madsim)]
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
        .init(move || risingwave_compute::start(opts.clone(), CancellationToken::new()))
        .build();
}

#[cfg(not(madsim))]
fn create_compute_node(_cluster: &Cluster, _idx: usize, _role: &str) {
    panic!("dynamic compute node creation requires madsim");
}

async fn database_id_mapping(session: &mut Session) -> Result<HashMap<String, u32>> {
    let events = session.run("select name, id from rw_databases").await?;

    let databases: HashMap<_, _> = events
        .lines()
        .map(|line| {
            let (name, num_str) = line.rsplit_once(' ').unwrap();
            let num = u32::from_str(num_str.trim()).unwrap();
            (name.to_owned(), num)
        })
        .collect();

    Ok(databases)
}

async fn database_recovery_events(session: &mut Session) -> Result<HashMap<u32, Vec<String>>> {
    let events = session.run("
    select event_type,
       case event_type
           when 'DATABASE_RECOVERY_START' then info -> 'recovery' -> 'databaseStart' ->> 'databaseId'
           when 'DATABASE_RECOVERY_SUCCESS' then info -> 'recovery' -> 'databaseSuccess' ->> 'databaseId'
           when 'DATABASE_RECOVERY_FAILURE' then info -> 'recovery' -> 'databaseFailure' ->> 'databaseId'
           end as database_id
from rw_catalog.rw_event_logs
where event_type like '%DATABASE_RECOVERY%'
order by timestamp;").await?;

    let mut result = HashMap::new();

    for line in events.lines() {
        let (event_type, num_str) = line.rsplit_once(' ').unwrap();
        let num = u32::from_str(num_str.trim())?;
        result
            .entry(num)
            .or_insert_with(Vec::new)
            .push(event_type.to_owned());
    }

    Ok(result)
}

async fn global_recovery_events(session: &mut Session) -> Result<Vec<(String, String)>> {
    let events = session
        .run(
            "select event_type,
       case event_type
           when 'GLOBAL_RECOVERY_START' then info -> 'recovery' -> 'globalStart' ->> 'reason'
           when 'GLOBAL_RECOVERY_SUCCESS' then info -> 'recovery' -> 'globalSuccess' ->> 'reason'
           end as reason
from rw_catalog.rw_event_logs
where event_type like '%GLOBAL_RECOVERY%'
order by timestamp;",
        )
        .await?;

    let events = events
        .lines()
        .map(|line| {
            let (event_type, reason) = line.split_once(' ').unwrap();
            (event_type.to_owned(), reason.to_owned())
        })
        .collect_vec();

    Ok(events)
}

#[tokio::test]
async fn test_isolation_simple_two_databases_join() -> Result<()> {
    let (cluster, mut session) = prepare_isolation_env().await?;

    let database_id_mapping = database_id_mapping(&mut session).await?;

    let group1_database_id = database_id_mapping["group1"];
    let group2_database_id = database_id_mapping["group2"];

    session.run("use group1").await?;
    session.run("create table t1 (v int);").await?;
    session
        .run("insert into t1 select * from generate_series(1, 100);")
        .await?;
    session
        .run("create subscription sub1 from t1 with(retention = '1D');")
        .await?;

    session.run("use group2").await?;
    session.run("create table t2 (v int);").await?;

    session
        .run("insert into t2 select * from generate_series(1, 50);")
        .await?;

    wait_until(
        &mut session,
        "select count(*) from group1.public.t1;",
        "100",
    )
    .await?;

    session
        .run("create materialized view mv_join as select t2.v as v from group1.public.t1 join t2 on t1.v = t2.v;")
        .await?;

    wait_until(&mut session, "select count(*) from mv_join;", "50").await?;

    cluster.simple_kill_nodes(["compute-1"]).await;

    session.run("use group1").await?;

    // should fail
    assert!(session.run("insert into t1 values (1)").await.is_err());

    session.run("use group2").await?;
    session
        .run("insert into t2 select * from generate_series(51, 120)")
        .await?;

    wait_until(&mut session, "select max(v) from t2", "120").await?;
    wait_until(&mut session, "select count(*) from mv_join;", "100").await?;

    // Wait until the killed compute node is removed from `rw_worker_nodes`.
    let wait_worker_unregister_sql = "select count(*) from rw_catalog.rw_worker_nodes \
where host = '192.168.3.1';";
    wait_until(&mut session, wait_worker_unregister_sql, "0").await?;

    session.run("recover").await?;

    cluster.simple_restart_nodes(["compute-1"]).await;

    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC * 2)).await;

    session.run("use group1").await?;
    session
        .run("insert into t1 select * from generate_series(101, 110);")
        .await?;

    session.run("use group2").await?;

    // flush is only oriented to the current database, so flush is required here
    session.run("flush").await?;

    wait_until(&mut session, "select count(*) from mv_join;", "110").await?;

    let mut database_recovery_events = database_recovery_events(&mut session).await?;

    assert!(!database_recovery_events.contains_key(&group2_database_id));

    assert_eq!(
        database_recovery_events.remove(&group1_database_id),
        Some(vec![
            DATABASE_RECOVERY_START.to_owned(),
            DATABASE_RECOVERY_SUCCESS.to_owned(),
        ])
    );

    assert_no_expected_global_recovery(&mut session).await?;

    Ok(())
}

#[tokio::test]
async fn test_isolation_simple_two_databases_join_in_other() -> Result<()> {
    let (cluster, mut session) = prepare_isolation_env().await?;

    let database_id_mapping = database_id_mapping(&mut session).await?;

    let group1_database_id = database_id_mapping["group1"];
    let group2_database_id = database_id_mapping["group2"];
    let group3_database_id = database_id_mapping["group3"];

    // group1
    session.run("use group1").await?;
    session.run("create table t1 (v int);").await?;
    session
        .run("insert into t1 select * from generate_series(1, 100);")
        .await?;
    session
        .run("create subscription sub1 from t1 with(retention = '1D');")
        .await?;

    // group2
    session.run("use group2").await?;
    session.run("create table t2 (v int);").await?;

    session
        .run("insert into t2 select * from generate_series(1, 50);")
        .await?;
    session
        .run("create subscription sub2 from t2 with(retention = '1D');")
        .await?;

    // group3
    session.run("use group3").await?;
    session
        .run("create materialized view mv_join as select t2.v as v from group1.public.t1 join group2.public.t2 on t1.v = t2.v;")
        .await?;

    wait_until(&mut session, "select count(*) from mv_join;", "50").await?;

    cluster.simple_kill_nodes(["compute-1", "compute-3"]).await;

    session.run("use group1").await?;

    // should fail
    assert!(session.run("insert into t1 values (1)").await.is_err());

    // should fail
    assert!(session.run("flush").await.is_err());

    session.run("use group2").await?;
    session
        .run("insert into t2 select * from generate_series(51, 120)")
        .await?;

    session.run("flush").await?;

    cluster.simple_restart_nodes(["compute-1"]).await;

    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC)).await;

    session.run("use group1").await?;

    session
        .run("insert into t1 select * from generate_series(101, 110);")
        .await?;

    cluster.simple_restart_nodes(["compute-3"]).await;

    sleep(Duration::from_secs(MAX_HEARTBEAT_INTERVAL_SEC)).await;

    session.run("use group3").await?;

    wait_until(&mut session, "select count(*) from mv_join;", "110").await?;

    let mut database_recovery_events = database_recovery_events(&mut session).await?;

    assert!(!database_recovery_events.contains_key(&group2_database_id));

    assert_eq!(
        database_recovery_events.remove(&group1_database_id),
        Some(vec![
            DATABASE_RECOVERY_START.to_owned(),
            DATABASE_RECOVERY_SUCCESS.to_owned()
        ])
    );

    assert_eq!(
        database_recovery_events.remove(&group3_database_id),
        Some(vec![
            DATABASE_RECOVERY_START.to_owned(),
            DATABASE_RECOVERY_SUCCESS.to_owned()
        ])
    );

    assert_no_expected_global_recovery(&mut session).await?;

    Ok(())
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

async fn wait_until_run_ok(session: &mut Session, sql: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(100), async {
        loop {
            if session.run(sql).await.is_ok() {
                return;
            }
            sleep(Duration::from_secs(1)).await;
        }
    })
    .await?;

    Ok(())
}

async fn expected_serving_refill_vnodes_on_compute(
    cluster: &Cluster,
    session: &mut Session,
    worker_host: &str,
    result_table_id: u32,
) -> Result<HashMap<u32, Vec<u16>>> {
    let fragment_ids = session
        .run(format!(
            "select fragment_id from rw_catalog.rw_fragments \
             where table_id = {result_table_id} \
             and {result_table_id} = any(state_table_ids) \
             order by fragment_id;"
        ))
        .await?
        .lines()
        .map(|line| {
            line.trim()
                .parse::<u32>()
                .with_context(|| format!("failed to parse result fragment id from {line:?}"))
        })
        .collect::<Result<Vec<_>>>()?;
    let [result_fragment_id] = fragment_ids.as_slice() else {
        return Err(anyhow!(
            "result table {result_table_id} must have exactly one owning fragment, got {fragment_ids:?}"
        ));
    };
    let worker_id = cluster
        .get_cluster_info()
        .await?
        .worker_nodes
        .into_iter()
        .find(|worker| {
            worker.r#type() == WorkerType::ComputeNode
                && worker
                    .host
                    .as_ref()
                    .is_some_and(|host| host.host == worker_host)
        })
        .map(|worker| worker.id.as_raw_id())
        .with_context(|| format!("target compute node {worker_host} is missing"))?;

    let serving_vnode_mappings = cluster
        .run_on_client(async move {
            let meta_addr = "http://meta-1:5690".parse::<MetaAddressStrategy>()?;
            let host_addr = "serving-refill-test-meta-client:0".parse::<HostAddr>()?;
            let meta_config = Arc::new(RwConfig::default().meta);
            let (meta_client, _) = MetaClient::register_new(
                meta_addr,
                WorkerType::RiseCtl,
                &host_addr,
                Property::default(),
                meta_config,
            )
            .await;
            meta_client.list_serving_vnode_mappings().await
        })
        .await?;

    let (fragment_job_id, mapping) = serving_vnode_mappings
        .get(&FragmentId::new(*result_fragment_id))
        .with_context(|| {
            format!("serving vnode mapping missing result fragment {result_fragment_id}")
        })?;
    if fragment_job_id.as_raw_id() != result_table_id {
        return Err(anyhow!(
            "serving vnode mapping fragment {result_fragment_id} belongs to job {}, expected {result_table_id}",
            fragment_job_id.as_raw_id()
        ));
    }

    let mut vnodes = Vec::new();
    for (worker_slot_id, bitmap) in mapping.to_bitmaps() {
        if worker_slot_id.worker_id().as_raw_id() == worker_id {
            vnodes.extend(bitmap.iter_ones().map(|vnode| vnode as u16));
        }
    }
    vnodes.sort_unstable();
    vnodes.dedup();

    Ok(HashMap::from([(result_table_id, vnodes)]))
}

async fn table_cache_refill_stats_on_compute(
    cluster: &Cluster,
    worker_host: &str,
) -> Result<Value> {
    let worker_nodes = cluster.get_cluster_info().await?.worker_nodes;
    let worker = worker_nodes
        .into_iter()
        .find(|worker| {
            worker.r#type() == WorkerType::ComputeNode
                && worker
                    .host
                    .as_ref()
                    .is_some_and(|host| host.host == worker_host)
        })
        .context("target compute node is missing")?;
    let host = worker.host.context("compute node host is missing")?;
    let endpoint = format!("http://{}:{}", host.host, host.port);

    cluster
        .run_on_client(async move {
            let channel = Endpoint::from_shared(endpoint)?
                .connect_timeout(Duration::from_secs(5))
                .monitored_connect("grpc-table-cache-refill-stats-client", Default::default())
                .await?;
            let mut client = MonitorServiceClient::new(channel);
            let response = client
                .get_table_cache_refill_stats(GetTableCacheRefillStatsRequest {})
                .await?
                .into_inner();
            serde_json::from_str(&response.stats)
                .context("failed to parse table cache refill stats")
        })
        .await
}

fn normalize_serving_refill_vnodes(
    serving_vnodes: serde_json::Map<String, Value>,
) -> Result<HashMap<u32, Vec<u16>>> {
    serving_vnodes
        .into_iter()
        .map(|(table_id, vnodes)| {
            let table_id = table_id
                .parse::<u32>()
                .with_context(|| format!("failed to parse serving table id from {table_id:?}"))?;
            let mut vnodes = vnodes
                .as_array()
                .with_context(|| format!("serving vnodes for table {table_id} is not an array"))?
                .iter()
                .map(|vnode| {
                    vnode
                        .as_u64()
                        .and_then(|vnode| u16::try_from(vnode).ok())
                        .with_context(|| {
                            format!("failed to parse serving vnode for table {table_id}")
                        })
                })
                .try_collect::<_, Vec<_>, _>()?;
            vnodes.sort_unstable();
            Ok((table_id, vnodes))
        })
        .collect()
}

async fn wait_refill_policy_on_compute(
    cluster: &Cluster,
    worker_host: &str,
    table_ids: &[u32],
    expected_policy: Option<&str>,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(100), async {
        loop {
            if let Ok(stats) = table_cache_refill_stats_on_compute(cluster, worker_host).await
                && let Some(policies) = stats.get("policies").and_then(Value::as_object)
                && table_ids.iter().all(|table_id| {
                    policies.get(&table_id.to_string()).and_then(Value::as_str) == expected_policy
                })
            {
                return Ok::<(), anyhow::Error>(());
            }
            sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow!(
            "timed out waiting for table cache refill policy {:?} on {} for {:?}",
            expected_policy,
            worker_host,
            table_ids
        )
    })?
}

async fn wait_serving_refill_on_compute(
    cluster: &Cluster,
    worker_host: &str,
    target_table_ids: &[u32],
    expected: &HashMap<u32, Vec<u16>>,
) -> Result<HashMap<u32, Vec<u16>>> {
    let mut last_observed = None;
    tokio::time::timeout(Duration::from_secs(100), async {
        loop {
            if let Ok(stats) = table_cache_refill_stats_on_compute(cluster, worker_host).await
                && let Some(serving_vnodes) =
                    stats.get("serving").and_then(Value::as_object).cloned()
                && let Ok(serving_vnodes) = normalize_serving_refill_vnodes(serving_vnodes)
            {
                let target_serving_vnodes = target_table_ids
                    .iter()
                    .filter_map(|table_id| {
                        serving_vnodes
                            .get(table_id)
                            .map(|vnodes| (*table_id, vnodes.clone()))
                    })
                    .collect::<HashMap<_, _>>();
                if &target_serving_vnodes == expected {
                    return Ok::<_, anyhow::Error>(serving_vnodes);
                }
                last_observed = Some(target_serving_vnodes);
            }
            sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow!(
            "timed out waiting for serving refill vnodes on {} to match {:?}, last observed {:?}",
            worker_host,
            expected,
            last_observed
        )
    })?
}

async fn prepare_isolation_env() -> Result<(Cluster, Session)> {
    let mut config = Configuration::for_auto_parallelism(MAX_HEARTBEAT_INTERVAL_SEC, true);

    config.compute_nodes = 3;
    config.compute_node_cores = 2;
    config.compute_resource_groups = HashMap::from([
        (1, "group1".to_owned()),
        (2, "group2".to_owned()),
        (3, "group3".to_owned()),
    ]);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("create database group1 with resource_group='group1'")
        .await?;
    session
        .run("create database group2 with resource_group='group2'")
        .await?;
    session
        .run("create database group3 with resource_group='group3'")
        .await?;

    session.run("set rw_implicit_flush = true;").await?;

    Ok((cluster, session))
}

async fn prepare_refill_runtime_state_db_recovery_env() -> Result<(Cluster, Session)> {
    let mut config = Configuration::for_auto_parallelism(MAX_HEARTBEAT_INTERVAL_SEC, true);

    config.compute_nodes = 3;
    config.compute_node_cores = 2;
    config.compute_resource_groups = HashMap::from([
        (1, "group1".to_owned()),
        (2, "group1".to_owned()),
        (3, "group2".to_owned()),
    ]);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("create database group1 with resource_group='group1'")
        .await?;
    session
        .run("create database group2 with resource_group='group2'")
        .await?;
    session.run("set rw_implicit_flush = true;").await?;

    Ok((cluster, session))
}
