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
use std::time::Duration;

use anyhow::{Result, anyhow};
use rdkafka::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use risingwave_common::config::meta::default;
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;
#[cfg(madsim)]
use risingwave_pb::serverless_backfill_controller::node_group_controller_service_server::{
    NodeGroupControllerService, NodeGroupControllerServiceServer,
};
#[cfg(madsim)]
use risingwave_pb::serverless_backfill_controller::{ProvisionRequest, ProvisionResponse};
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;
#[cfg(madsim)]
use tonic::{Request, Response, Status};

#[cfg(madsim)]
const SERVERLESS_BACKFILL_RESOURCE_GROUP: &str = "serverless_rg";
#[cfg(madsim)]
const SERVERLESS_BACKFILL_CONTROLLER_ADDR: &str = "http://192.168.13.1:50051";
#[cfg(madsim)]
const SERVERLESS_BACKFILL_CONTROLLER_BIND_ADDR: &str = "0.0.0.0:50051";

#[cfg(madsim)]
struct FixedServerlessBackfillController;

#[cfg(madsim)]
#[async_trait::async_trait]
impl NodeGroupControllerService for FixedServerlessBackfillController {
    async fn provision(
        &self,
        _request: Request<ProvisionRequest>,
    ) -> std::result::Result<Response<ProvisionResponse>, Status> {
        Ok(Response::new(ProvisionResponse {
            resource_group: SERVERLESS_BACKFILL_RESOURCE_GROUP.to_owned(),
        }))
    }
}

#[cfg(madsim)]
fn start_fixed_serverless_backfill_controller() {
    madsim::runtime::Handle::current()
        .create_node()
        .name("serverless-backfill-controller")
        .ip([192, 168, 13, 1].into())
        .init(|| async {
            tonic::transport::Server::builder()
                .add_service(NodeGroupControllerServiceServer::new(
                    FixedServerlessBackfillController,
                ))
                .serve(
                    SERVERLESS_BACKFILL_CONTROLLER_BIND_ADDR
                        .parse()
                        .expect("valid serverless backfill controller bind address"),
                )
                .await
                .expect("serverless backfill controller should serve");
        })
        .build();
}

async fn assert_streaming_job_effective_resource_group(
    session: &mut Session,
    job_name: &str,
    expected_resource_group: &str,
) -> Result<()> {
    session
        .run(format!(
            "select resource_group from rw_streaming_jobs where name = '{}'",
            job_name
        ))
        .await?
        .assert_result_eq(expected_resource_group);
    Ok(())
}

#[derive(Clone, Copy)]
enum FragmentPlacementFilter {
    All,
    SourceBackfill,
    NonSourceBackfill,
}

async fn assert_streaming_job_actor_placement(
    cluster: &Cluster,
    session: &mut Session,
    job_name: &str,
    expected_resource_group: &str,
    fragment_filter: FragmentPlacementFilter,
) -> Result<()> {
    let job_id = session
        .run(format!(
            "select id from rw_streaming_jobs where name = '{}'",
            job_name
        ))
        .await?
        .trim()
        .parse::<u32>()?;
    let info = cluster.get_cluster_info().await?;
    let worker_resource_groups = info
        .worker_nodes
        .iter()
        .map(|worker| {
            (
                worker.id,
                worker
                    .resource_group()
                    .unwrap_or_else(|| DEFAULT_RESOURCE_GROUP.to_owned()),
            )
        })
        .collect::<HashMap<_, _>>();

    let table_fragments = info
        .table_fragments
        .iter()
        .find(|fragments| fragments.table_id.as_raw_id() == job_id)
        .ok_or_else(|| anyhow!("streaming job fragments not found for {job_name}({job_id})"))?;

    let mut actor_count = 0;
    for fragment in table_fragments.fragments.values() {
        let is_source_backfill = fragment
            .nodes
            .as_ref()
            .and_then(|node| node.find_source_backfill())
            .is_some();
        let should_check = match fragment_filter {
            FragmentPlacementFilter::All => true,
            FragmentPlacementFilter::SourceBackfill => is_source_backfill,
            FragmentPlacementFilter::NonSourceBackfill => !is_source_backfill,
        };
        if !should_check {
            continue;
        }

        for actor in &fragment.actors {
            actor_count += 1;
            let worker_id = table_fragments.actor_status[&actor.actor_id].worker_id();
            assert_eq!(
                worker_resource_groups[&worker_id], expected_resource_group,
                "actor {} of job {} is scheduled on worker {} outside resource group {}",
                actor.actor_id, job_name, worker_id, expected_resource_group
            );
        }
    }
    assert!(
        actor_count > 0,
        "job {job_name} has no actors matching the placement assertion"
    );

    Ok(())
}

#[cfg(madsim)]
async fn wait_streaming_job_actor_placement(
    cluster: &Cluster,
    session: &mut Session,
    job_name: &str,
    expected_resource_group: &str,
    fragment_filter: FragmentPlacementFilter,
) -> Result<()> {
    let mut last_error = None;
    for _ in 0..50 {
        match assert_streaming_job_actor_placement(
            cluster,
            session,
            job_name,
            expected_resource_group,
            fragment_filter,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(err) => last_error = Some(err),
        }
        sleep(Duration::from_millis(200)).await;
    }

    Err(last_error.unwrap_or_else(|| anyhow!("timed out waiting for actor placement")))
}

async fn assert_streaming_job_placement(
    cluster: &Cluster,
    session: &mut Session,
    job_name: &str,
    expected_resource_group: &str,
) -> Result<()> {
    assert_streaming_job_effective_resource_group(session, job_name, expected_resource_group)
        .await?;
    assert_streaming_job_actor_placement(
        cluster,
        session,
        job_name,
        expected_resource_group,
        FragmentPlacementFilter::All,
    )
    .await
}

async fn assert_non_source_backfill_placement(
    cluster: &Cluster,
    session: &mut Session,
    job_name: &str,
    expected_resource_group: &str,
) -> Result<()> {
    assert_streaming_job_actor_placement(
        cluster,
        session,
        job_name,
        expected_resource_group,
        FragmentPlacementFilter::NonSourceBackfill,
    )
    .await
}

async fn assert_source_backfill_placement(
    cluster: &Cluster,
    session: &mut Session,
    mv_name: &str,
    expected_resource_group: &str,
) -> Result<()> {
    assert_streaming_job_actor_placement(
        cluster,
        session,
        mv_name,
        expected_resource_group,
        FragmentPlacementFilter::SourceBackfill,
    )
    .await
}

#[tokio::test]
async fn test_resource_group() -> Result<()> {
    let mut config = Configuration::for_arrangement_backfill();

    config.compute_nodes = 3;
    config.compute_node_cores = 2;
    config.compute_resource_groups = HashMap::from([
        (1, DEFAULT_RESOURCE_GROUP.to_owned()),
        (2, "test".to_owned()),
        (3, "test".to_owned()),
    ]);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    cluster.simple_kill_nodes(["compute-2", "compute-3"]).await;

    let compute_node_timeout = default::meta::max_heartbeat_interval_sec() as u64;
    let meta_parallelism_ctrl_period = default::meta::parallelism_control_trigger_period_sec();

    sleep(Duration::from_secs(compute_node_timeout * 2)).await;

    session.run("create table t(v int)").await?;
    session
        .run("create materialized view m as select * from t")
        .await?;

    assert!(
        session
            .run("alter table t set resource_group to 'test'")
            .await
            .is_err()
    );
    assert!(
        session
            .run("alter materialized view m set resource_group to 'test'")
            .await
            .is_err()
    );

    cluster.simple_restart_nodes(["compute-2"]).await;

    sleep(Duration::from_secs(meta_parallelism_ctrl_period * 2)).await;

    let union_fragment = cluster
        .locate_one_fragment([identity_contains("union")])
        .await?;

    let mat_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("union"),
        ])
        .await?;

    assert_eq!(union_fragment.inner.actors.len(), 2);
    assert_eq!(mat_fragment.inner.actors.len(), 2);

    let _ = session
        .run("alter materialized view m set resource_group to 'test'")
        .await?;

    let mat_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("union"),
        ])
        .await?;

    assert_eq!(mat_fragment.inner.actors.len(), 2);

    cluster.simple_restart_nodes(["compute-3"]).await;

    sleep(Duration::from_secs(meta_parallelism_ctrl_period * 2)).await;

    let union_fragment = cluster
        .locate_one_fragment([identity_contains("union")])
        .await?;

    let mat_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("union"),
        ])
        .await?;

    assert_eq!(union_fragment.inner.actors.len(), 2);
    assert_eq!(mat_fragment.inner.actors.len(), 4);

    session
        .run("select resource_group from rw_streaming_jobs where name = 'm'")
        .await?
        .assert_result_eq("test");

    let _ = session
        .run("alter materialized view m reset resource_group;")
        .await?;

    session
        .run("select resource_group from rw_streaming_jobs where name = 'm'")
        .await?
        .assert_result_eq(DEFAULT_RESOURCE_GROUP);

    let union_fragment = cluster
        .locate_one_fragment([identity_contains("union")])
        .await?;

    let mat_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("union"),
        ])
        .await?;

    assert_eq!(union_fragment.inner.actors.len(), 2);
    assert_eq!(mat_fragment.inner.actors.len(), 2);

    Ok(())
}

#[cfg(madsim)]
#[tokio::test]
async fn test_mv_serverless_backfill_uses_active_resource_group_during_create() -> Result<()> {
    start_fixed_serverless_backfill_controller();

    let mut config = Configuration::for_arrangement_backfill();
    config.compute_nodes = 3;
    config.compute_node_cores = 2;
    config.serverless_backfill_controller_addr =
        Some(SERVERLESS_BACKFILL_CONTROLLER_ADDR.to_owned());
    config.compute_resource_groups = HashMap::from([
        (1, DEFAULT_RESOURCE_GROUP.to_owned()),
        (2, "mv_rg".to_owned()),
        (3, SERVERLESS_BACKFILL_RESOURCE_GROUP.to_owned()),
    ]);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("set streaming_use_arrangement_backfill = true")
        .await?;
    session.run("set background_ddl = true").await?;
    session.run("set backfill_rate_limit = 1").await?;

    session.run("create table rg_serverless_t(v int)").await?;
    session
        .run("insert into rg_serverless_t select * from generate_series(1, 1000)")
        .await?;
    session
        .run(
            "create materialized view rg_serverless_mv with (
                resource_group = 'mv_rg',
                cloud.serverless_backfill_enabled = true
            ) as select * from rg_serverless_t",
        )
        .await?;

    assert_streaming_job_effective_resource_group(&mut session, "rg_serverless_mv", "mv_rg")
        .await?;
    wait_streaming_job_actor_placement(
        &cluster,
        &mut session,
        "rg_serverless_mv",
        SERVERLESS_BACKFILL_RESOURCE_GROUP,
        FragmentPlacementFilter::All,
    )
    .await?;

    let job_id = session
        .run("select id from rw_streaming_jobs where name = 'rg_serverless_mv'")
        .await?;
    session
        .run(format!("cancel job {};", job_id.trim()))
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_create_streaming_jobs_resource_group_placement() -> Result<()> {
    let mut config = Configuration::for_arrangement_backfill();
    config.compute_nodes = 5;
    config.compute_node_cores = 2;
    config.compute_resource_groups = HashMap::from([
        (1, DEFAULT_RESOURCE_GROUP.to_owned()),
        (2, "source_rg".to_owned()),
        (3, "mv_rg".to_owned()),
        (4, "sink_rg".to_owned()),
        (5, "index_rg".to_owned()),
    ]);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("set streaming_use_arrangement_backfill = true")
        .await?;
    session
        .run("set streaming_use_shared_source = true")
        .await?;

    cluster.create_kafka_topics(HashMap::from([("rg_source_topic".to_owned(), 2)]));

    session
        .run(
            "create source rg_source (id int) with (
                connector = 'kafka',
                properties.bootstrap.server = '192.168.11.1:29092',
                topic = 'rg_source_topic',
                resource_group = 'source_rg'
            ) format plain encode json",
        )
        .await?;
    assert_streaming_job_placement(&cluster, &mut session, "rg_source", "source_rg").await?;

    session
        .run(
            "create materialized view rg_source_mv with (resource_group = 'mv_rg') as
             select count(*) as cnt from rg_source",
        )
        .await?;
    assert_streaming_job_effective_resource_group(&mut session, "rg_source_mv", "mv_rg").await?;
    assert_source_backfill_placement(&cluster, &mut session, "rg_source_mv", "source_rg").await?;
    assert_non_source_backfill_placement(&cluster, &mut session, "rg_source_mv", "mv_rg").await?;

    cluster
        .run_on_client(async move {
            let producer = ClientConfig::new()
                .set("bootstrap.servers", "192.168.11.1:29092")
                .create::<BaseProducer>()
                .await
                .expect("failed to create kafka producer");
            for id in 1..=3 {
                let payload = format!(r#"{{"id":{id}}}"#);
                producer
                    .send(
                        BaseRecord::to("rg_source_topic")
                            .payload(payload.as_bytes())
                            .key(""),
                    )
                    .expect("failed to send kafka record");
            }
            producer
                .flush(None)
                .await
                .expect("failed to flush producer");
        })
        .await;

    for _ in 0..30 {
        if session
            .run("select cnt >= 3 from rg_source_mv")
            .await?
            .trim()
            == "t"
        {
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }
    session
        .run("select cnt >= 3 from rg_source_mv")
        .await?
        .assert_result_eq("t");

    session
        .run("create table rg_t(id int primary key, v int)")
        .await?;
    session
        .run("insert into rg_t values (1, 10), (2, 20), (3, 30)")
        .await?;
    session
        .run("create materialized view rg_mv as select * from rg_t")
        .await?;

    session
        .run("create index rg_idx on rg_mv(id) with (resource_group = 'index_rg')")
        .await?;
    assert_streaming_job_placement(&cluster, &mut session, "rg_idx", "index_rg").await?;

    session.run("insert into rg_t values (4, 40)").await?;
    session.flush().await?;
    session
        .run("select v from rg_mv where id = 4")
        .await?
        .assert_result_eq("40");

    session
        .run("create sink rg_sink from rg_mv with (connector = 'blackhole', resource_group = 'sink_rg')")
        .await?;
    assert_streaming_job_placement(&cluster, &mut session, "rg_sink", "sink_rg").await?;
    session.flush().await?;

    Ok(())
}
