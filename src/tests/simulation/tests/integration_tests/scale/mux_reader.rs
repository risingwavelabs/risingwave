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

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
use maplit::{convert_args, hashmap};
use rdkafka::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
#[cfg(test)]
use risingwave_connector::source::kafka::source::mux_reader::KafkaMuxReader;
use risingwave_pb::id::ActorId;
use risingwave_simulation::cluster::{Cluster, Configuration};

use crate::scale::shared_source::validate_splits_aligned;

async fn produce_kafka_values(
    cluster: &Cluster,
    topic: &str,
    values: impl IntoIterator<Item = i64>,
) -> Result<()> {
    let payloads = values
        .into_iter()
        .map(|v| format!(r#"{{"v1": {v}}}"#))
        .collect::<Vec<_>>();
    let topic = topic.to_owned();
    cluster
        .run_on_client(async move {
            let producer: BaseProducer = ClientConfig::new()
                .set("bootstrap.servers", "192.168.11.1:29092")
                .create()
                .await
                .context("failed to create kafka producer")?;
            for payload in payloads {
                loop {
                    // Pin to partition 0 to avoid falling onto unassigned partitions in mux tests.
                    let record = BaseRecord::<(), [u8]>::to(&topic)
                        .partition(0)
                        .payload(payload.as_bytes());
                    match producer.send(record) {
                        Ok(_) => break,
                        Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                            producer
                                .flush(None)
                                .await
                                .context("failed to flush producer")?;
                        }
                        Err((e, _)) => return Err(anyhow!(e)),
                    }
                }
            }
            producer
                .flush(None)
                .await
                .context("failed to flush producer")?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("failed to run producer on client")?;
    Ok(())
}

async fn wait_for_row_count(cluster: &mut Cluster, sql: &str, expected: usize) -> Result<()> {
    let expected_str = expected.to_string();
    cluster
        .wait_until(
            sql.to_owned(),
            move |res| res.trim() == expected_str,
            Duration::from_millis(200),
            Duration::from_secs(30),
        )
        .await
        .with_context(|| format!("wait for {expected} rows via `{sql}`"))?;
    Ok(())
}

fn ensure_unique_assignments<K: Ord + std::fmt::Debug>(
    assignments: &BTreeMap<K, String>,
) -> Result<()> {
    tracing::info!(
        assignments = ?assignments,
        "Checking mux reader assignments for duplicates"
    );
    let mut seen = HashSet::new();
    for splits in assignments.values() {
        for split in splits.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            ensure!(
                seen.insert(split.to_owned()),
                "duplicate split detected in mux reader assignments: {split}"
            );
        }
    }
    Ok(())
}

/// Return actor ids of source fragments (excluding backfill) by inspecting `rw_fragments` flags.
async fn source_fragment_actor_ids(cluster: &mut Cluster) -> Result<HashSet<ActorId>> {
    let rows = cluster
        .run("select fragment_id, table_id, flags from rw_fragments where distribution_type = 'HASH'")
        .await?;
    let mut actor_ids = HashSet::new();
    for line in rows.trim().lines() {
        let cols: Vec<_> = line.split_whitespace().collect();
        if cols.len() < 3 {
            continue;
        }
        let fragment_id: i32 = cols[0].parse()?;
        let flags = cols[2];
        if flags.contains("SOURCE_SCAN") {
            continue;
        }
        let actors = cluster
            .run(&format!(
                "select actor_id from rw_actors where fragment_id = {fragment_id}"
            ))
            .await?;
        for actor in actors.trim().lines() {
            let actor_id: u32 = actor.parse()?;
            actor_ids.insert(ActorId::new(actor_id));
        }
    }
    Ok(actor_ids)
}

#[tokio::test]
async fn test_mux_reader_rescale_incremental_assign() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::Subscriber::builder()
        .with_max_level(tracing::Level::ERROR)
        .try_init();

    #[cfg(test)]
    KafkaMuxReader::clear_registry_for_test().await;

    let configuration = Configuration::for_scale_shared_source();
    let mut cluster = Cluster::start(configuration).await?;
    cluster.create_kafka_topics(convert_args!(hashmap!(
        "mux_rescale" => 4,
    )));

    let mut session = cluster.start_session();
    session.run("set rw_implicit_flush = true;").await?;
    session
        .run("create connection mux_conn_scale with (type = 'kafka', properties.bootstrap.server = '192.168.11.1:29092');")
        .await?;
    session
        .run(
            "create source mux_source_scale(v1 int) with \
        (connector = 'kafka', connection = mux_conn_scale, topic = 'mux_rescale', enable.mux.reader = true) \
        format plain encode json;",
        )
        .await?;
    session
        .run("create materialized view mux_mv_scale as select v1 from mux_source_scale;")
        .await?;

    produce_kafka_values(&cluster, "mux_rescale", [1_i64, 2, 3, 4]).await?;
    wait_for_row_count(&mut cluster, "select count(*) from mux_mv_scale", 4).await?;

    let assignments_before = cluster.list_source_splits().await?;
    let source_actor_ids: HashSet<ActorId> = source_fragment_actor_ids(&mut cluster).await?;
    ensure_unique_assignments(
        &assignments_before
            .into_iter()
            .filter(|(actor, _)| source_actor_ids.contains(actor))
            .collect(),
    )?;

    cluster
        .run("alter source mux_source_scale set parallelism = 3")
        .await?;
    validate_splits_aligned(&mut cluster).await?;

    let assignments_after = cluster.list_source_splits().await?;
    ensure_unique_assignments(
        &assignments_after
            .into_iter()
            .filter(|(actor, _)| source_actor_ids.contains(actor))
            .collect(),
    )?;

    produce_kafka_values(&cluster, "mux_rescale", [101_i64, 102, 103, 104]).await?;
    wait_for_row_count(&mut cluster, "select count(*) from mux_mv_scale", 8).await?;

    session.run("drop materialized view mux_mv_scale;").await?;
    session.run("drop source mux_source_scale;").await?;
    session.run("drop connection mux_conn_scale;").await?;
    Ok(())
}

#[tokio::test]
async fn test_mux_reader_release_handles_on_drop() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::Subscriber::builder()
        .with_max_level(tracing::Level::ERROR)
        .try_init();

    #[cfg(test)]
    KafkaMuxReader::clear_registry_for_test().await;

    let mut configuration = Configuration::for_scale_shared_source();
    let mut cluster = Cluster::start(configuration).await?;
    cluster.create_kafka_topics(convert_args!(hashmap!(
        "mux_release" => 4,
    )));

    let mut session = cluster.start_session();
    session.run("set rw_implicit_flush = true;").await?;
    session
        .run("create connection mux_conn with (type = 'kafka', properties.bootstrap.server = '192.168.11.1:29092');")
        .await?;
    session
        .run(
            "create source mux_source(v1 int) with \
        (connector = 'kafka', connection = mux_conn, topic = 'mux_release', enable.mux.reader = true) \
        format plain encode json;",
        )
        .await?;
    session
        .run("create materialized view mux_mv as select v1 from mux_source;")
        .await?;

    produce_kafka_values(&cluster, "mux_release", [1_i64, 2, 3]).await?;
    wait_for_row_count(&mut cluster, "select count(*) from mux_mv", 3).await?;

    session.run("drop materialized view mux_mv;").await?;
    session
        .run("create materialized view mux_mv as select v1 from mux_source;")
        .await?;
    let count_after_restart = session
        .run("select count(*) from mux_mv;")
        .await?
        .trim()
        .parse::<usize>()
        .context("failed to parse count after restarting mux view")?;
    // For shared source + mux reader, dropping downstream mview does not rewind offsets.
    // Recreating the same mview should keep previously ingested rows.
    ensure!(
        count_after_restart == 3,
        "expected to see existing 3 rows after restart, got {count_after_restart}"
    );

    produce_kafka_values(&cluster, "mux_release", [4_i64, 5, 6]).await?;
    wait_for_row_count(&mut cluster, "select count(*) from mux_mv", 6).await?;

    session.run("drop materialized view mux_mv;").await?;
    session.run("drop source mux_source;").await?;
    session.run("drop connection mux_conn;").await?;
    Ok(())
}
