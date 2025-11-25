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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
use itertools::Itertools;
use maplit::{convert_args, hashmap};
use rdkafka::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use risingwave_pb::id::ActorId;
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::stream_plan::DispatcherType;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};

const CREATE_SOURCE: &str = r#"
CREATE SOURCE s(v1 int, v2 varchar) WITH (
    connector='kafka',
    properties.bootstrap.server='192.168.11.1:29092',
    topic='shared_source'
) FORMAT PLAIN ENCODE JSON;"#;

/// Returns `Vec<(backfill_actor_id, source_actor_id)>`
fn source_backfill_upstream(
    source_backfill_fragment: &Fragment,
    source_fragment: &Fragment,
) -> Vec<(ActorId, ActorId)> {
    let mut no_shuffle_downstream_to_upstream = HashMap::new();
    for source_actor in &source_fragment.actors {
        for dispatcher in &source_actor.dispatcher {
            if dispatcher.r#type == DispatcherType::NoShuffle as i32 {
                assert_eq!(dispatcher.downstream_actor_id.len(), 1);
                let downstream_actor_id = dispatcher.downstream_actor_id[0];
                no_shuffle_downstream_to_upstream
                    .insert(downstream_actor_id, source_actor.actor_id);
            }
        }
    }

    let (_, source_fragment_id) = source_backfill_fragment
        .get_nodes()
        .unwrap()
        .find_source_backfill()
        .unwrap();
    assert_eq!(source_fragment.fragment_id, source_fragment_id);

    source_backfill_fragment
        .actors
        .iter()
        .map(|backfill_actor| {
            (
                backfill_actor.actor_id,
                no_shuffle_downstream_to_upstream[&backfill_actor.actor_id],
            )
        })
        .collect_vec()
}

async fn validate_splits_aligned(cluster: &mut Cluster) -> Result<()> {
    let source_backfill_fragment = cluster
        .locate_one_fragment([identity_contains("StreamSourceScan")])
        .await?;
    let source_fragment = cluster
        .locate_one_fragment([
            identity_contains("Source"),
            no_identity_contains("StreamSourceScan"),
        ])
        .await?;
    // The result of scaling is non-deterministic.
    // So we just print the result here, instead of asserting with a fixed value.
    let actor_upstream =
        source_backfill_upstream(&source_backfill_fragment.inner, &source_fragment.inner);
    tracing::info!(
        "{}",
        actor_upstream
            .iter()
            .format_with("\n", |(actor_id, upstream), f| f(&format_args!(
                "{} <- {:?}",
                actor_id, upstream
            )))
    );

    let actor_splits = cluster.list_source_splits().await?;
    for (actor, upstream) in actor_upstream {
        assert_eq!(
            actor_splits.get(&actor).unwrap(),
            actor_splits.get(&upstream).unwrap()
        );
    }
    Ok(())
}

async fn produce_kafka_values(
    cluster: &Cluster,
    topic: &str,
    values: impl IntoIterator<Item = i64>,
) -> Result<()> {
    let payloads = values
        .into_iter()
        .map(|v| format!(r#"{{"v1": {v}}}"#))
        .collect_vec();
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
                    let record = BaseRecord::<(), [u8]>::to(&topic).payload(payload.as_bytes());
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

fn ensure_unique_assignments<K: Ord>(assignments: &BTreeMap<K, String>) -> Result<()> {
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

#[tokio::test]
async fn test_shared_source() -> Result<()> {
    tracing_subscriber::fmt::Subscriber::builder()
        .with_max_level(tracing::Level::ERROR)
        .with_env_filter("risingwave_stream::executor::source::source_backfill_executor=DEBUG,integration_tests=DEBUG")
        .init();

    let configuration = Configuration::for_scale_shared_source();
    let total_cores = configuration.total_streaming_cores();
    let mut cluster = Cluster::start(configuration).await?;
    cluster.create_kafka_topics(convert_args!(hashmap!(
        "shared_source" => 4,
    )));
    let mut session = cluster.start_session();

    session.run("set rw_implicit_flush = true;").await?;

    session.run(CREATE_SOURCE).await?;
    session
        .run("create materialized view mv as select count(*) from s group by v1;")
        .await?;

    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 6 HASH {SOURCE} 6 256
        2 8 HASH {MVIEW} 6 256
        3 8 HASH {SOURCE_SCAN} 6 256"#]]
    .assert_eq(&cluster.run("select fragment_id, table_id, distribution_type, flags, parallelism, max_parallelism from rw_fragments;").await?);
    expect_test::expect![[r#"
        6 CREATED ADAPTIVE 256
        8 CREATED ADAPTIVE 256"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);

    // hash agg can be scaled independently
    cluster
        .run(format!(
            "alter materialized view mv set parallelism = {}",
            total_cores - 1
        ))
        .await?;

    expect_test::expect![[r#"
        1 6 HASH {SOURCE} 6 256
        2 8 HASH {MVIEW} 5 256
        3 8 HASH {SOURCE_SCAN} 6 256"#]]
    .assert_eq(&cluster.run("select fragment_id, table_id, distribution_type, flags, parallelism, max_parallelism from rw_fragments;").await?);

    // source is the NoShuffle upstream. It can be scaled, and the downstream SourceBackfill will be scaled together.
    cluster
        .run(format!(
            "alter source s set parallelism = {}",
            total_cores - 3
        ))
        .await?;

    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 6 HASH {SOURCE} 3 256
        2 8 HASH {MVIEW} 5 256
        3 8 HASH {SOURCE_SCAN} 3 256"#]]
    .assert_eq(&cluster.run("select fragment_id, table_id, distribution_type, flags, parallelism, max_parallelism from rw_fragments;").await?);
    expect_test::expect![[r#"
        6 CREATED FIXED(3) 256
        8 CREATED FIXED(5) 256"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);

    cluster.run("alter source s set parallelism = 7").await?;

    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 6 HASH {SOURCE} 7 256
        2 8 HASH {MVIEW} 5 256
        3 8 HASH {SOURCE_SCAN} 7 256"#]]
    .assert_eq(&cluster.run("select fragment_id, table_id, distribution_type, flags, parallelism, max_parallelism from rw_fragments;").await?);
    expect_test::expect![[r#"
        6 CREATED FIXED(7) 256
        8 CREATED FIXED(5) 256"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);
    Ok(())
}

#[tokio::test]
async fn test_issue_19563() -> Result<()> {
    tracing_subscriber::fmt::Subscriber::builder()
        .with_max_level(tracing::Level::ERROR)
        .with_env_filter("risingwave_stream::executor::source::source_backfill_executor=DEBUG,integration_tests=DEBUG")
        .init();

    let configuration = Configuration::for_scale_shared_source();
    let mut cluster = Cluster::start(configuration).await?;
    cluster.create_kafka_topics(convert_args!(hashmap!(
        "shared_source" => 4,
    )));
    let mut session = cluster.start_session();

    session.run("set rw_implicit_flush = true;").await?;

    session
        .run(
            r#"
CREATE SOURCE s(v1 timestamp with time zone) WITH (
    connector='kafka',
    properties.bootstrap.server='192.168.11.1:29092',
    topic='shared_source'
) FORMAT PLAIN ENCODE JSON;"#,
        )
        .await?;
    session
        .run("create materialized view mv1 as select v1 from s where v1 between now() and now() + interval '1 day' * 365 * 2000;")
        .await?;

    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 6 HASH {SOURCE} 6 256
        2 8 HASH {MVIEW,SOURCE_SCAN} 6 256
        3 8 SINGLE {NOW} 1 1
        4 8 SINGLE {NOW} 1 1"#]]
    .assert_eq(&cluster.run("select fragment_id, table_id, distribution_type, flags, parallelism, max_parallelism from rw_fragments;").await?);
    expect_test::expect![[r#"
        6 CREATED ADAPTIVE 256
        8 CREATED ADAPTIVE 256"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);

    // source is the NoShuffle upstream. It can be scaled, and the downstream SourceBackfill/DynamicFilter will be scaled together.
    cluster.run("alter source s set parallelism = 3").await?;

    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 6 HASH {SOURCE} 3 256
        2 8 HASH {MVIEW,SOURCE_SCAN} 3 256
        3 8 SINGLE {NOW} 1 1
        4 8 SINGLE {NOW} 1 1"#]]
    .assert_eq(&cluster.run("select fragment_id, table_id, distribution_type, flags, parallelism, max_parallelism from rw_fragments;").await?);
    expect_test::expect![[r#"
        6 CREATED FIXED(3) 256
        8 CREATED ADAPTIVE 256"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);

    // resolve_no_shuffle for backfill fragment is OK, which will scale the upstream together.
    cluster.run("alter source s set parallelism = 7").await?;

    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 6 HASH {SOURCE} 7 256
        2 8 HASH {MVIEW,SOURCE_SCAN} 7 256
        3 8 SINGLE {NOW} 1 1
        4 8 SINGLE {NOW} 1 1"#]]
    .assert_eq(&cluster.run("select fragment_id, table_id, distribution_type, flags, parallelism, max_parallelism from rw_fragments;").await?);
    expect_test::expect![[r#"
        6 CREATED FIXED(7) 256
        8 CREATED ADAPTIVE 256"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);
    Ok(())
}

#[tokio::test]
async fn test_mux_reader_release_handles_on_drop() -> Result<()> {
    let _ = tracing_subscriber::fmt::Subscriber::builder()
        .with_max_level(tracing::Level::ERROR)
        .try_init();

    let configuration = Configuration::for_scale_shared_source();
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
    ensure!(
        count_after_restart == 0,
        "expected clean restart, got {count_after_restart}"
    );

    produce_kafka_values(&cluster, "mux_release", [4_i64, 5, 6]).await?;
    wait_for_row_count(&mut cluster, "select count(*) from mux_mv", 3).await?;

    session.run("drop materialized view mux_mv;").await?;
    session.run("drop source mux_source;").await?;
    session.run("drop connection mux_conn;").await?;
    Ok(())
}

#[tokio::test]
async fn test_mux_reader_seek_to_latest_skips_backlog() -> Result<()> {
    let _ = tracing_subscriber::fmt::Subscriber::builder()
        .with_max_level(tracing::Level::ERROR)
        .try_init();

    let configuration = Configuration::for_scale_shared_source();
    let mut cluster = Cluster::start(configuration).await?;
    cluster.create_kafka_topics(convert_args!(hashmap!(
        "mux_seek" => 4,
    )));

    // Preload backlog before any readers exist.
    produce_kafka_values(&cluster, "mux_seek", [10_i64, 11, 12, 13]).await?;

    let mut session = cluster.start_session();
    session.run("set rw_implicit_flush = true;").await?;
    session
        .run("create connection mux_conn_seek with (type = 'kafka', properties.bootstrap.server = '192.168.11.1:29092');")
        .await?;
    session
        .run(
            "create source mux_source_seek(v1 int) with \
        (connector = 'kafka', connection = mux_conn_seek, topic = 'mux_seek', enable.mux.reader = true) \
        format plain encode json;",
        )
        .await?;
    session
        .run("create materialized view mux_mv_seek as select v1 from mux_source_seek;")
        .await?;

    cluster
        .wait_until(
            "select count(*) from mux_mv_seek",
            |res| {
                let trimmed = res.trim();
                trimmed == "0" || trimmed.is_empty()
            },
            Duration::from_millis(200),
            Duration::from_secs(30),
        )
        .await?;

    produce_kafka_values(&cluster, "mux_seek", [101_i64, 102, 103]).await?;
    wait_for_row_count(&mut cluster, "select count(*) from mux_mv_seek", 3).await?;

    session.run("drop materialized view mux_mv_seek;").await?;
    session.run("drop source mux_source_seek;").await?;
    session.run("drop connection mux_conn_seek;").await?;
    Ok(())
}

#[tokio::test]
async fn test_mux_reader_rescale_incremental_assign() -> Result<()> {
    let _ = tracing_subscriber::fmt::Subscriber::builder()
        .with_max_level(tracing::Level::ERROR)
        .try_init();

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
    ensure_unique_assignments(&assignments_before)?;

    cluster
        .run("alter source mux_source_scale set parallelism = 3")
        .await?;
    validate_splits_aligned(&mut cluster).await?;

    let assignments_after = cluster.list_source_splits().await?;
    ensure_unique_assignments(&assignments_after)?;

    produce_kafka_values(&cluster, "mux_rescale", [101_i64, 102, 103, 104]).await?;
    wait_for_row_count(&mut cluster, "select count(*) from mux_mv_scale", 8).await?;

    session.run("drop materialized view mux_mv_scale;").await?;
    session.run("drop source mux_source_scale;").await?;
    session.run("drop connection mux_conn_scale;").await?;
    Ok(())
}
