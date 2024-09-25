// Copyright 2024 RisingWave Labs
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

use std::collections::BTreeMap;

use anyhow::Result;
use itertools::Itertools;
use maplit::{convert_args, hashmap};
use risingwave_common::hash::WorkerSlotId;
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};

const CREATE_SOURCE: &str = r#"
CREATE SOURCE s(v1 int, v2 varchar) WITH (
    connector='kafka',
    properties.bootstrap.server='192.168.11.1:29092',
    topic='shared_source'
) FORMAT PLAIN ENCODE JSON;"#;

fn actor_upstream(fragment: &Fragment) -> Vec<(u32, Vec<u32>)> {
    fragment
        .actors
        .iter()
        .map(|actor| (actor.actor_id, actor.upstream_actor_id.clone()))
        .collect_vec()
}

async fn validate_splits_aligned(cluster: &mut Cluster) -> Result<()> {
    let source_backfill_fragment = cluster
        .locate_one_fragment([identity_contains("StreamSourceScan")])
        .await?;
    // The result of scaling is non-deterministic.
    // So we just print the result here, instead of asserting with a fixed value.
    let actor_upstream = actor_upstream(&source_backfill_fragment.inner);
    tracing::info!(
        "{}",
        actor_upstream
            .iter()
            .format_with("\n", |(actor_id, upstream), f| f(&format_args!(
                "{} <- {:?}",
                actor_id, upstream
            )))
    );
    let splits = cluster.list_source_splits().await?;
    tracing::info!("{:#?}", splits);
    let actor_splits: BTreeMap<u32, String> = splits
        .values()
        .flat_map(|m| m.clone().into_iter())
        .collect();
    for (actor, upstream) in actor_upstream {
        assert!(upstream.len() == 1, "invalid upstream: {:?}", upstream);
        let upstream_actor = upstream[0];
        assert_eq!(
            actor_splits.get(&actor).unwrap(),
            actor_splits.get(&upstream_actor).unwrap()
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_shared_source() -> Result<()> {
    tracing_subscriber::fmt::Subscriber::builder()
        .with_max_level(tracing::Level::ERROR)
        .with_env_filter("risingwave_stream::executor::source::source_backfill_executor=DEBUG,integration_tests=DEBUG")
        .init();

    let mut cluster = Cluster::start(Configuration::for_scale_shared_source()).await?;
    cluster.create_kafka_topics(convert_args!(hashmap!(
        "shared_source" => 4,
    )));
    let mut session = cluster.start_session();

    session.run("set rw_implicit_flush = true;").await?;

    session.run(CREATE_SOURCE).await?;
    session
        .run("create materialized view mv as select count(*) from s group by v1;")
        .await?;
    let source_fragment = cluster
        .locate_one_fragment([
            identity_contains("Source"),
            no_identity_contains("StreamSourceScan"),
        ])
        .await?;
    let source_workers = source_fragment.all_worker_count().into_keys().collect_vec();
    let source_backfill_fragment = cluster
        .locate_one_fragment([identity_contains("StreamSourceScan")])
        .await?;
    let source_backfill_workers = source_backfill_fragment
        .all_worker_count()
        .into_keys()
        .collect_vec();
    let hash_agg_fragment = cluster
        .locate_one_fragment([identity_contains("hashagg")])
        .await?;
    let hash_agg_workers = hash_agg_fragment
        .all_worker_count()
        .into_keys()
        .collect_vec();
    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 1 HASH {2} {} {SOURCE} 6
        2 3 HASH {4,3} {3} {MVIEW} 6
        3 3 HASH {5} {1} {SOURCE_SCAN} 6"#]]
    .assert_eq(&cluster.run("select * from rw_fragments;").await?);
    expect_test::expect![[r#"
        1 CREATED ADAPTIVE
        3 CREATED ADAPTIVE"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);

    // SourceBackfill cannot be scaled because of NoShuffle.
    assert!(
        &cluster
            .reschedule(
                source_backfill_fragment
                    .reschedule([WorkerSlotId::new(source_backfill_workers[0], 0)], []),
            )
            .await.unwrap_err().to_string().contains("rescheduling NoShuffle downstream fragment (maybe Chain fragment) is forbidden, please use NoShuffle upstream fragment (like Materialized fragment) to scale"),
    );

    // hash agg can be scaled independently
    cluster
        .reschedule(hash_agg_fragment.reschedule([WorkerSlotId::new(hash_agg_workers[0], 0)], []))
        .await
        .unwrap();
    expect_test::expect![[r#"
        1 1 HASH {2} {} {SOURCE} 6
        2 3 HASH {4,3} {3} {MVIEW} 5
        3 3 HASH {5} {1} {SOURCE_SCAN} 6"#]]
    .assert_eq(&cluster.run("select * from rw_fragments;").await?);

    // source is the NoShuffle upstream. It can be scaled, and the downstream SourceBackfill will be scaled together.
    cluster
        .reschedule(source_fragment.reschedule(
            [
                WorkerSlotId::new(source_workers[0], 0),
                WorkerSlotId::new(source_workers[0], 1),
                WorkerSlotId::new(source_workers[2], 0),
            ],
            [],
        ))
        .await
        .unwrap();
    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 1 HASH {2} {} {SOURCE} 3
        2 3 HASH {4,3} {3} {MVIEW} 5
        3 3 HASH {5} {1} {SOURCE_SCAN} 3"#]]
    .assert_eq(&cluster.run("select * from rw_fragments;").await?);
    expect_test::expect![[r#"
        1 CREATED CUSTOM
        3 CREATED CUSTOM"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);

    // resolve_no_shuffle for backfill fragment is OK, which will scale the upstream together.
    cluster
        .reschedule_resolve_no_shuffle(source_backfill_fragment.reschedule(
            [],
            [
                WorkerSlotId::new(source_workers[0], 0),
                WorkerSlotId::new(source_workers[0], 1),
                WorkerSlotId::new(source_workers[2], 0),
                WorkerSlotId::new(source_workers[2], 1),
            ],
        ))
        .await
        .unwrap();
    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 1 HASH {2} {} {SOURCE} 7
        2 3 HASH {4,3} {3} {MVIEW} 5
        3 3 HASH {5} {1} {SOURCE_SCAN} 7"#]]
    .assert_eq(&cluster.run("select * from rw_fragments;").await?);
    expect_test::expect![[r#"
1 CREATED CUSTOM
3 CREATED CUSTOM"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);
    Ok(())
}
