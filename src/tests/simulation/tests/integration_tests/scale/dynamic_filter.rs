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

use std::collections::HashSet;
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_body;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

const SELECT: &str = "select * from mv1 order by v1;";

#[tokio::test]
async fn test_dynamic_filter() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    session.run("create table t1 (v1 int);").await?;
    session.run("create table t2 (v2 int);").await?;
    session.run("create materialized view mv1 as with max_v2 as (select max(v2) max from t2) select v1 from t1, max_v2 where v1 > max;").await?;
    session.run("insert into t1 values (1), (2), (3)").await?;
    session.run("flush").await?;
    sleep(Duration::from_secs(5)).await;

    let dynamic_filter_fragment = cluster
        .locate_one_fragment([identity_contains("dynamicFilter")])
        .await?;

    let materialize_fragments = cluster
        .locate_fragments([identity_contains("materialize")])
        .await?;

    let mut upstream_fragment_ids = HashSet::new();
    visit_stream_node_body(
        dynamic_filter_fragment.inner.nodes.as_ref().unwrap(),
        |node| {
            if let NodeBody::Merge(merge) = node {
                upstream_fragment_ids.insert(merge.upstream_fragment_id);
            }
        },
    );

    let fragment = materialize_fragments
        .iter()
        .find(|fragment| upstream_fragment_ids.contains(&fragment.id()))
        .unwrap();

    let id = fragment.id();

    let (worker_1, worker_2, worker_3) = fragment
        .all_worker_count()
        .into_keys()
        .collect_tuple::<(_, _, _)>()
        .unwrap();

    // prev -[1,2,3]
    cluster
        .reschedule(format!("{id}:[{worker_1}:-2, {worker_2}:-1]"))
        .await?;
    sleep(Duration::from_secs(3)).await;

    session.run(SELECT).await?.assert_result_eq("");
    session.run("insert into t2 values (0)").await?;
    session.run("flush").await?;
    sleep(Duration::from_secs(5)).await;
    session.run(SELECT).await?.assert_result_eq("1\n2\n3");
    // 1
    // 2
    // 3

    // prev -[4,5]+[1,2,3]
    cluster
        .reschedule(format!("{id}:[{worker_3}:-1, {worker_1}:2]"))
        .await?;
    sleep(Duration::from_secs(3)).await;
    session.run(SELECT).await?.assert_result_eq("1\n2\n3");

    session.run("insert into t2 values (2)").await?;
    session.run("flush").await?;
    sleep(Duration::from_secs(5)).await;
    session.run(SELECT).await?.assert_result_eq("3");
    // 3

    // prev -[1,2,3]+[4,5]
    cluster
        .reschedule(format!("{id}:[{worker_1}:-2, {worker_3}:1]"))
        .await?;
    sleep(Duration::from_secs(3)).await;
    session.run(SELECT).await?.assert_result_eq("3");

    session.run("update t2 set v2 = 1 where v2 = 2").await?;
    session.run("flush").await?;
    sleep(Duration::from_secs(5)).await;
    session.run(SELECT).await?.assert_result_eq("2\n3");
    // 2
    // 3
    //
    // prev +[1,2,3]
    cluster
        .reschedule(format!("{id}:[{worker_1}:2, {worker_2}:1]"))
        .await?;
    sleep(Duration::from_secs(3)).await;
    session.run(SELECT).await?.assert_result_eq("2\n3");

    session.run("delete from t2 where true").await?;
    session.run("flush").await?;
    sleep(Duration::from_secs(5)).await;
    session.run(SELECT).await?.assert_result_eq("");

    // prev -[1]
    cluster.reschedule(format!("{id}:[{worker_1}:1]")).await?;
    sleep(Duration::from_secs(3)).await;
    session.run(SELECT).await?.assert_result_eq("");

    session.run("insert into t2 values (1)").await?;
    session.run("flush").await?;
    sleep(Duration::from_secs(5)).await;
    session.run(SELECT).await?.assert_result_eq("2\n3");

    Ok(())
}
