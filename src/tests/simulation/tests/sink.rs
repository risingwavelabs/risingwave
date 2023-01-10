// Copyright 2023 Singularity Data
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
//
// #![cfg(madsim)]
//
use std::collections::HashMap;
use std::convert::identity;
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use madsim::export::futures::StreamExt;
use madsim::time::sleep;
use rand::prelude::SliceRandom;
use rand::thread_rng;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};
use risingwave_simulation::utils::AssertResult;
use rdkafka::{ClientConfig, Message, TopicPartitionList};
use rdkafka::consumer::StreamConsumer;
use rdkafka::consumer::{Consumer};
use tokio::time;

const ROOT_TABLE_CREATE: &str = "create table t (v1 int);";
const APPEND_ONLY_SINK_CREATE: &str = "create sink s1 from t with (connector='kafka', kafka.brokers='192.168.11.1:29092', kafka.topic='t_sink_append_only', format='append_only');";
const MV_CREATE: &str = "create materialized view m as select count(*) from t;";
const DEBEZIUM_SINK_CREATE: &str = "create sink s2 from m with (connector='kafka', kafka.brokers='192.168.11.1:29092', kafka.topic='t_sink_debezium', format='debezium');";

#[madsim::test]
async fn test_sink() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;

    let mut topics = HashMap::new();
    topics.insert("t_sink_append_only".to_string(), 3);
    topics.insert("t_sink_debezium".to_string(), 3);
    cluster.create_kafka_topics(topics);

    time::sleep(Duration::from_secs(10)).await;

    cluster.run(ROOT_TABLE_CREATE).await?;
    cluster.run(APPEND_ONLY_SINK_CREATE).await?;
    cluster.run(MV_CREATE).await?;
    cluster.run(DEBEZIUM_SINK_CREATE).await?;

    let consumer = ClientConfig::new()
        .set("bootstrap.servers", "192.168.11.1:29092")
        .set("group.id", "id")
        .create::<StreamConsumer<_>>()
        .await
        .expect("failed to create kafka consumer client");

    let mut tpl = TopicPartitionList::new();

    for i in 0..3 {
        tpl.add_partition("t_sink_append_only", i);
        tpl.add_partition("t_sink_debezium", i);
    }

    consumer.assign(&tpl).unwrap();

    let materialize_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("globalSimpleAgg"),
        ])
        .await?;

    let id = materialize_fragment.id();
    cluster.reschedule(format!("{id}-[1,2,3,4,5]")).await?;

    let mut stream = consumer.stream();

    for i in &[1,2,3,4,5] {
        cluster.run(&format!("insert into t values ({});", i)).await?;
        cluster.run(&format!("flush;")).await?;

        let x = stream.next().await.unwrap().unwrap();
        println!("{}", x.topic());

        let x = stream.next().await.unwrap().unwrap();
        println!("{}", x.topic());

        // let x = consumer.recv().await.unwrap();

        // let x = consumer.recv().await.unwrap();
        // println!("{}", x.topic());
    }

    Ok(())
}
