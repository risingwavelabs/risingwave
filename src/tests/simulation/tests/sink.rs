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
#![cfg(madsim)]
use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use madsim::export::futures::StreamExt;
use madsim::time;
use rdkafka::consumer::StreamConsumer;
use rdkafka::message::BorrowedMessage;
use rdkafka::{ClientConfig, Message, TopicPartitionList};
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};

const ROOT_TABLE_CREATE: &str = "create table t (v1 int);";
const APPEND_ONLY_SINK_CREATE: &str = "create sink s1 from t with (connector='kafka', kafka.brokers='192.168.11.1:29092', kafka.topic='t_sink_append_only', format='append_only');";
const MV_CREATE: &str = "create materialized view m as select count(*) from t;";
const DEBEZIUM_SINK_CREATE: &str = "create sink s2 from m with (connector='kafka', kafka.brokers='192.168.11.1:29092', kafka.topic='t_sink_debezium', format='debezium');";

const APPEND_ONLY_TOPIC: &str = "t_sink_append_only";
const DEBEZIUM_TOPIC: &str = "t_sink_debezium";

use serde_derive::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppendOnlyPayload {
    pub v1: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DebeziumPayload {
    pub payload: Payload,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Payload {
    pub after: Option<After>,
    pub before: Option<Before>,
    pub op: String,
    #[serde(rename = "ts_ms")]
    pub ts_ms: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct After {
    pub count: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Before {
    pub count: i64,
}

#[madsim::test]
async fn test_sink_append_only() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;

    let mut topics = HashMap::new();
    topics.insert("t_sink_append_only".to_string(), 3);
    cluster.create_kafka_topics(topics);

    time::sleep(Duration::from_secs(10)).await;

    cluster.run(ROOT_TABLE_CREATE).await?;
    cluster.run(APPEND_ONLY_SINK_CREATE).await?;

    let materialize_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("globalSimpleAgg"),
        ])
        .await?;

    let id = materialize_fragment.id();
    cluster.reschedule(format!("{id}-[1,2,3,4,5]")).await?;

    let consumer: StreamConsumer<_> = cluster
        .run_on_client(async move {
            let consumer = ClientConfig::new()
                .set("bootstrap.servers", "192.168.11.1:29092")
                .set("group.id", "id")
                .create::<StreamConsumer<_>>()
                .await
                .expect("failed to create kafka consumer client");

            let mut tpl = TopicPartitionList::new();

            for i in 0..3 {
                tpl.add_partition(APPEND_ONLY_TOPIC, i);
            }

            consumer.assign(&tpl).unwrap();
            consumer
        })
        .await;

    let mut stream = consumer.stream();

    for i in &[1, 2, 3, 4, 5] {
        cluster
            .run(&format!("insert into t values ({});", i))
            .await?;
        cluster.run(&format!("flush;")).await?;

        let msg = stream.next().await.unwrap().unwrap();
        let payload = msg.payload().unwrap();

        check_payload(&msg, payload, *i);
    }

    Ok(())
}

#[madsim::test]
async fn test_sink_debezium() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;

    let mut topics = HashMap::new();
    topics.insert("t_sink_debezium".to_string(), 3);
    cluster.create_kafka_topics(topics);

    time::sleep(Duration::from_secs(10)).await;

    cluster.run(ROOT_TABLE_CREATE).await?;
    cluster.run(MV_CREATE).await?;
    cluster.run(DEBEZIUM_SINK_CREATE).await?;

    let materialize_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("globalSimpleAgg"),
        ])
        .await?;

    let id = materialize_fragment.id();
    cluster.reschedule(format!("{id}-[1,2,3,4,5]")).await?;

    let consumer: StreamConsumer<_> = cluster
        .run_on_client(async move {
            let consumer = ClientConfig::new()
                .set("bootstrap.servers", "192.168.11.1:29092")
                .set("group.id", "id")
                .create::<StreamConsumer<_>>()
                .await
                .expect("failed to create kafka consumer client");

            let mut tpl = TopicPartitionList::new();

            for i in 0..3 {
                tpl.add_partition(DEBEZIUM_TOPIC, i);
            }

            consumer.assign(&tpl).unwrap();
            consumer
        })
        .await;

    let mut stream = consumer.stream();

    for i in &[1, 2, 3, 4, 5] {
        cluster
            .run(&format!("insert into t values ({});", i))
            .await?;
        cluster.run(&format!("flush;")).await?;

        let msg = stream.next().await.unwrap().unwrap();
        let payload = msg.payload().unwrap();

        check_payload(&msg, payload, *i);
    }

    Ok(())
}

fn check_payload(msg: &BorrowedMessage, payload: &[u8], i: i64) {
    match msg.topic() {
        APPEND_ONLY_TOPIC => {
            let data: AppendOnlyPayload = serde_json::from_slice(payload).unwrap();
            assert_eq!(data.v1, i);
        }
        DEBEZIUM_TOPIC => {
            let data: DebeziumPayload = serde_json::from_slice(payload).unwrap();

            if i == 1 {
                assert_eq!(data.payload.op, "c".to_string());
                assert!(data.payload.before.is_none());
            } else {
                assert_eq!(data.payload.op, "u".to_string());
                let before = data.payload.before.as_ref().unwrap();
                assert_eq!(before.count, i - 1);
            }

            let after = data.payload.after.as_ref().unwrap();
            assert_eq!(after.count, i);
        }
        _ => unreachable!(),
    }
}
