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

use anyhow::Result;
use futures::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::BorrowedMessage;
use rdkafka::{ClientConfig, Message, TopicPartitionList};
use risingwave_simulation::cluster::{Cluster, Configuration};
use tokio::time;

const ROOT_TABLE_CREATE: &str = "create table t (v1 int) append only;";
const APPEND_ONLY_SINK_CREATE: &str = "create sink s1 from t with (connector='kafka', properties.bootstrap.server='192.168.11.1:29092', topic='t_sink_append_only', type='append-only');";
const MV_CREATE: &str = "create materialized view m as select count(*) from t;";
const DEBEZIUM_SINK_CREATE: &str = "create sink s2 from m with (connector='kafka', properties.bootstrap.server='192.168.11.1:29092', topic='t_sink_debezium', type='debezium');";

const APPEND_ONLY_TOPIC: &str = "t_sink_append_only";
const DEBEZIUM_TOPIC: &str = "t_sink_debezium";

use risingwave_simulation::ctl_ext::predicate::identity_contains;
use serde::{Deserialize, Serialize};

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

#[tokio::test]
#[ignore] // https://github.com/risingwavelabs/risingwave/issues/12003
async fn test_sink_append_only() -> Result<()> {
    let configuration = Configuration::for_scale();
    let total_cores = configuration.total_streaming_cores();
    let mut cluster = Cluster::start(configuration).await?;

    let mut topics = HashMap::new();
    topics.insert(APPEND_ONLY_TOPIC.to_owned(), 3);
    cluster.create_kafka_topics(topics);

    time::sleep(Duration::from_secs(10)).await;

    cluster.run(ROOT_TABLE_CREATE).await?;
    cluster.run(APPEND_ONLY_SINK_CREATE).await?;

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

    check_kafka_after_insert(&mut cluster, &mut stream, &[1, 2, 3]).await?;
    cluster
        .run(format!(
            "alter sink s1 set parallelism = {}",
            total_cores - 5
        ))
        .await?;

    check_kafka_after_insert(&mut cluster, &mut stream, &[4, 5, 6]).await?;
    cluster
        .run(format!("alter sink s1 set parallelism = {}", total_cores))
        .await?;

    check_kafka_after_insert(&mut cluster, &mut stream, &[7, 8, 9]).await?;

    Ok(())
}

#[tokio::test]
#[ignore] // https://github.com/risingwavelabs/risingwave/issues/12003
async fn test_sink_debezium() -> Result<()> {
    let configuration = Configuration::for_scale();
    let total_cores = configuration.total_streaming_cores();
    let mut cluster = Cluster::start(configuration).await?;

    let mut topics = HashMap::new();
    topics.insert(DEBEZIUM_TOPIC.to_owned(), 3);
    cluster.create_kafka_topics(topics);

    time::sleep(Duration::from_secs(10)).await;

    cluster.run(ROOT_TABLE_CREATE).await?;
    cluster.run(MV_CREATE).await?;
    cluster.run(DEBEZIUM_SINK_CREATE).await?;

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

    let materialize_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            identity_contains("simpleAgg"),
        ])
        .await?;

    let used_worker_slots = materialize_fragment.used_worker_slots();

    assert_eq!(used_worker_slots.len(), 1);

    check_kafka_after_insert(&mut cluster, &mut stream, &[1, 2, 3]).await?;

    cluster
        .run("alter materialized view m set parallelism = 1")
        .await?;

    check_kafka_after_insert(&mut cluster, &mut stream, &[4, 5, 6]).await?;

    cluster
        .run(format!(
            "alter materialized view m set parallelism = {}",
            total_cores
        ))
        .await?;

    check_kafka_after_insert(&mut cluster, &mut stream, &[7, 8, 9]).await?;

    Ok(())
}

fn check_payload(msg: &BorrowedMessage<'_>, payload: &[u8], i: i64) {
    match msg.topic() {
        APPEND_ONLY_TOPIC => {
            let data: AppendOnlyPayload = serde_json::from_slice(payload).unwrap();
            assert_eq!(data.v1, i);
        }
        DEBEZIUM_TOPIC => {
            let data: DebeziumPayload = serde_json::from_slice(payload).unwrap();

            if i == 1 {
                assert_eq!(data.payload.op, "c".to_owned());
                assert!(data.payload.before.is_none());
            } else {
                assert_eq!(data.payload.op, "u".to_owned());
                let before = data.payload.before.as_ref().unwrap();
                assert_eq!(before.count, i - 1);
            }

            let after = data.payload.after.as_ref().unwrap();
            assert_eq!(after.count, i);
        }
        _ => unreachable!(),
    }
}

async fn check_kafka_after_insert(
    cluster: &mut Cluster,
    stream: &mut (impl futures::Stream<Item = KafkaResult<BorrowedMessage<'_>>> + Unpin),
    input: &[i64],
) -> Result<()> {
    for i in input {
        cluster
            .run(&format!("insert into t values ({});", i))
            .await?;
        cluster.run("flush;").await?;

        let msg = stream.next().await.unwrap().unwrap();
        let payload = msg.payload().unwrap();

        check_payload(&msg, payload, *i);
    }

    Ok(())
}
