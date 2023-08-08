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

use prometheus::core::{AtomicU64, GenericGaugeVec};
use prometheus::{opts, register_int_gauge_vec_with_registry, IntGaugeVec, Registry};
use rdkafka::statistics::Broker;
use rdkafka::Statistics;

#[macro_use]
macro_rules! register_gauge_vec {
    ($TYPE:ident, $OPTS:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let gauge_vec = prometheus::$TYPE::new($OPTS, $LABELS_NAMES).unwrap();
        $REGISTRY
            .register(Box::new(gauge_vec.clone()))
            .map(|_| gauge_vec)
    }};
}

#[macro_use]
macro_rules! register_uint_gauge_vec_with_registry {
    ($OPTS:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        register_gauge_vec!(GenericGaugeVec<AtomicU64>, $OPTS, $LABELS_NAMES, $REGISTRY)
    }};

    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        register_uint_gauge_vec_with_registry!(opts!($NAME, $HELP), $LABELS_NAMES, $REGISTRY)
    }};
}

pub struct RdKafkaStats {
    pub registry: Registry,

    pub ts: IntGaugeVec,
    pub time: IntGaugeVec,
    pub age: IntGaugeVec,
    pub replyq: IntGaugeVec,
    pub msg_cnt: GenericGaugeVec<AtomicU64>,
    pub msg_size: GenericGaugeVec<AtomicU64>,
    pub msg_max: GenericGaugeVec<AtomicU64>,
    pub msg_size_max: GenericGaugeVec<AtomicU64>,
    pub tx: IntGaugeVec,
    pub tx_bytes: IntGaugeVec,
    pub rx: IntGaugeVec,
    pub rx_bytes: IntGaugeVec,
    pub tx_msgs: IntGaugeVec,
    pub tx_msgs_bytes: IntGaugeVec,
    pub rx_msgs: IntGaugeVec,
    pub rx_msgs_bytes: IntGaugeVec,
    pub simple_cnt: IntGaugeVec,
    pub metadata_cache_cnt: IntGaugeVec,

    pub broker_stats: BrokerStats,
    pub topic_stats: TopicStats,
}

pub struct BrokerStats {
    pub registry: Registry,

    pub state: IntGaugeVec,
    pub state_age: IntGaugeVec,
    pub outbuf_cnt: IntGaugeVec,
    pub outbuf_msg_cnt: IntGaugeVec,
}

pub struct TopicStats {
    pub registry: Registry,

    pub metadata_age: IntGaugeVec,
    pub batch_size: StatsWindow,
    pub batch_cnt: StatsWindow,
    pub partitions: PartitionStats,
}

pub struct StatsWindow {
    pub registry: Registry,

    pub min: IntGaugeVec,
    pub max: IntGaugeVec,
    pub avg: IntGaugeVec,
    pub sum: IntGaugeVec,
    pub cnt: IntGaugeVec,
    pub stddev: IntGaugeVec,
    pub hdr_size: IntGaugeVec,
    pub p50: IntGaugeVec,
    pub p75: IntGaugeVec,
    pub p90: IntGaugeVec,
    pub p95: IntGaugeVec,
    pub p99: IntGaugeVec,
    pub p99_99: IntGaugeVec,
    pub out_of_range: IntGaugeVec,
}

pub struct ConsumerGroupStats {
    pub registry: Registry,

    // todo: (do not know value set) state: IntGaugeVec,
    pub state_age: IntGaugeVec,
    // todo: (do not know value set) join_state: IntGaugeVec,
    pub rebalance_age: IntGaugeVec,
    pub rebalance_cnt: IntGaugeVec,
    // todo: (cannot handle string) rebalance_reason,
    pub assignment_size: IntGaugeVec,
}

impl ConsumerGroupStats {
    pub fn new(registry: Registry) -> Self {
        let state_age = register_int_gauge_vec_with_registry!(
            "rdkafka.consumer.group.state.age",
            "Age of the consumer group state in seconds",
            &["id", "client_id"],
            registry.clone()
        )
        .unwrap();
    }
}

impl StatsWindow {
    pub fn new(registry: Registry, path: &str) -> Self {
        let get_metric_name = |name: &str| format!("rdkafka.{}.{}", path, name);
        let min = register_int_gauge_vec_with_registry!(
            get_metric_name("min"),
            "Minimum value",
            &["id", "client_id", "broker", "topic"],
            registry.clone()
        )
        .unwrap();
        let max = register_int_gauge_vec_with_registry!(
            get_metric_name("max"),
            "Maximum value",
            &["id", "client_id", "broker", "topic"],
            registry.clone()
        )
        .unwrap();
        let avg = register_int_gauge_vec_with_registry!(
            get_metric_name("avg"),
            "Average value",
            &["id", "client_id", "broker", "topic"],
            registry.clone()
        )
        .unwrap();
        let sum = register_int_gauge_vec_with_registry!(
            get_metric_name("sum"),
            "Sum of values",
            &["id", "client_id", "broker", "topic"],
            registry.clone()
        )
        .unwrap();
        let cnt = register_int_gauge_vec_with_registry!(
            get_metric_name("cnt"),
            "Count of values",
            &["id"],
            registry.clone()
        )
        .unwrap();
        let stddev = register_int_gauge_vec_with_registry!(
            get_metric_name("stddev"),
            "Standard deviation",
            &["id", "client_id", "broker", "topic"],
            registry.clone()
        )
        .unwrap();
        let hdr_size = register_int_gauge_vec_with_registry!(
            get_metric_name("hdr.size"),
            "Size of the histogram header",
            &["id", "client_id", "broker", "topic"],
            registry.clone()
        )
        .unwrap();
        let p50 = register_int_gauge_vec_with_registry!(
            get_metric_name("p50"),
            "50th percentile",
            &["id", "client_id", "broker", "topic"],
            registry.clone()
        )
        .unwrap();
        let p75 = register_int_gauge_vec_with_registry!(
            get_metric_name("p75"),
            "75th percentile",
            &["id", "client_id", "broker", "topic"],
            registry.clone()
        )
        .unwrap();
        let p90 = register_int_gauge_vec_with_registry!(
            get_metric_name("p90"),
            "90th percentile",
            &["id", "client_id", "broker", "topic"],
            registry.clone()
        )
        .unwrap();
        let p95 = register_int_gauge_vec_with_registry!(
            get_metric_name("p95"),
            "95th percentile",
            &["id", "client_id", "broker", "topic"],
            registry.clone()
        )
        .unwrap();
        let p99 = register_int_gauge_vec_with_registry!(
            get_metric_name("p99"),
            "99th percentile",
            &["id", "client_id", "broker", "topic"],
            registry.clone()
        )
        .unwrap();
        let p99_99 = register_int_gauge_vec_with_registry!(
            get_metric_name("p99.99"),
            "99.99th percentile",
            &["id", "client_id", "broker", "topic"],
            registry.clone()
        )
        .unwrap();
        let out_of_range = register_int_gauge_vec_with_registry!(
            get_metric_name("out.of.range"),
            "Out of range values",
            &["id", "client_id", "broker", "topic"],
            registry.clone()
        )
        .unwrap();

        Self {
            registry,
            min,
            max,
            avg,
            sum,
            cnt,
            stddev,
            hdr_size,
            p50,
            p75,
            p90,
            p95,
            p99,
            p99_99,
            out_of_range,
        }
    }
}

impl TopicStats {
    pub fn new(registry: Registry) -> Self {
        let metadata_age = register_int_gauge_vec_with_registry!(
            "rdkafka.topic.metadata.age",
            "Age of the topic metadata in milliseconds",
            &["id", "client_id", "broker", "topic"],
            registry.clone()
        )
        .unwrap();
        let batch_size = StatsWindow::new(registry.clone(), "topic.batchsize");
        let batch_cnt = StatsWindow::new(registry.clone(), "topic.batchcnt");
        let partitions = PartitionStats::new(registry.clone());
        Self {
            registry,
            metadata_age,
            batch_cnt,
            batch_size,
            partitions,
        }
    }
}

pub struct PartitionStats {
    pub registry: Registry,
}

impl PartitionStats {
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }
}

impl RdKafkaStats {
    pub fn new(registry: Registry) -> Self {
        let ts = register_int_gauge_vec_with_registry!(
            "rdkafka.top.ts",
            "librdkafka's internal monotonic clock (microseconds)",
            // we cannot tell whether it is for consumer or producer,
            // it may refer to source_id or sink_id
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let time = register_int_gauge_vec_with_registry!(
            "rdkafka.top.time",
            "Wall clock time in seconds since the epoch",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let age = register_int_gauge_vec_with_registry!(
            "rdkafka.top.age",
            "Age of the topic metadata in milliseconds",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let replyq = register_int_gauge_vec_with_registry!(
            "rdkafka.top.replyq",
            "Number of replies waiting to be served",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let msg_cnt = register_uint_gauge_vec_with_registry!(
            "rdkafka.top.msg.cnt",
            "Number of messages in all topics",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let msg_size = register_uint_gauge_vec_with_registry!(
            "rdkafka.top.msg.size",
            "Size of messages in all topics",
            &["id", "client_id"],
            registry
        )
        .unwarp();
        let msg_max = register_uint_gauge_vec_with_registry!(
            "rdkafka.top.msg.max",
            "Maximum message size in all topics",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let msg_size_max = register_uint_gauge_vec_with_registry!(
            "rdkafka.top.msg.size.max",
            "Maximum message size in all topics",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let tx = register_int_gauge_vec_with_registry!(
            "rdkafka.top.tx",
            "Number of transmitted messages",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let tx_bytes = register_int_gauge_vec_with_registry!(
            "rdkafka.top.tx.bytes",
            "Number of transmitted bytes",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let rx = register_int_gauge_vec_with_registry!(
            "rdkafka.top.rx",
            "Number of received messages",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let rx_bytes = register_int_gauge_vec_with_registry!(
            "rdkafka.top.rx.bytes",
            "Number of received bytes",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let tx_msgs = register_int_gauge_vec_with_registry!(
            "rdkafka.top.tx.msgs",
            "Number of transmitted messages",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let tx_msgs_bytes = register_int_gauge_vec_with_registry!(
            "rdkafka.top.tx.msgs.bytes",
            "Number of transmitted bytes",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let rx_msgs = register_int_gauge_vec_with_registry!(
            "rdkafka.top.rx.msgs",
            "Number of received messages",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let rx_msgs_bytes = register_int_gauge_vec_with_registry!(
            "rdkafka.top.rx.msgs.bytes",
            "Number of received bytes",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let simple_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka.top.simple.cnt",
            "Number of simple consumer queues",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let metadata_cache_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka.top.metadata.cache.cnt",
            "Number of entries in the metadata cache",
            &["id", "client_id"],
            registry
        )
        .unwrap();

        let broker_stats = BrokerStats::new(registry.clone());
        let topic_stats = TopicStats::new(registry.clone());
        RdKafkaStats {
            registry,
            ts,
            time,
            age,
            replyq,
            msg_cnt,
            msg_size,
            msg_max,
            msg_size_max,
            tx,
            tx_bytes,
            rx,
            rx_bytes,
            tx_msgs,
            tx_msgs_bytes,
            rx_msgs,
            rx_msgs_bytes,
            simple_cnt,
            metadata_cache_cnt,
            broker_stats,
            topic_stats,
        }
    }

    pub fn report(&self, id: &str, stats: &Statistics) {
        let client_id = stats.name.as_str();
        self.ts.with_label_values(&[id, client_id]).set(stats.ts);
        self.time
            .with_label_values(&[id, client_id])
            .set(stats.time);
        self.broker_stats.report(id, client_id, stats);
    }
}

impl BrokerStats {
    pub fn new(registry: Registry) -> Self {
        let state = register_int_gauge_vec_with_registry!(
            "rdkafka.broker.state",
            "Broker state",
            &["id", "client_id", "broker"],
            registry.clone()
        )
        .unwrap();
        let state_age = register_int_gauge_vec_with_registry!(
            "rdkafka.broker.state.age",
            "Age of the broker state in seconds",
            &["id", "client_id", "broker"],
            registry
        )
        .unwrap();
        let outbuf_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka.broker.outbuf.cnt",
            "Number of messages waiting to be sent to broker",
            &["id", "client_id", "broker"],
            registry
        )
        .unwrap();
        let outbuf_msg_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka.broker.outbuf.msg.cnt",
            "Number of messages waiting to be sent to broker",
            &["id", "client_id", "broker"],
            registry
        )
        .unwrap();

        BrokerStats {
            registry,
            state,
            state_age,
            outbuf_cnt,
            outbuf_msg_cnt,
        }
    }

    pub fn report(&self, id: &str, client_id: &str, stats: &Statistics) {
        for broker_stats in stats.brokers.values() {
            self.report_inner(id, client_id, broker_stats);
        }
    }

    fn report_inner(&self, id: &str, client_id: &str, stats: &Broker) {
        let broker = stats.nodename.as_str();

        if let Some(state_index) = state_str_to_index(stats.state.as_str()) {
            self.state
                .with_label_values(&[id, client_id, broker])
                .set(state_index);
        } else {
            tracing::warn!(
                "Unknown broker state: {}, id {}, client_id {}, broker {}",
                stats.state,
                id,
                client_id,
                broker
            );
        }

        self.state_age
            .with_label_values(&[id, client_id, broker])
            .set(stats.stateage);
        self.outbuf_cnt
            .with_label_values(&[id, client_id, broker])
            .set(stats.outbuf_cnt);
        self.outbuf_msg_cnt
            .with_label_values(&[id, client_id, broker])
            .set(stats.outbuf_msg_cnt);
    }
}

#[inline]
fn state_str_to_index(state_str: &str) -> Option<i64> {
    match state_str {
        "INIT" => Some(0),
        "DOWN" => Some(1),
        "CONNECT" => Some(2),
        "AUTH" => Some(3),
        "APIVERSION_QUERY" => Some(4),
        "AUTH_HANDSHAKE" => Some(5),
        "UP" => Some(6),
        "UPDATE" => Some(7),
        _ => None,
    }
}
