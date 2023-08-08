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

use prometheus::{register_int_gauge_vec_with_registry, IntGauge, IntGaugeVec, Registry};
use rdkafka::statistics::Broker;
use rdkafka::Statistics;

pub struct RdKafkaStats {
    pub registry: Registry,

    pub top_ts: IntGaugeVec,
    pub top_time: IntGaugeVec,
    pub broker_stats: BrokerStats,
}

pub struct BrokerStats {
    pub registry: Registry,

    pub state_age: IntGaugeVec,
    pub outbuf_cnt: IntGaugeVec,
    pub outbuf_msg_cnt: IntGaugeVec,
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
            &["id"],
            registry.clone()
        )
        .unwrap();
        let avg = register_int_gauge_vec_with_registry!(
            get_metric_name("avg"),
            "Average value",
            &["id"],
            registry.clone()
        )
        .unwrap();
        let sum = register_int_gauge_vec_with_registry!(
            get_metric_name("sum"),
            "Sum of values",
            &["id"],
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
            &["id"],
            registry.clone()
        )
        .unwrap();
        let hdr_size = register_int_gauge_vec_with_registry!(
            get_metric_name("hdr.size"),
            "Size of the histogram header",
            &["id"],
            registry.clone()
        )
        .unwrap();
        let p50 = register_int_gauge_vec_with_registry!(
            get_metric_name("p50"),
            "50th percentile",
            &["id"],
            registry.clone()
        )
        .unwrap();
        let p75 = register_int_gauge_vec_with_registry!(
            get_metric_name("p75"),
            "75th percentile",
            &["id"],
            registry.clone()
        )
        .unwrap();
        let p90 = register_int_gauge_vec_with_registry!(
            get_metric_name("p90"),
            "90th percentile",
            &["id"],
            registry.clone()
        )
        .unwrap();
        let p95 = register_int_gauge_vec_with_registry!(
            get_metric_name("p95"),
            "95th percentile",
            &["id"],
            registry.clone()
        )
        .unwrap();
        let p99 = register_int_gauge_vec_with_registry!(
            get_metric_name("p99"),
            "99th percentile",
            &["id"],
            registry.clone()
        )
        .unwrap();
        let p99_99 = register_int_gauge_vec_with_registry!(
            get_metric_name("p99.99"),
            "99.99th percentile",
            &["id"],
            registry.clone()
        )
        .unwrap();
        let out_of_range = register_int_gauge_vec_with_registry!(
            get_metric_name("out.of.range"),
            "Out of range values",
            &["id"],
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

pub struct TopicStats {
    pub registry: Registry,

    pub metadata_age: IntGaugeVec,
    pub batch_size: StatsWindow,
    pub batch_cnt: StatsWindow,
    pub partitions: PartitionStats,
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
        let top_ts = register_int_gauge_vec_with_registry!(
            "rdkafka.top.ts",
            "librdkafka's internal monotonic clock (microseconds)",
            // we cannot tell whether it is for consumer or producer,
            // it may refer to source_id or sink_id
            &["id", "client_id", "type"],
            registry
        )
        .unwrap();
        let top_time = register_int_gauge_vec_with_registry!(
            "rdkafka.top.time",
            "Wall clock time in seconds since the epoch",
            &["id", "client_id", "type"],
            registry
        )
        .unwrap();

        let broker_stats = BrokerStats::new(registry.clone());
        RdKafkaStats {
            registry,
            top_ts,
            top_time,
            broker_stats,
        }
    }

    pub fn report(&self, id: &str, stats: &Statistics) {
        let client_id = stats.client_id.as_str();
        let type_ = stats.client_type.as_str();

        self.top_ts
            .with_label_values(&[id, client_id, type_])
            .set(stats.ts);
        self.top_time
            .with_label_values(&[id, client_id, type_])
            .set(stats.time);
    }
}

impl BrokerStats {
    pub fn new(registry: Registry) -> Self {
        let state_age = register_int_gauge_vec_with_registry!(
            "rdkafka.broker.state.age",
            "Age of the broker state in seconds",
            &["id", "node-name", "state"],
            registry
        )
        .unwrap();
        let outbuf_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka.broker.outbuf.cnt",
            "Number of messages waiting to be sent to broker",
            &["id", "node-name", "state"],
            registry
        )
        .unwrap();
        let outbuf_msg_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka.broker.outbuf.msg.cnt",
            "Number of messages waiting to be sent to broker",
            &["id", "node-name", "state"],
            registry
        )
        .unwrap();

        BrokerStats {
            registry,
            state_age,
            outbuf_cnt,
            outbuf_msg_cnt,
        }
    }

    pub fn report(&self, id: &str, stats: &Statistics) {
        for broker_stats in stats.brokers() {
            self.report_inner(id, broker_stats);
        }
    }

    fn report_inner(&self, id: &str, stats: &Broker) {
        let node_name = stats.nodename.as_str();
        let state = stats.state.as_str();

        self.state_age
            .with_label_values(&[id, node_name, state])
            .set(stats.state_age);
        self.outbuf_cnt
            .with_label_values(&[id, node_name, state])
            .set(stats.outbuf_cnt);
        self.outbuf_msg_cnt
            .with_label_values(&[id, node_name, state])
            .set(stats.outbuf_msg_cnt);
    }
}
