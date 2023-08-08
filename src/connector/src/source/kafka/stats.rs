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
