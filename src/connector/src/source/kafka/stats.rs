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

use std::collections::HashMap;

use itertools::Itertools;
use prometheus::core::{AtomicU64, GenericGaugeVec};
use prometheus::{opts, register_int_gauge_vec_with_registry, IntGaugeVec, Registry};
use rdkafka::statistics::{Broker, ConsumerGroup, Partition, Topic, Window};
use rdkafka::Statistics;

type UintGaugeVec = GenericGaugeVec<AtomicU64>;

macro_rules! register_gauge_vec {
    ($TYPE:ident, $OPTS:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let gauge_vec = $TYPE::new($OPTS, $LABELS_NAMES).unwrap();
        $REGISTRY
            .register(Box::new(gauge_vec.clone()))
            .map(|_| gauge_vec)
    }};
}

macro_rules! register_uint_gauge_vec_with_registry {
    ($OPTS:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        register_gauge_vec!(UintGaugeVec, $OPTS, $LABELS_NAMES, $REGISTRY)
    }};

    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        register_uint_gauge_vec_with_registry!(opts!($NAME, $HELP), $LABELS_NAMES, $REGISTRY)
    }};
}

#[derive(Debug, Clone)]
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
    pub cgrp: ConsumerGroupStats,
}

#[derive(Debug, Clone)]
pub struct BrokerStats {
    pub registry: Registry,

    pub state_age: IntGaugeVec,
    pub outbuf_cnt: IntGaugeVec,
    pub outbuf_msg_cnt: IntGaugeVec,
    pub waitresp_cnt: IntGaugeVec,
    pub waitresp_msg_cnt: IntGaugeVec,
    pub tx: GenericGaugeVec<AtomicU64>,
    pub tx_bytes: GenericGaugeVec<AtomicU64>,
    pub tx_errs: GenericGaugeVec<AtomicU64>,
    pub tx_retries: GenericGaugeVec<AtomicU64>,
    pub tx_idle: IntGaugeVec,
    pub req_timeouts: GenericGaugeVec<AtomicU64>,
    pub rx: GenericGaugeVec<AtomicU64>,
    pub rx_bytes: GenericGaugeVec<AtomicU64>,
    pub rx_errs: GenericGaugeVec<AtomicU64>,
    pub rx_corriderrs: GenericGaugeVec<AtomicU64>,
    pub rx_partial: GenericGaugeVec<AtomicU64>,
    pub rx_idle: IntGaugeVec,
    pub req: IntGaugeVec,
    pub zbuf_grow: GenericGaugeVec<AtomicU64>,
    pub buf_grow: GenericGaugeVec<AtomicU64>,
    pub wakeups: GenericGaugeVec<AtomicU64>,
    pub connects: IntGaugeVec,
    pub disconnects: IntGaugeVec,
    pub int_latency: StatsWindow,
    pub outbuf_latency: StatsWindow,
    pub rtt: StatsWindow,
    pub throttle: StatsWindow,
}

#[derive(Debug, Clone)]
pub struct TopicStats {
    pub registry: Registry,

    pub metadata_age: IntGaugeVec,
    pub batch_size: StatsWindow,
    pub batch_cnt: StatsWindow,
    pub partitions: PartitionStats,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct ConsumerGroupStats {
    pub registry: Registry,

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
            "rdkafka_consumer_group_state_age",
            "Age of the consumer group state in seconds",
            &["id", "client_id", "state"],
            registry
        )
        .unwrap();
        let rebalance_age = register_int_gauge_vec_with_registry!(
            "rdkafka_consumer_group_rebalance_age",
            "Age of the last rebalance in seconds",
            &["id", "client_id", "state"],
            registry
        )
        .unwrap();
        let rebalance_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka_consumer_group_rebalance_cnt",
            "Number of rebalances",
            &["id", "client_id", "state"],
            registry
        )
        .unwrap();
        let assignment_size = register_int_gauge_vec_with_registry!(
            "rdkafka_consumer_group_assignment_size",
            "Number of assigned partitions",
            &["id", "client_id", "state"],
            registry
        )
        .unwrap();

        Self {
            registry,
            state_age,
            rebalance_age,
            rebalance_cnt,
            assignment_size,
        }
    }

    pub fn report(&self, id: &str, client_id: &str, stats: &ConsumerGroup) {
        let state = stats.state.as_str();
        self.state_age
            .with_label_values(&[id, client_id, state])
            .set(stats.stateage);
        self.rebalance_age
            .with_label_values(&[id, client_id, state])
            .set(stats.rebalance_age);
        self.rebalance_cnt
            .with_label_values(&[id, client_id, state])
            .set(stats.rebalance_cnt);
        self.assignment_size
            .with_label_values(&[id, client_id, state])
            .set(stats.assignment_size as i64);
    }
}

impl StatsWindow {
    pub fn new(registry: Registry, path: &str) -> Self {
        let get_metric_name = |name: &str| format!("rdkafka_{}_{}", path, name);
        let min = register_int_gauge_vec_with_registry!(
            get_metric_name("min"),
            "Minimum value",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let max = register_int_gauge_vec_with_registry!(
            get_metric_name("max"),
            "Maximum value",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let avg = register_int_gauge_vec_with_registry!(
            get_metric_name("avg"),
            "Average value",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let sum = register_int_gauge_vec_with_registry!(
            get_metric_name("sum"),
            "Sum of values",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let cnt = register_int_gauge_vec_with_registry!(
            get_metric_name("cnt"),
            "Count of values",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let stddev = register_int_gauge_vec_with_registry!(
            get_metric_name("stddev"),
            "Standard deviation",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let hdr_size = register_int_gauge_vec_with_registry!(
            get_metric_name("hdrsize"),
            "Size of the histogram header",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let p50 = register_int_gauge_vec_with_registry!(
            get_metric_name("p50"),
            "50th percentile",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let p75 = register_int_gauge_vec_with_registry!(
            get_metric_name("p75"),
            "75th percentile",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let p90 = register_int_gauge_vec_with_registry!(
            get_metric_name("p90"),
            "90th percentile",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let p95 = register_int_gauge_vec_with_registry!(
            get_metric_name("p95"),
            "95th percentile",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let p99 = register_int_gauge_vec_with_registry!(
            get_metric_name("p99"),
            "99th percentile",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let p99_99 = register_int_gauge_vec_with_registry!(
            get_metric_name("p99_99"),
            "99.99th percentile",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let out_of_range = register_int_gauge_vec_with_registry!(
            get_metric_name("out_of_range"),
            "Out of range values",
            &["id", "client_id", "broker", "topic"],
            registry
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

    pub fn report(&self, id: &str, client_id: &str, broker: &str, topic: &str, stats: &Window) {
        let labels = [id, client_id, broker, topic];

        self.min.with_label_values(&labels).set(stats.min);
        self.max.with_label_values(&labels).set(stats.max);
        self.avg.with_label_values(&labels).set(stats.avg);
        self.sum.with_label_values(&labels).set(stats.sum);
        self.cnt.with_label_values(&labels).set(stats.cnt);
        self.stddev.with_label_values(&labels).set(stats.stddev);
        self.hdr_size.with_label_values(&labels).set(stats.hdrsize);
        self.p50.with_label_values(&labels).set(stats.p50);
        self.p75.with_label_values(&labels).set(stats.p75);
        self.p90.with_label_values(&labels).set(stats.p90);
        self.p99_99.with_label_values(&labels).set(stats.p99_99);
        self.out_of_range
            .with_label_values(&labels)
            .set(stats.outofrange);
    }
}

impl TopicStats {
    pub fn new(registry: Registry) -> Self {
        let metadata_age = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_metadata_age",
            "Age of the topic metadata in milliseconds",
            &["id", "client_id", "topic"],
            registry
        )
        .unwrap();
        let batch_size = StatsWindow::new(registry.clone(), "topic_batchsize");
        let batch_cnt = StatsWindow::new(registry.clone(), "topic_batchcnt");
        let partitions = PartitionStats::new(registry.clone());
        Self {
            registry,
            metadata_age,
            batch_size,
            batch_cnt,
            partitions,
        }
    }

    pub fn report(
        &self,
        id: &str,
        client_id: &str,
        mapping: &HashMap<(String, i32), String>,
        stats: &Statistics,
    ) {
        for (topic, topic_stats) in &stats.topics {
            self.report_inner(id, client_id, topic, mapping, topic_stats);
        }
    }

    fn report_inner(
        &self,
        id: &str,
        client_id: &str,
        topic: &str,
        mapping: &HashMap<(String, i32), String>,
        stats: &Topic,
    ) {
        self.metadata_age
            .with_label_values(&[id, client_id, topic])
            .set(stats.metadata_age);
        self.batch_size
            .report(id, client_id, "", topic, &stats.batchsize);
        self.batch_cnt
            .report(id, client_id, "", topic, &stats.batchcnt);
        self.partitions.report(id, client_id, topic, mapping, stats)
    }
}

#[derive(Debug, Clone)]
pub struct PartitionStats {
    pub registry: Registry,

    pub msgq_cnt: IntGaugeVec,
    pub msgq_bytes: GenericGaugeVec<AtomicU64>,
    pub xmit_msgq_cnt: IntGaugeVec,
    pub xmit_msgq_bytes: GenericGaugeVec<AtomicU64>,
    pub fetchq_cnt: IntGaugeVec,
    pub fetchq_size: GenericGaugeVec<AtomicU64>,
    pub query_offset: IntGaugeVec,
    pub next_offset: IntGaugeVec,
    pub app_offset: IntGaugeVec,
    pub stored_offset: IntGaugeVec,
    pub committed_offset: IntGaugeVec,
    pub eof_offset: IntGaugeVec,
    pub lo_offset: IntGaugeVec,
    pub hi_offset: IntGaugeVec,
    pub consumer_lag: IntGaugeVec,
    pub consumer_lag_store: IntGaugeVec,
    pub txmsgs: GenericGaugeVec<AtomicU64>,
    pub txbytes: GenericGaugeVec<AtomicU64>,
    pub rxmsgs: GenericGaugeVec<AtomicU64>,
    pub rxbytes: GenericGaugeVec<AtomicU64>,
    pub msgs: GenericGaugeVec<AtomicU64>,
    pub rx_ver_drops: GenericGaugeVec<AtomicU64>,
    pub msgs_inflight: IntGaugeVec,
    pub next_ack_seq: IntGaugeVec,
    pub next_err_seq: IntGaugeVec,
    pub acked_msgid: GenericGaugeVec<AtomicU64>,
}

impl PartitionStats {
    pub fn new(registry: Registry) -> Self {
        let msgq_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_msgq_cnt",
            "Number of messages in the producer queue",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let msgq_bytes = register_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_msgq_bytes",
            "Size of messages in the producer queue",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let xmit_msgq_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_xmit_msgq_cnt",
            "Number of messages in the transmit queue",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let xmit_msgq_bytes = register_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_xmit_msgq_bytes",
            "Size of messages in the transmit queue",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let fetchq_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_fetchq_cnt",
            "Number of messages in the fetch queue",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let fetchq_size = register_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_fetchq_size",
            "Size of messages in the fetch queue",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let query_offset = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_query_offset",
            "Current query offset",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let next_offset = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_next_offset",
            "Next offset to query",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let app_offset = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_app_offset",
            "Last acknowledged offset",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let stored_offset = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_stored_offset",
            "Last stored offset",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let committed_offset = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_committed_offset",
            "Last committed offset",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let eof_offset = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_eof_offset",
            "Last offset in broker log",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let lo_offset = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_lo_offset",
            "Low offset",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let hi_offset = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_hi_offset",
            "High offset",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let consumer_lag = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_consumer_lag",
            "Consumer lag",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let consumer_lag_store = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_consumer_lag_store",
            "Consumer lag stored",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let txmsgs = register_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_txmsgs",
            "Number of transmitted messages",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let txbytes = register_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_txbytes",
            "Number of transmitted bytes",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let rxmsgs = register_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_rxmsgs",
            "Number of received messages",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let rxbytes = register_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_rxbytes",
            "Number of received bytes",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let msgs = register_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_msgs",
            "Number of messages in partition",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let rx_ver_drops = register_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_rx_ver_drops",
            "Number of received messages dropped due to version mismatch",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let msgs_inflight = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_msgs_inflight",
            "Number of messages in-flight",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let next_ack_seq = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_next_ack_seq",
            "Next ack sequence number",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let next_err_seq = register_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_next_err_seq",
            "Next error sequence number",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();
        let acked_msgid = register_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_acked_msgid",
            "Acknowledged message ID",
            &["id", "client_id", "broker", "topic", "partition"],
            registry
        )
        .unwrap();

        Self {
            registry,
            msgq_cnt,
            msgq_bytes,
            xmit_msgq_cnt,
            xmit_msgq_bytes,
            fetchq_cnt,
            fetchq_size,
            query_offset,
            next_offset,
            app_offset,
            stored_offset,
            committed_offset,
            eof_offset,
            lo_offset,
            hi_offset,
            consumer_lag,
            consumer_lag_store,
            txmsgs,
            txbytes,
            rxmsgs,
            rxbytes,
            msgs,
            rx_ver_drops,
            msgs_inflight,
            next_ack_seq,
            next_err_seq,
            acked_msgid,
        }
    }

    pub fn report(
        &self,
        id: &str,
        client_id: &str,
        topic: &str,
        broker_mapping: &HashMap<(String, i32), String>,
        stats: &Topic,
    ) {
        for partition_stats in stats.partitions.values() {
            self.report_inner(id, client_id, topic, broker_mapping, partition_stats);
        }
    }

    fn report_inner(
        &self,
        id: &str,
        client_id: &str,
        topic: &str,
        broker_mapping: &HashMap<(String, i32), String>,
        stats: &Partition,
    ) {
        let broker_name = match broker_mapping.get(&(topic.to_string(), stats.partition)) {
            Some(broker_name) => broker_name.as_str(),
            None => {
                tracing::warn!(
                    "Cannot find broker name for topic {} partition {}, id {}, client_id {}",
                    topic,
                    stats.partition,
                    id,
                    client_id
                );
                return;
            }
        };
        let labels = [
            id,
            client_id,
            broker_name,
            topic,
            &stats.partition.to_string(),
        ];

        self.msgq_cnt.with_label_values(&labels).set(stats.msgq_cnt);
        self.msgq_bytes
            .with_label_values(&labels)
            .set(stats.msgq_bytes);
        self.xmit_msgq_cnt
            .with_label_values(&labels)
            .set(stats.xmit_msgq_cnt);
        self.xmit_msgq_bytes
            .with_label_values(&labels)
            .set(stats.xmit_msgq_bytes);
        self.fetchq_cnt
            .with_label_values(&labels)
            .set(stats.fetchq_cnt);
        self.fetchq_size
            .with_label_values(&labels)
            .set(stats.fetchq_size);
        self.query_offset
            .with_label_values(&labels)
            .set(stats.query_offset);
        self.next_offset
            .with_label_values(&labels)
            .set(stats.next_offset);
        self.app_offset
            .with_label_values(&labels)
            .set(stats.app_offset);
        self.stored_offset
            .with_label_values(&labels)
            .set(stats.stored_offset);
        self.committed_offset
            .with_label_values(&labels)
            .set(stats.committed_offset);
        self.eof_offset
            .with_label_values(&labels)
            .set(stats.eof_offset);
        self.lo_offset
            .with_label_values(&labels)
            .set(stats.lo_offset);
        self.hi_offset
            .with_label_values(&labels)
            .set(stats.hi_offset);
        self.consumer_lag
            .with_label_values(&labels)
            .set(stats.consumer_lag);
        self.consumer_lag_store
            .with_label_values(&labels)
            .set(stats.consumer_lag_stored);
        self.txmsgs.with_label_values(&labels).set(stats.txmsgs);
        self.txbytes.with_label_values(&labels).set(stats.txbytes);
        self.rxmsgs.with_label_values(&labels).set(stats.rxmsgs);
        self.rxbytes.with_label_values(&labels).set(stats.rxbytes);
        self.msgs.with_label_values(&labels).set(stats.msgs);
        self.rx_ver_drops
            .with_label_values(&labels)
            .set(stats.rx_ver_drops);
        self.msgs_inflight
            .with_label_values(&labels)
            .set(stats.msgs_inflight);
        self.next_ack_seq
            .with_label_values(&labels)
            .set(stats.next_ack_seq);
        self.next_err_seq
            .with_label_values(&labels)
            .set(stats.next_err_seq);
        self.acked_msgid
            .with_label_values(&labels)
            .set(stats.acked_msgid);
    }
}

impl RdKafkaStats {
    pub fn new(registry: Registry) -> Self {
        let ts = register_int_gauge_vec_with_registry!(
            "rdkafka_top_ts",
            "librdkafka's internal monotonic clock (microseconds)",
            // we cannot tell whether it is for consumer or producer,
            // it may refer to source_id or sink_id
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let time = register_int_gauge_vec_with_registry!(
            "rdkafka_top_time",
            "Wall clock time in seconds since the epoch",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let age = register_int_gauge_vec_with_registry!(
            "rdkafka_top_age",
            "Age of the topic metadata in milliseconds",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let replyq = register_int_gauge_vec_with_registry!(
            "rdkafka_top_replyq",
            "Number of replies waiting to be served",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let msg_cnt = register_uint_gauge_vec_with_registry!(
            "rdkafka_top_msg_cnt",
            "Number of messages in all topics",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let msg_size = register_uint_gauge_vec_with_registry!(
            "rdkafka_top_msg_size",
            "Size of messages in all topics",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let msg_max = register_uint_gauge_vec_with_registry!(
            "rdkafka_top_msg_max",
            "Maximum message size in all topics",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let msg_size_max = register_uint_gauge_vec_with_registry!(
            "rdkafka_top_msg_size_max",
            "Maximum message size in all topics",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let tx = register_int_gauge_vec_with_registry!(
            "rdkafka_top_tx",
            "Number of transmitted messages",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let tx_bytes = register_int_gauge_vec_with_registry!(
            "rdkafka_top_tx_bytes",
            "Number of transmitted bytes",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let rx = register_int_gauge_vec_with_registry!(
            "rdkafka_top_rx",
            "Number of received messages",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let rx_bytes = register_int_gauge_vec_with_registry!(
            "rdkafka_top_rx_bytes",
            "Number of received bytes",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let tx_msgs = register_int_gauge_vec_with_registry!(
            "rdkafka_top_tx_msgs",
            "Number of transmitted messages",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let tx_msgs_bytes = register_int_gauge_vec_with_registry!(
            "rdkafka_top_tx_msgs_bytes",
            "Number of transmitted bytes",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let rx_msgs = register_int_gauge_vec_with_registry!(
            "rdkafka_top_rx_msgs",
            "Number of received messages",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let rx_msgs_bytes = register_int_gauge_vec_with_registry!(
            "rdkafka_top_rx_msgs_bytes",
            "Number of received bytes",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let simple_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka_top_simple_cnt",
            "Number of simple consumer queues",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let metadata_cache_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka_top_metadata_cache_cnt",
            "Number of entries in the metadata cache",
            &["id", "client_id"],
            registry
        )
        .unwrap();

        let broker_stats = BrokerStats::new(registry.clone());
        let topic_stats = TopicStats::new(registry.clone());
        let cgrp = ConsumerGroupStats::new(registry.clone());
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
            cgrp,
        }
    }

    pub fn report(&self, id: &str, stats: &Statistics) {
        let topic_partition_to_broker_mapping = get_topic_partition_to_broker_mapping(stats);

        let client_id = stats.name.as_str();
        self.ts.with_label_values(&[id, client_id]).set(stats.ts);
        self.time
            .with_label_values(&[id, client_id])
            .set(stats.time);
        self.age.with_label_values(&[id, client_id]).set(stats.age);
        self.replyq
            .with_label_values(&[id, client_id])
            .set(stats.replyq);
        self.msg_cnt
            .with_label_values(&[id, client_id])
            .set(stats.msg_cnt);
        self.msg_size
            .with_label_values(&[id, client_id])
            .set(stats.msg_size);
        self.msg_max
            .with_label_values(&[id, client_id])
            .set(stats.msg_max);
        self.msg_size_max
            .with_label_values(&[id, client_id])
            .set(stats.msg_size_max);
        self.tx.with_label_values(&[id, client_id]).set(stats.tx);
        self.tx_bytes
            .with_label_values(&[id, client_id])
            .set(stats.tx_bytes);
        self.rx.with_label_values(&[id, client_id]).set(stats.rx);
        self.rx_bytes
            .with_label_values(&[id, client_id])
            .set(stats.rx_bytes);
        self.tx_msgs
            .with_label_values(&[id, client_id])
            .set(stats.txmsgs);
        self.tx_msgs_bytes
            .with_label_values(&[id, client_id])
            .set(stats.txmsg_bytes);
        self.rx_msgs
            .with_label_values(&[id, client_id])
            .set(stats.rxmsgs);
        self.rx_msgs_bytes
            .with_label_values(&[id, client_id])
            .set(stats.rxmsg_bytes);
        self.simple_cnt
            .with_label_values(&[id, client_id])
            .set(stats.simple_cnt);
        self.metadata_cache_cnt
            .with_label_values(&[id, client_id])
            .set(stats.metadata_cache_cnt);

        self.broker_stats.report(id, client_id, stats);
        self.topic_stats
            .report(id, client_id, &topic_partition_to_broker_mapping, stats);
        if let Some(cgrp) = &stats.cgrp {
            self.cgrp.report(id, client_id, cgrp)
        }
    }
}

#[inline]
fn get_topic_partition_to_broker_mapping(stats: &Statistics) -> HashMap<(String, i32), String> {
    let topic_partition_to_broker_mapping = stats
        .brokers
        .values()
        .flat_map(|broker| {
            let broker_name = &broker.name;
            broker
                .toppars
                .iter()
                .map(|(topic, partition)| {
                    ((topic.clone(), partition.partition), broker_name.clone())
                })
                .collect_vec()
        })
        .collect::<HashMap<_, _>>();

    topic_partition_to_broker_mapping
}

impl BrokerStats {
    pub fn new(registry: Registry) -> Self {
        let state_age = register_int_gauge_vec_with_registry!(
            "rdkafka_broker_state_age",
            "Age of the broker state in seconds",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let outbuf_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka_broker_outbuf_cnt",
            "Number of messages waiting to be sent to broker",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let outbuf_msg_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka_broker_outbuf_msg_cnt",
            "Number of messages waiting to be sent to broker",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let waitresp_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka_broker_waitresp_cnt",
            "Number of requests waiting for response",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let waitresp_msg_cnt = register_int_gauge_vec_with_registry!(
            "rdkafka_broker_waitresp_msg_cnt",
            "Number of messages waiting for response",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let tx = register_uint_gauge_vec_with_registry!(
            "rdkafka_broker_tx",
            "Number of transmitted messages",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let tx_bytes = register_uint_gauge_vec_with_registry!(
            "rdkafka_broker_tx_bytes",
            "Number of transmitted bytes",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let tx_errs = register_uint_gauge_vec_with_registry!(
            "rdkafka_broker_tx_errs",
            "Number of failed transmitted messages",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let tx_retries = register_uint_gauge_vec_with_registry!(
            "rdkafka_broker_tx_retries",
            "Number of message retries",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let tx_idle = register_int_gauge_vec_with_registry!(
            "rdkafka_broker_tx_idle",
            "Number of idle transmit connections",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let req_timeouts = register_uint_gauge_vec_with_registry!(
            "rdkafka_broker_req_timeouts",
            "Number of request timeouts",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let rx = register_uint_gauge_vec_with_registry!(
            "rdkafka_broker_rx",
            "Number of received messages",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let rx_bytes = register_uint_gauge_vec_with_registry!(
            "rdkafka_broker_rx_bytes",
            "Number of received bytes",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let rx_errs = register_uint_gauge_vec_with_registry!(
            "rdkafka_broker_rx_errs",
            "Number of failed received messages",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let rx_corriderrs = register_uint_gauge_vec_with_registry!(
            "rdkafka_broker_rx_corriderrs",
            "Number of received messages with invalid correlation id",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let rx_partial = register_uint_gauge_vec_with_registry!(
            "rdkafka_broker_rx_partial",
            "Number of partial messages received",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let rx_idle = register_int_gauge_vec_with_registry!(
            "rdkafka_broker_rx_idle",
            "Number of idle receive connections",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let req = register_int_gauge_vec_with_registry!(
            "rdkafka_broker_req",
            "Number of requests in flight",
            &["id", "client_id", "broker", "state", "type"],
            registry
        )
        .unwrap();
        let zbuf_grow = register_uint_gauge_vec_with_registry!(
            "rdkafka_broker_zbuf_grow",
            "Number of times the broker's output buffer has been reallocated",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let buf_grow = register_uint_gauge_vec_with_registry!(
            "rdkafka_broker_buf_grow",
            "Number of times the broker's input buffer has been reallocated",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let wakeups = register_uint_gauge_vec_with_registry!(
            "rdkafka_broker_wakeups",
            "Number of wakeups",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let connects = register_int_gauge_vec_with_registry!(
            "rdkafka_broker_connects",
            "Number of connection attempts",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let disconnects = register_int_gauge_vec_with_registry!(
            "rdkafka_broker_disconnects",
            "Number of disconnects",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let int_latency = StatsWindow::new(registry.clone(), "broker_intlatency");
        let outbuf_latency = StatsWindow::new(registry.clone(), "broker_outbuflatency");
        let rtt = StatsWindow::new(registry.clone(), "broker_rtt");
        let throttle = StatsWindow::new(registry.clone(), "broker_throttle");

        BrokerStats {
            registry,
            state_age,
            outbuf_cnt,
            outbuf_msg_cnt,
            waitresp_cnt,
            waitresp_msg_cnt,
            tx,
            tx_bytes,
            tx_errs,
            tx_retries,
            tx_idle,
            req_timeouts,
            rx,
            rx_bytes,
            rx_errs,
            rx_corriderrs,
            rx_partial,
            rx_idle,
            req,
            zbuf_grow,
            buf_grow,
            wakeups,
            connects,
            disconnects,
            int_latency,
            outbuf_latency,
            rtt,
            throttle,
        }
    }

    pub fn report(&self, id: &str, client_id: &str, stats: &Statistics) {
        for broker_stats in stats.brokers.values() {
            self.report_inner(id, client_id, broker_stats);
        }
    }

    fn report_inner(&self, id: &str, client_id: &str, stats: &Broker) {
        let broker = stats.nodename.as_str();
        let state = stats.state.as_str();
        let labels = [id, client_id, broker, state];

        self.state_age
            .with_label_values(&labels)
            .set(stats.stateage);
        self.outbuf_cnt
            .with_label_values(&labels)
            .set(stats.outbuf_cnt);
        self.outbuf_msg_cnt
            .with_label_values(&labels)
            .set(stats.outbuf_msg_cnt);
        self.waitresp_cnt
            .with_label_values(&labels)
            .set(stats.waitresp_cnt);
        self.waitresp_msg_cnt
            .with_label_values(&labels)
            .set(stats.waitresp_msg_cnt);
        self.tx.with_label_values(&labels).set(stats.tx);
        self.tx_bytes.with_label_values(&labels).set(stats.txbytes);
        self.tx_errs.with_label_values(&labels).set(stats.txerrs);
        self.tx_retries
            .with_label_values(&labels)
            .set(stats.txretries);
        self.tx_idle.with_label_values(&labels).set(stats.txidle);
        self.req_timeouts
            .with_label_values(&labels)
            .set(stats.req_timeouts);
        self.rx.with_label_values(&labels).set(stats.rx);
        self.rx_bytes.with_label_values(&labels).set(stats.rxbytes);
        self.rx_errs.with_label_values(&labels).set(stats.rxerrs);
        self.rx_corriderrs
            .with_label_values(&labels)
            .set(stats.rxcorriderrs);
        self.rx_partial
            .with_label_values(&labels)
            .set(stats.rxpartial);
        self.rx_idle.with_label_values(&labels).set(stats.rxidle);
        for (req_type, req_cnt) in &stats.req {
            self.req
                .with_label_values(&[id, client_id, broker, state, req_type])
                .set(*req_cnt);
        }
        self.zbuf_grow
            .with_label_values(&labels)
            .set(stats.zbuf_grow);
        self.buf_grow.with_label_values(&labels).set(stats.buf_grow);
        if let Some(wakeups) = stats.wakeups {
            self.wakeups.with_label_values(&labels).set(wakeups);
        }
        if let Some(connects) = stats.connects {
            self.connects.with_label_values(&labels).set(connects);
        }
        if let Some(disconnects) = stats.disconnects {
            self.disconnects.with_label_values(&labels).set(disconnects);
        }
        if let Some(int_latency) = &stats.int_latency {
            self.int_latency
                .report(id, client_id, broker, "", int_latency);
        }
        if let Some(outbuf_latency) = &stats.outbuf_latency {
            self.outbuf_latency
                .report(id, client_id, broker, "", outbuf_latency);
        }
        if let Some(rtt) = &stats.rtt {
            self.rtt.report(id, client_id, broker, "", rtt);
        }
        if let Some(throttle) = &stats.throttle {
            self.throttle.report(id, client_id, broker, "", throttle);
        }
    }
}
