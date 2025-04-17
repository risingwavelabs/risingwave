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

use prometheus::Registry;
use rdkafka::Statistics;
use rdkafka::statistics::{Broker, ConsumerGroup, Partition, Topic, Window};
use risingwave_common::metrics::{LabelGuardedIntGaugeVec, LabelGuardedUintGaugeVec};
use risingwave_common::{
    register_guarded_int_gauge_vec_with_registry, register_guarded_uint_gauge_vec_with_registry,
};

#[derive(Debug, Clone)]
pub struct RdKafkaStats {
    pub registry: Registry,

    pub ts: LabelGuardedIntGaugeVec,
    pub time: LabelGuardedIntGaugeVec,
    pub age: LabelGuardedIntGaugeVec,
    pub replyq: LabelGuardedIntGaugeVec,
    pub msg_cnt: LabelGuardedUintGaugeVec,
    pub msg_size: LabelGuardedUintGaugeVec,
    pub msg_max: LabelGuardedUintGaugeVec,
    pub msg_size_max: LabelGuardedUintGaugeVec,
    pub tx: LabelGuardedIntGaugeVec,
    pub tx_bytes: LabelGuardedIntGaugeVec,
    pub rx: LabelGuardedIntGaugeVec,
    pub rx_bytes: LabelGuardedIntGaugeVec,
    pub tx_msgs: LabelGuardedIntGaugeVec,
    pub tx_msgs_bytes: LabelGuardedIntGaugeVec,
    pub rx_msgs: LabelGuardedIntGaugeVec,
    pub rx_msgs_bytes: LabelGuardedIntGaugeVec,
    pub simple_cnt: LabelGuardedIntGaugeVec,
    pub metadata_cache_cnt: LabelGuardedIntGaugeVec,

    pub broker_stats: BrokerStats,
    pub topic_stats: TopicStats,
    pub cgrp: ConsumerGroupStats,
}

#[derive(Debug, Clone)]
pub struct BrokerStats {
    pub registry: Registry,

    pub state_age: LabelGuardedIntGaugeVec,
    pub outbuf_cnt: LabelGuardedIntGaugeVec,
    pub outbuf_msg_cnt: LabelGuardedIntGaugeVec,
    pub waitresp_cnt: LabelGuardedIntGaugeVec,
    pub waitresp_msg_cnt: LabelGuardedIntGaugeVec,
    pub tx: LabelGuardedUintGaugeVec,
    pub tx_bytes: LabelGuardedUintGaugeVec,
    pub tx_errs: LabelGuardedUintGaugeVec,
    pub tx_retries: LabelGuardedUintGaugeVec,
    pub tx_idle: LabelGuardedIntGaugeVec,
    pub req_timeouts: LabelGuardedUintGaugeVec,
    pub rx: LabelGuardedUintGaugeVec,
    pub rx_bytes: LabelGuardedUintGaugeVec,
    pub rx_errs: LabelGuardedUintGaugeVec,
    pub rx_corriderrs: LabelGuardedUintGaugeVec,
    pub rx_partial: LabelGuardedUintGaugeVec,
    pub rx_idle: LabelGuardedIntGaugeVec,
    pub req: LabelGuardedIntGaugeVec,
    pub zbuf_grow: LabelGuardedUintGaugeVec,
    pub buf_grow: LabelGuardedUintGaugeVec,
    pub wakeups: LabelGuardedUintGaugeVec,
    pub connects: LabelGuardedIntGaugeVec,
    pub disconnects: LabelGuardedIntGaugeVec,
    pub int_latency: StatsWindow,
    pub outbuf_latency: StatsWindow,
    pub rtt: StatsWindow,
    pub throttle: StatsWindow,
}

#[derive(Debug, Clone)]
pub struct TopicStats {
    pub registry: Registry,

    pub metadata_age: LabelGuardedIntGaugeVec,
    pub batch_size: StatsWindow,
    pub batch_cnt: StatsWindow,
    pub partitions: PartitionStats,
}

#[derive(Debug, Clone)]
pub struct StatsWindow {
    pub registry: Registry,

    pub min: LabelGuardedIntGaugeVec,
    pub max: LabelGuardedIntGaugeVec,
    pub avg: LabelGuardedIntGaugeVec,
    pub sum: LabelGuardedIntGaugeVec,
    pub cnt: LabelGuardedIntGaugeVec,
    pub stddev: LabelGuardedIntGaugeVec,
    pub hdr_size: LabelGuardedIntGaugeVec,
    pub p50: LabelGuardedIntGaugeVec,
    pub p75: LabelGuardedIntGaugeVec,
    pub p90: LabelGuardedIntGaugeVec,
    pub p95: LabelGuardedIntGaugeVec,
    pub p99: LabelGuardedIntGaugeVec,
    pub p99_99: LabelGuardedIntGaugeVec,
    pub out_of_range: LabelGuardedIntGaugeVec,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroupStats {
    pub registry: Registry,

    pub state_age: LabelGuardedIntGaugeVec,
    // todo: (do not know value set) join_state: IntGaugeVec,
    pub rebalance_age: LabelGuardedIntGaugeVec,
    pub rebalance_cnt: LabelGuardedIntGaugeVec,
    // todo: (cannot handle string) rebalance_reason,
    pub assignment_size: LabelGuardedIntGaugeVec,
}

impl ConsumerGroupStats {
    pub fn new(registry: Registry) -> Self {
        let state_age = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_consumer_group_state_age",
            "Age of the consumer group state in seconds",
            &["id", "client_id", "state"],
            registry
        )
        .unwrap();
        let rebalance_age = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_consumer_group_rebalance_age",
            "Age of the last rebalance in seconds",
            &["id", "client_id", "state"],
            registry
        )
        .unwrap();
        let rebalance_cnt = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_consumer_group_rebalance_cnt",
            "Number of rebalances",
            &["id", "client_id", "state"],
            registry
        )
        .unwrap();
        let assignment_size = register_guarded_int_gauge_vec_with_registry!(
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
            .with_guarded_label_values(&[id, client_id, state])
            .set(stats.stateage);
        self.rebalance_age
            .with_guarded_label_values(&[id, client_id, state])
            .set(stats.rebalance_age);
        self.rebalance_cnt
            .with_guarded_label_values(&[id, client_id, state])
            .set(stats.rebalance_cnt);
        self.assignment_size
            .with_guarded_label_values(&[id, client_id, state])
            .set(stats.assignment_size as i64);
    }
}

impl StatsWindow {
    pub fn new(registry: Registry, path: &str) -> Self {
        let get_metric_name = |name: &str| format!("rdkafka_{}_{}", path, name);
        let min = register_guarded_int_gauge_vec_with_registry!(
            get_metric_name("min"),
            "Minimum value",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let max = register_guarded_int_gauge_vec_with_registry!(
            get_metric_name("max"),
            "Maximum value",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let avg = register_guarded_int_gauge_vec_with_registry!(
            get_metric_name("avg"),
            "Average value",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let sum = register_guarded_int_gauge_vec_with_registry!(
            get_metric_name("sum"),
            "Sum of values",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let cnt = register_guarded_int_gauge_vec_with_registry!(
            get_metric_name("cnt"),
            "Count of values",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let stddev = register_guarded_int_gauge_vec_with_registry!(
            get_metric_name("stddev"),
            "Standard deviation",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let hdr_size = register_guarded_int_gauge_vec_with_registry!(
            get_metric_name("hdrsize"),
            "Size of the histogram header",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let p50 = register_guarded_int_gauge_vec_with_registry!(
            get_metric_name("p50"),
            "50th percentile",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let p75 = register_guarded_int_gauge_vec_with_registry!(
            get_metric_name("p75"),
            "75th percentile",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let p90 = register_guarded_int_gauge_vec_with_registry!(
            get_metric_name("p90"),
            "90th percentile",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let p95 = register_guarded_int_gauge_vec_with_registry!(
            get_metric_name("p95"),
            "95th percentile",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let p99 = register_guarded_int_gauge_vec_with_registry!(
            get_metric_name("p99"),
            "99th percentile",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let p99_99 = register_guarded_int_gauge_vec_with_registry!(
            get_metric_name("p99_99"),
            "99.99th percentile",
            &["id", "client_id", "broker", "topic"],
            registry
        )
        .unwrap();
        let out_of_range = register_guarded_int_gauge_vec_with_registry!(
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

        self.min.with_guarded_label_values(&labels).set(stats.min);
        self.max.with_guarded_label_values(&labels).set(stats.max);
        self.avg.with_guarded_label_values(&labels).set(stats.avg);
        self.sum.with_guarded_label_values(&labels).set(stats.sum);
        self.cnt.with_guarded_label_values(&labels).set(stats.cnt);
        self.stddev
            .with_guarded_label_values(&labels)
            .set(stats.stddev);
        self.hdr_size
            .with_guarded_label_values(&labels)
            .set(stats.hdrsize);
        self.p50.with_guarded_label_values(&labels).set(stats.p50);
        self.p75.with_guarded_label_values(&labels).set(stats.p75);
        self.p90.with_guarded_label_values(&labels).set(stats.p90);
        self.p99_99
            .with_guarded_label_values(&labels)
            .set(stats.p99_99);
        self.out_of_range
            .with_guarded_label_values(&labels)
            .set(stats.outofrange);
    }
}

impl TopicStats {
    pub fn new(registry: Registry) -> Self {
        let metadata_age = register_guarded_int_gauge_vec_with_registry!(
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

    pub fn report(&self, id: &str, client_id: &str, stats: &Statistics) {
        for (topic, topic_stats) in &stats.topics {
            self.report_inner(id, client_id, topic, topic_stats);
        }
    }

    fn report_inner(&self, id: &str, client_id: &str, topic: &str, stats: &Topic) {
        self.metadata_age
            .with_guarded_label_values(&[id, client_id, topic])
            .set(stats.metadata_age);
        self.batch_size
            .report(id, client_id, "", topic, &stats.batchsize);
        self.batch_cnt
            .report(id, client_id, "", topic, &stats.batchcnt);
        self.partitions.report(id, client_id, topic, stats)
    }
}

#[derive(Debug, Clone)]
pub struct PartitionStats {
    pub registry: Registry,

    pub msgq_cnt: LabelGuardedIntGaugeVec,
    pub msgq_bytes: LabelGuardedUintGaugeVec,
    pub xmit_msgq_cnt: LabelGuardedIntGaugeVec,
    pub xmit_msgq_bytes: LabelGuardedUintGaugeVec,
    pub fetchq_cnt: LabelGuardedIntGaugeVec,
    pub fetchq_size: LabelGuardedUintGaugeVec,
    pub query_offset: LabelGuardedIntGaugeVec,
    pub next_offset: LabelGuardedIntGaugeVec,
    pub app_offset: LabelGuardedIntGaugeVec,
    pub stored_offset: LabelGuardedIntGaugeVec,
    pub committed_offset: LabelGuardedIntGaugeVec,
    pub eof_offset: LabelGuardedIntGaugeVec,
    pub lo_offset: LabelGuardedIntGaugeVec,
    pub hi_offset: LabelGuardedIntGaugeVec,
    pub consumer_lag: LabelGuardedIntGaugeVec,
    pub consumer_lag_store: LabelGuardedIntGaugeVec,
    pub txmsgs: LabelGuardedUintGaugeVec,
    pub txbytes: LabelGuardedUintGaugeVec,
    pub rxmsgs: LabelGuardedUintGaugeVec,
    pub rxbytes: LabelGuardedUintGaugeVec,
    pub msgs: LabelGuardedUintGaugeVec,
    pub rx_ver_drops: LabelGuardedUintGaugeVec,
    pub msgs_inflight: LabelGuardedIntGaugeVec,
    pub next_ack_seq: LabelGuardedIntGaugeVec,
    pub next_err_seq: LabelGuardedIntGaugeVec,
    pub acked_msgid: LabelGuardedUintGaugeVec,
}

impl PartitionStats {
    pub fn new(registry: Registry) -> Self {
        let msgq_cnt = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_msgq_cnt",
            "Number of messages in the producer queue",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let msgq_bytes = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_msgq_bytes",
            "Size of messages in the producer queue",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let xmit_msgq_cnt = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_xmit_msgq_cnt",
            "Number of messages in the transmit queue",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let xmit_msgq_bytes = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_xmit_msgq_bytes",
            "Size of messages in the transmit queue",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let fetchq_cnt = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_fetchq_cnt",
            "Number of messages in the fetch queue",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let fetchq_size = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_fetchq_size",
            "Size of messages in the fetch queue",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let query_offset = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_query_offset",
            "Current query offset",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let next_offset = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_next_offset",
            "Next offset to query",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let app_offset = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_app_offset",
            "Last acknowledged offset",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let stored_offset = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_stored_offset",
            "Last stored offset",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let committed_offset = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_committed_offset",
            "Last committed offset",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let eof_offset = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_eof_offset",
            "Last offset in broker log",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let lo_offset = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_lo_offset",
            "Low offset",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let hi_offset = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_hi_offset",
            "High offset",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let consumer_lag = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_consumer_lag",
            "Consumer lag",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let consumer_lag_store = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_consumer_lag_store",
            "Consumer lag stored",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let txmsgs = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_txmsgs",
            "Number of transmitted messages",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let txbytes = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_txbytes",
            "Number of transmitted bytes",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let rxmsgs = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_rxmsgs",
            "Number of received messages",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let rxbytes = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_rxbytes",
            "Number of received bytes",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let msgs = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_msgs",
            "Number of messages in partition",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let rx_ver_drops = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_rx_ver_drops",
            "Number of received messages dropped due to version mismatch",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let msgs_inflight = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_msgs_inflight",
            "Number of messages in-flight",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let next_ack_seq = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_next_ack_seq",
            "Next ack sequence number",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let next_err_seq = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_topic_partition_next_err_seq",
            "Next error sequence number",
            &["id", "client_id", "topic", "partition"],
            registry
        )
        .unwrap();
        let acked_msgid = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_topic_partition_acked_msgid",
            "Acknowledged message ID",
            &["id", "client_id", "topic", "partition"],
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

    pub fn report(&self, id: &str, client_id: &str, topic: &str, stats: &Topic) {
        for partition_stats in stats.partitions.values() {
            self.report_inner(id, client_id, topic, partition_stats);
        }
    }

    fn report_inner(&self, id: &str, client_id: &str, topic: &str, stats: &Partition) {
        let labels = [id, client_id, topic, &stats.partition.to_string()];

        self.msgq_cnt
            .with_guarded_label_values(&labels)
            .set(stats.msgq_cnt);
        self.msgq_bytes
            .with_guarded_label_values(&labels)
            .set(stats.msgq_bytes);
        self.xmit_msgq_cnt
            .with_guarded_label_values(&labels)
            .set(stats.xmit_msgq_cnt);
        self.xmit_msgq_bytes
            .with_guarded_label_values(&labels)
            .set(stats.xmit_msgq_bytes);
        self.fetchq_cnt
            .with_guarded_label_values(&labels)
            .set(stats.fetchq_cnt);
        self.fetchq_size
            .with_guarded_label_values(&labels)
            .set(stats.fetchq_size);
        self.query_offset
            .with_guarded_label_values(&labels)
            .set(stats.query_offset);
        self.next_offset
            .with_guarded_label_values(&labels)
            .set(stats.next_offset);
        self.app_offset
            .with_guarded_label_values(&labels)
            .set(stats.app_offset);
        self.stored_offset
            .with_guarded_label_values(&labels)
            .set(stats.stored_offset);
        self.committed_offset
            .with_guarded_label_values(&labels)
            .set(stats.committed_offset);
        self.eof_offset
            .with_guarded_label_values(&labels)
            .set(stats.eof_offset);
        self.lo_offset
            .with_guarded_label_values(&labels)
            .set(stats.lo_offset);
        self.hi_offset
            .with_guarded_label_values(&labels)
            .set(stats.hi_offset);
        self.consumer_lag
            .with_guarded_label_values(&labels)
            .set(stats.consumer_lag);
        self.consumer_lag_store
            .with_guarded_label_values(&labels)
            .set(stats.consumer_lag_stored);
        self.txmsgs
            .with_guarded_label_values(&labels)
            .set(stats.txmsgs);
        self.txbytes
            .with_guarded_label_values(&labels)
            .set(stats.txbytes);
        self.rxmsgs
            .with_guarded_label_values(&labels)
            .set(stats.rxmsgs);
        self.rxbytes
            .with_guarded_label_values(&labels)
            .set(stats.rxbytes);
        self.msgs.with_guarded_label_values(&labels).set(stats.msgs);
        self.rx_ver_drops
            .with_guarded_label_values(&labels)
            .set(stats.rx_ver_drops);
        self.msgs_inflight
            .with_guarded_label_values(&labels)
            .set(stats.msgs_inflight);
        self.next_ack_seq
            .with_guarded_label_values(&labels)
            .set(stats.next_ack_seq);
        self.next_err_seq
            .with_guarded_label_values(&labels)
            .set(stats.next_err_seq);
        self.acked_msgid
            .with_guarded_label_values(&labels)
            .set(stats.acked_msgid);
    }
}

impl RdKafkaStats {
    pub fn new(registry: Registry) -> Self {
        let ts = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_top_ts",
            "librdkafka's internal monotonic clock (microseconds)",
            // we cannot tell whether it is for consumer or producer,
            // it may refer to source_id or sink_id
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let time = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_top_time",
            "Wall clock time in seconds since the epoch",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let age = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_top_age",
            "Age of the topic metadata in milliseconds",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let replyq = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_top_replyq",
            "Number of replies waiting to be served",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let msg_cnt = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_top_msg_cnt",
            "Number of messages in all topics",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let msg_size = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_top_msg_size",
            "Size of messages in all topics",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let msg_max = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_top_msg_max",
            "Maximum message size in all topics",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let msg_size_max = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_top_msg_size_max",
            "Maximum message size in all topics",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let tx = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_top_tx",
            "Number of transmitted messages",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let tx_bytes = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_top_tx_bytes",
            "Number of transmitted bytes",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let rx = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_top_rx",
            "Number of received messages",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let rx_bytes = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_top_rx_bytes",
            "Number of received bytes",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let tx_msgs = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_top_tx_msgs",
            "Number of transmitted messages",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let tx_msgs_bytes = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_top_tx_msgs_bytes",
            "Number of transmitted bytes",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let rx_msgs = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_top_rx_msgs",
            "Number of received messages",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let rx_msgs_bytes = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_top_rx_msgs_bytes",
            "Number of received bytes",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let simple_cnt = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_top_simple_cnt",
            "Number of simple consumer queues",
            &["id", "client_id"],
            registry
        )
        .unwrap();
        let metadata_cache_cnt = register_guarded_int_gauge_vec_with_registry!(
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
        let client_id = stats.name.as_str();
        self.ts
            .with_guarded_label_values(&[id, client_id])
            .set(stats.ts);
        self.time
            .with_guarded_label_values(&[id, client_id])
            .set(stats.time);
        self.age
            .with_guarded_label_values(&[id, client_id])
            .set(stats.age);
        self.replyq
            .with_guarded_label_values(&[id, client_id])
            .set(stats.replyq);
        self.msg_cnt
            .with_guarded_label_values(&[id, client_id])
            .set(stats.msg_cnt);
        self.msg_size
            .with_guarded_label_values(&[id, client_id])
            .set(stats.msg_size);
        self.msg_max
            .with_guarded_label_values(&[id, client_id])
            .set(stats.msg_max);
        self.msg_size_max
            .with_guarded_label_values(&[id, client_id])
            .set(stats.msg_size_max);
        self.tx
            .with_guarded_label_values(&[id, client_id])
            .set(stats.tx);
        self.tx_bytes
            .with_guarded_label_values(&[id, client_id])
            .set(stats.tx_bytes);
        self.rx
            .with_guarded_label_values(&[id, client_id])
            .set(stats.rx);
        self.rx_bytes
            .with_guarded_label_values(&[id, client_id])
            .set(stats.rx_bytes);
        self.tx_msgs
            .with_guarded_label_values(&[id, client_id])
            .set(stats.txmsgs);
        self.tx_msgs_bytes
            .with_guarded_label_values(&[id, client_id])
            .set(stats.txmsg_bytes);
        self.rx_msgs
            .with_guarded_label_values(&[id, client_id])
            .set(stats.rxmsgs);
        self.rx_msgs_bytes
            .with_guarded_label_values(&[id, client_id])
            .set(stats.rxmsg_bytes);
        self.simple_cnt
            .with_guarded_label_values(&[id, client_id])
            .set(stats.simple_cnt);
        self.metadata_cache_cnt
            .with_guarded_label_values(&[id, client_id])
            .set(stats.metadata_cache_cnt);

        self.broker_stats.report(id, client_id, stats);
        self.topic_stats.report(id, client_id, stats);
        if let Some(cgrp) = &stats.cgrp {
            self.cgrp.report(id, client_id, cgrp)
        }
    }
}

impl BrokerStats {
    pub fn new(registry: Registry) -> Self {
        let state_age = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_broker_state_age",
            "Age of the broker state in seconds",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let outbuf_cnt = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_broker_outbuf_cnt",
            "Number of messages waiting to be sent to broker",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let outbuf_msg_cnt = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_broker_outbuf_msg_cnt",
            "Number of messages waiting to be sent to broker",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let waitresp_cnt = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_broker_waitresp_cnt",
            "Number of requests waiting for response",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let waitresp_msg_cnt = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_broker_waitresp_msg_cnt",
            "Number of messages waiting for response",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let tx = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_broker_tx",
            "Number of transmitted messages",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let tx_bytes = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_broker_tx_bytes",
            "Number of transmitted bytes",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let tx_errs = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_broker_tx_errs",
            "Number of failed transmitted messages",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let tx_retries = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_broker_tx_retries",
            "Number of message retries",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let tx_idle = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_broker_tx_idle",
            "Number of idle transmit connections",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let req_timeouts = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_broker_req_timeouts",
            "Number of request timeouts",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let rx = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_broker_rx",
            "Number of received messages",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let rx_bytes = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_broker_rx_bytes",
            "Number of received bytes",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let rx_errs = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_broker_rx_errs",
            "Number of failed received messages",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let rx_corriderrs = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_broker_rx_corriderrs",
            "Number of received messages with invalid correlation id",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let rx_partial = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_broker_rx_partial",
            "Number of partial messages received",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let rx_idle = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_broker_rx_idle",
            "Number of idle receive connections",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let req = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_broker_req",
            "Number of requests in flight",
            &["id", "client_id", "broker", "state", "type"],
            registry
        )
        .unwrap();
        let zbuf_grow = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_broker_zbuf_grow",
            "Number of times the broker's output buffer has been reallocated",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let buf_grow = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_broker_buf_grow",
            "Number of times the broker's input buffer has been reallocated",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let wakeups = register_guarded_uint_gauge_vec_with_registry!(
            "rdkafka_broker_wakeups",
            "Number of wakeups",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let connects = register_guarded_int_gauge_vec_with_registry!(
            "rdkafka_broker_connects",
            "Number of connection attempts",
            &["id", "client_id", "broker", "state"],
            registry
        )
        .unwrap();
        let disconnects = register_guarded_int_gauge_vec_with_registry!(
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
            .with_guarded_label_values(&labels)
            .set(stats.stateage);
        self.outbuf_cnt
            .with_guarded_label_values(&labels)
            .set(stats.outbuf_cnt);
        self.outbuf_msg_cnt
            .with_guarded_label_values(&labels)
            .set(stats.outbuf_msg_cnt);
        self.waitresp_cnt
            .with_guarded_label_values(&labels)
            .set(stats.waitresp_cnt);
        self.waitresp_msg_cnt
            .with_guarded_label_values(&labels)
            .set(stats.waitresp_msg_cnt);
        self.tx.with_guarded_label_values(&labels).set(stats.tx);
        self.tx_bytes
            .with_guarded_label_values(&labels)
            .set(stats.txbytes);
        self.tx_errs
            .with_guarded_label_values(&labels)
            .set(stats.txerrs);
        self.tx_retries
            .with_guarded_label_values(&labels)
            .set(stats.txretries);
        self.tx_idle
            .with_guarded_label_values(&labels)
            .set(stats.txidle);
        self.req_timeouts
            .with_guarded_label_values(&labels)
            .set(stats.req_timeouts);
        self.rx.with_guarded_label_values(&labels).set(stats.rx);
        self.rx_bytes
            .with_guarded_label_values(&labels)
            .set(stats.rxbytes);
        self.rx_errs
            .with_guarded_label_values(&labels)
            .set(stats.rxerrs);
        self.rx_corriderrs
            .with_guarded_label_values(&labels)
            .set(stats.rxcorriderrs);
        self.rx_partial
            .with_guarded_label_values(&labels)
            .set(stats.rxpartial);
        self.rx_idle
            .with_guarded_label_values(&labels)
            .set(stats.rxidle);
        for (req_type, req_cnt) in &stats.req {
            self.req
                .with_guarded_label_values(&[id, client_id, broker, state, req_type])
                .set(*req_cnt);
        }
        self.zbuf_grow
            .with_guarded_label_values(&labels)
            .set(stats.zbuf_grow);
        self.buf_grow
            .with_guarded_label_values(&labels)
            .set(stats.buf_grow);
        if let Some(wakeups) = stats.wakeups {
            self.wakeups.with_guarded_label_values(&labels).set(wakeups);
        }
        if let Some(connects) = stats.connects {
            self.connects
                .with_guarded_label_values(&labels)
                .set(connects);
        }
        if let Some(disconnects) = stats.disconnects {
            self.disconnects
                .with_guarded_label_values(&labels)
                .set(disconnects);
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
