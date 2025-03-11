from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Kafka Metrics",
            [
                panels.timeseries_count(
                    "Kafka high watermark and source latest message",
                    "Kafka high watermark by source and partition and source latest message by partition, source and actor",
                    [
                        panels.target(
                            f"{metric('source_kafka_high_watermark')}",
                            "high watermark: source={{source_id}} partition={{partition}}",
                        ),
                        panels.target(
                            f"{metric('source_latest_message_id')}",
                            "latest msg: source={{source_id}} partition={{partition}} actor_id={{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Message Count in Producer Queue",
                    "Current number of messages in producer queues",
                    [
                        panels.target(
                            f"{metric('rdkafka_top_msg_cnt')}",
                            "id {{ id }}, client_id {{ client_id }}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Message Size in Producer Queue",
                    "Current total size of messages in producer queues",
                    [
                        panels.target(
                            f"{metric('rdkafka_top_msg_size')}",
                            "id {{ id }}, client_id {{ client_id }}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Message Produced Count",
                    "Total number of messages transmitted (produced) to Kafka brokers",
                    [
                        panels.target(
                            f"{metric('rdkafka_top_tx_msgs')}",
                            "id {{ id }}, client_id {{ client_id }}",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Message Received Count",
                    "Total number of messages consumed, not including ignored messages (due to offset, etc), from Kafka brokers.",
                    [
                        panels.target(
                            f"{metric('rdkafka_top_rx_msgs')}",
                            "id {{ id }}, client_id {{ client_id }}",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Message Count Pending to Transmit (per broker)",
                    "Number of messages awaiting transmission to broker",
                    [
                        panels.target(
                            f"{metric('rdkafka_broker_outbuf_msg_cnt')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, state {{ state }}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Inflight Message Count (per broker)",
                    "Number of messages in-flight to broker awaiting response",
                    [
                        panels.target(
                            f"{metric('rdkafka_broker_waitresp_msg_cnt')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, state {{ state }}",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Error Count When Transmitting (per broker)",
                    "Total number of transmission errors",
                    [
                        panels.target(
                            f"{metric('rdkafka_broker_tx_errs')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, state {{ state }}",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Error Count When Receiving (per broker)",
                    "Total number of receive errors",
                    [
                        panels.target(
                            f"{metric('rdkafka_broker_rx_errs')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, state {{ state }}",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Timeout Request Count (per broker)",
                    "Total number of requests timed out",
                    [
                        panels.target(
                            f"{metric('rdkafka_broker_req_timeouts')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, state {{ state }}",
                        )
                    ],
                ),
                panels.timeseries_latency_ms(
                    "RTT (per broker)",
                    "Broker latency / round-trip time in milli seconds",
                    [
                        panels.target(
                            f"{metric('rdkafka_broker_rtt_avg')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_rtt_p75')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_rtt_p90')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_rtt_p99')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_rtt_p99_99')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_rtt_out_of_range')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Throttle Time (per broker)",
                    "Broker throttling time in milliseconds",
                    [
                        panels.target(
                            f"{metric('rdkafka_broker_throttle_avg')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_throttle_p75')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_throttle_p90')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_throttle_p99')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_throttle_p99_99')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_throttle_out_of_range')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Topic Metadata_age Age",
                    "Age of metadata from broker for this topic (milliseconds)",
                    [
                        panels.target(
                            f"{metric('rdkafka_topic_metadata_age')}",
                            "id {{ id }}, client_id {{ client_id}}, topic {{ topic }}",
                        )
                    ],
                ),
                panels.timeseries_bytes(
                    "Topic Batch Size",
                    "Batch sizes in bytes",
                    [
                        panels.target(
                            f"{metric('rdkafka_topic_batchsize_avg')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_topic_batchsize_p75')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_topic_batchsize_p90')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_topic_batchsize_p99')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_topic_batchsize_p99_99')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_topic_batchsize_out_of_range')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                        ),
                        panels.timeseries_count(
                            "Topic Batch Messages",
                            "Batch message counts",
                            [
                                panels.target(
                                    f"{metric('rdkafka_topic_batchcnt_avg')}",
                                    "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                                ),
                                panels.target(
                                    f"{metric('rdkafka_topic_batchcnt_p75')}",
                                    "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                                ),
                                panels.target(
                                    f"{metric('rdkafka_topic_batchcnt_p90')}",
                                    "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                                ),
                                panels.target(
                                    f"{metric('rdkafka_topic_batchcnt_p99')}",
                                    "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                                ),
                                panels.target(
                                    f"{metric('rdkafka_topic_batchcnt_p99_99')}",
                                    "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                                ),
                                panels.target(
                                    f"{metric('rdkafka_topic_batchcnt_out_of_range')}",
                                    "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                                ),
                            ],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Message to be Transmitted",
                    "Number of messages ready to be produced in transmit queue",
                    [
                        panels.target(
                            f"{metric('rdkafka_topic_partition_xmit_msgq_cnt')}",
                            "id {{ id }}, client_id {{ client_id}}, topic {{ topic }}, partition {{ partition }}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Message in pre fetch queue",
                    "Number of pre-fetched messages in fetch queue",
                    [
                        panels.target(
                            f"{metric('rdkafka_topic_partition_fetchq_cnt')}",
                            "id {{ id }}, client_id {{ client_id}}, topic {{ topic }}, partition {{ partition }}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Next offset to fetch",
                    "Next offset to fetch",
                    [
                        panels.target(
                            f"{metric('rdkafka_topic_partition_next_offset')}",
                            "id {{ id }}, client_id {{ client_id}}, topic {{ topic }}, partition {{ partition }}",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Committed Offset",
                    "Last committed offset",
                    [
                        panels.target(
                            f"{metric('rdkafka_topic_partition_committed_offset')}",
                            "id {{ id }}, client_id {{ client_id}}, topic {{ topic }}, partition {{ partition }}",
                        )
                    ],
                ),
            ],
        )
    ]
