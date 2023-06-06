import os
import logging
import sys
p = os.path.dirname(__file__)
sys.path.append(p)
from common import *
from jsonmerge import merge

source_uid = os.environ.get(SOURCE_UID, "risedev-prometheus")
dashboard_uid = os.environ.get(DASHBOARD_UID, "Ecy3uV1nz")
dashboard_version = int(os.environ.get(DASHBOARD_VERSION, "0"))
datasource = {"type": "prometheus", "uid": f"{source_uid}"}

datasource_const = "datasource"
if dynamic_source_enabled:
    datasource = {"type": "prometheus", "uid": "${datasource}"}

panels = Panels(datasource)
logging.basicConfig(level=logging.WARN)


def section_actor_info(panels):
    excluded_cols = ['Time', 'Value', '__name__', 'job', 'instance']
    return [
        panels.row("Actor/Table Id Info"),
        panels.table_info("Actor Id Info",
                          "Mapping from actor id to fragment id",
                          [panels.table_target(f"{metric('actor_info')}")], excluded_cols),
        panels.table_info("Table Id Info",
                          "Mapping from table id to actor id and table name",
                          [panels.table_target(f"{metric('table_info')}")], excluded_cols),

    ]


def section_cluster_node(panels):
    return [
        panels.row("Cluster Node"),
        panels.timeseries_count(
            "Node Count",
            "The number of each type of RisingWave components alive.",
            [
                panels.target(f"sum({metric('worker_num')}) by (worker_type)",
                              "{{worker_type}}")
            ],
            ["last"],
        ),
        panels.timeseries_memory(
            "Node Memory",
            "The memory usage of each RisingWave component.",
            [
                panels.target(
                    f"avg({metric('process_resident_memory_bytes')}) by (job,instance)",
                    "{{job}} @ {{instance}}",
                )
            ],
        ),
        panels.timeseries_cpu(
            "Node CPU",
            "The CPU usage of each RisingWave component.",
            [
                panels.target(
                    f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by (job,instance)",
                    "cpu - {{job}} @ {{instance}}",
                ),

                panels.target(
                    f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by (job,instance) / avg({metric('process_cpu_core_num')}) by (job,instance)",
                    "cpu usage -{{job}} @ {{instance}}",
                ),
            ],
        ),

        panels.timeseries_count(
            "Meta Cluster",
            "RW cluster can configure multiple meta nodes to achieve high availability. One is the leader and the "
            "rest are the followers.",
            [
                panels.target(f"sum({metric('meta_num')}) by (worker_addr,role)",
                              "{{worker_addr}} @ {{role}}")
            ],
            ["last"],
        ),
    ]


def section_recovery_node(panels):
    return [
        panels.row("Recovery"),
        panels.timeseries_ops(
            "Recovery Successful Rate",
            "The rate of successful recovery attempts",
            [
                panels.target(f"sum(rate({metric('recovery_latency_count')}[$__rate_interval])) by (instance)",
                              "{{instance}}")
            ],
            ["last"],
        ),
        panels.timeseries_count(
            "Failed recovery attempts",
            "Total number of failed reocovery attempts",
            [
                panels.target(f"sum({metric('recovery_failure_cnt')}) by (instance)",
                              "{{instance}}")
            ],
            ["last"],
        ),
        panels.timeseries_latency(
            "Recovery latency",
            "Time spent in a successful recovery attempt",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({metric('recovery_latency_bucket')}[$__rate_interval])) by (le, instance))",
                        f"recovery latency p{legend}" +
                        " - {{instance}}",
                    ),
                    [50, 90, 99, "max"],
                ),
                panels.target(
                    f"sum by (le) (rate({metric('recovery_latency_sum')}[$__rate_interval])) / sum by (le) (rate({metric('recovery_latency_count')}[$__rate_interval]))",
                    "recovery latency avg",
                ),
            ],
            ["last"],
        )
    ]


def section_compaction(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Compaction",
            [
                panels.timeseries_count(
                    "SSTable Count",
                    "The number of SSTables at each level",
                    [
                        panels.target(
                            f"sum({metric('storage_level_sst_num')}) by (instance, level_index)",
                            "L{{level_index}}",
                        ),
                    ],
                ),
                panels.timeseries_kilobytes(
                    "SSTable Size(KB)",
                    "The size(KB) of SSTables at each level",
                    [
                        panels.target(
                            f"sum({metric('storage_level_total_file_size')}) by (instance, level_index)",
                            "L{{level_index}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compactor Core Count To Scale",
                    "The number of CPUs needed to meet the demand of compaction.",
                    [
                        panels.target(
                            f"sum({metric('storage_compactor_suggest_core_count')})",
                            "suggest-core-count"
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Success & Failure Count",
                    "The number of compactions from one level to another level that have completed or failed",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_frequency')}) by (compactor, group, task_type, result)",
                            "{{task_type}} - {{result}} - group-{{group}} @ {{compactor}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Skip Count",
                    "The number of compactions from one level to another level that have been skipped.",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_skip_compact_frequency')}[$__rate_interval])) by (level, type)",
                            "{{level}}-{{type}}",
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Compaction Task L0 Select Level Count",
                    "Avg l0 select_level_count of the compact task, and categorize it according to different cg, levels and task types",
                    [
                        panels.target(
                            f"sum by(le, group, type)(irate({metric('storage_l0_compact_level_count_sum')}[$__rate_interval]))  / sum by(le, group, type)(irate({metric('storage_l0_compact_level_count_count')}[$__rate_interval]))",
                            "avg cg{{group}}@{{type}}",
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Compaction Task File Count",
                    "Avg file count of the compact task, and categorize it according to different cg, levels and task types",
                    [
                        panels.target(
                            f"sum by(le, group, type)(irate({metric('storage_compact_task_file_count_sum')}[$__rate_interval]))  / sum by(le, group, type)(irate({metric('storage_compact_task_file_count_count')}[$__rate_interval]))",
                            "avg cg{{group}}@{{type}}",
                        ),
                    ],
                ),

                panels.timeseries_bytes(
                    "Compaction Task Size Distribution",
                    "The distribution of the compact task size triggered, including p90 and max. and categorize it according to different cg, levels and task types.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('storage_compact_task_size_bucket')}[$__rate_interval])) by (le, group, type))",
                                f"p{legend}" +
                                " - cg{{group}}@{{type}}",
                                ),
                            [90, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compactor Running Task Count",
                    "The number of compactions from one level to another level that are running.",
                    [
                        panels.target(
                            f"avg({metric('storage_compact_task_pending_num')}) by(job, instance)",
                            "compactor_task_split_count - {{job}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Compaction Duration",
                    "compact-task: The total time have been spent on compaction.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('compactor_compact_task_duration_bucket')}[$__rate_interval])) by (le, job, instance))",
                                f"compact-task p{legend}" +
                                " - {{job}} @ {{instance}}",
                            ),
                            [50, 90, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('compactor_compact_sst_duration_bucket')}[$__rate_interval])) by (le, job, instance))",
                                f"compact-key-range p{legend}" +
                                " - {{job}} @ {{instance}}",
                            ),
                            [90, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('compactor_get_table_id_total_time_duration_bucket')}[$__rate_interval])) by (le, job, instance))",
                                f"get-table-id p{legend}" +
                                " - {{job}} @ {{instance}}",
                            ),
                            [90, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('compactor_remote_read_time_per_task_bucket')}[$__rate_interval])) by (le, job, instance))",
                                f"remote-io p{legend}" +
                                " - {{job}} @ {{instance}}",
                            ),
                            [90, "max"],
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(rate({metric('compute_refill_cache_duration_bucket')}[$__rate_interval])) by (le, instance))",
                            "compute_apply_version_duration_p99 - {{instance}}",
                        ),
                        panels.target(
                            f"sum by(le)(rate({metric('compactor_compact_task_duration_sum')}[$__rate_interval])) / sum by(le)(rate({metric('compactor_compact_task_duration_count')}[$__rate_interval]))",
                            "compact-task avg",
                        ),
                        panels.target(
                            f"sum by(le)(rate({metric('state_store_compact_sst_duration_sum')}[$__rate_interval])) / sum by(le)(rate({metric('state_store_compact_sst_duration_count')}[$__rate_interval]))",
                            "compact-key-range avg",
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Compaction Throughput",
                    "KBs read from next level during history compactions to next level",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_read_next')}[$__rate_interval])) by(job,instance) + sum(rate("
                            f"{metric('storage_level_compact_read_curr')}[$__rate_interval])) by(job,instance)",
                            "read - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_write')}[$__rate_interval])) by(job,instance)",
                            "write - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('compactor_write_build_l0_bytes')}[$__rate_interval]))by (job,instance)",
                            "flush - {{job}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Compaction Write Bytes(GiB)",
                    "The number of bytes that have been written by compaction."
                    "Flush refers to the process of compacting Memtables to SSTables at Level 0."
                    "Write refers to the process of compacting SSTables at one level to another level.",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_write')}) by (job)",
                            "write - {{job}}",
                        ),
                        panels.target(
                            f"sum({metric('compactor_write_build_l0_bytes')}) by (job)",
                            "flush - {{job}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Compaction Write Amplification",
                    "Write amplification is the amount of bytes written to the remote storage by compaction for each "
                    "one byte of flushed SSTable data. Write amplification is by definition higher than 1.0 because "
                    "we write each piece of data to L0, and then write it again to an SSTable, and then compaction "
                    "may read this piece of data and write it to a new SSTable, that’s another write.",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_write')}) / sum({metric('compactor_write_build_l0_bytes')})",
                            "write amplification",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compacting SSTable Count",
                    "The number of SSTables that is being compacted at each level",
                    [
                        panels.target(f"{metric('storage_level_compact_cnt')}",
                                      "L{{level_index}}"),
                    ],
                ),
                panels.timeseries_count(
                    "Compacting Task Count",
                    "num of compact_task",
                    [
                        panels.target(f"{metric('storage_level_compact_task_cnt')}",
                                      "{{task}}"),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "KBs Read/Write by Level",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_read_next')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read from next level",
                        ),
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_read_curr')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read from current level",
                        ),
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_write')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} write to next level",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Count of SSTs Read/Write by level",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('storage_level_compact_write_sstn')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} write to next level",
                        ),
                        panels.target(
                            f"sum(irate({metric('storage_level_compact_read_sstn_next')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read from next level",
                        ),

                        panels.target(
                            f"sum(irate({metric('storage_level_compact_read_sstn_curr')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read from current level",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Hummock Sstable Size",
                    "Total bytes gotten from sstable_bloom_filter, for observing bloom_filter size",
                    [
                        panels.target(
                            f"sum by(le, job, instance)(rate({metric('compactor_sstable_bloom_filter_size_sum')}[$__rate_interval]))  / sum by(le, job, instance)(rate({metric('compactor_sstable_bloom_filter_size_count')}[$__rate_interval]))",
                            "avg_meta - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"sum by(le, job, instance)(rate({metric('compactor_sstable_file_size_sum')}[$__rate_interval]))  / sum by(le, job, instance)(rate({metric('compactor_sstable_file_size_count')}[$__rate_interval]))",
                            "avg_file - {{job}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Hummock Sstable Item Size",
                    "Total bytes gotten from sstable_avg_key_size, for observing sstable_avg_key_size",
                    [
                        panels.target(
                            f"sum by(le, job, instance)(rate({metric('compactor_sstable_avg_key_size_sum')}[$__rate_interval]))  / sum by(le, job, instance)(rate({metric('compactor_sstable_avg_key_size_count')}[$__rate_interval]))",
                            "avg_key_size - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"sum by(le, job, instance)(rate({metric('compactor_sstable_avg_value_size_sum')}[$__rate_interval]))  / sum by(le, job, instance)(rate({metric('compactor_sstable_avg_value_size_count')}[$__rate_interval]))",
                            "avg_value_size - {{job}} @ {{instance}}",
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Hummock Sstable Stat",
                    "Avg count gotten from sstable_distinct_epoch_count, for observing sstable_distinct_epoch_count",
                    [
                        panels.target(
                            f"sum by(le, job, instance)(rate({metric('compactor_sstable_distinct_epoch_count_sum')}[$__rate_interval]))  / sum by(le, job, instance)(rate({metric('compactor_sstable_distinct_epoch_count_count')}[$__rate_interval]))",
                            "avg_epoch_count - {{job}} @ {{instance}}",
                        ),
                    ],
                ),

                panels.timeseries_latency(
                    "Hummock Remote Read Duration",
                    "Total time of operations which read from remote storage when enable prefetch",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_remote_read_time_per_task_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                                f"remote-io p{legend}" +
                                " - {{table_id}} @ {{job}} @ {{instance}}",
                            ),
                            [90, "max"],
                        ),
                    ],
                ),

                panels.timeseries_ops(
                    "Compactor Iter keys",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('compactor_iter_scan_key_counts')}[$__rate_interval])) by (instance, type)",
                            "iter keys flow - {{type}} @ {{instance}} ",
                        ),
                    ],
                ),

                panels.timeseries_bytes(
                    "Lsm Compact Pending Bytes",
                    "bytes of Lsm tree needed to reach balance",
                    [
                        panels.target(
                            f"sum({metric('storage_compact_pending_bytes')}) by (instance, group)",
                            "compact pending bytes - {{group}} @ {{instance}} ",
                        ),
                    ],
                ),

                panels.timeseries_percentage(
                    "Lsm Level Compression Ratio",
                    "compression ratio of each level of the lsm tree",
                    [
                        panels.target(
                            f"sum({metric('storage_compact_level_compression_ratio')}) by (instance, group, level, algorithm)",
                            "lsm compression ratio - cg{{group}} @ L{{level}} - {{algorithm}} {{instance}} ",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_object_storage(outer_panels):
    panels = outer_panels.sub_panel()
    write_op_filter = "type=~'upload|delete'"
    read_op_filter = "type=~'read|readv|list|metadata'"
    request_cost_op1 = "type=~'read|streaming_read_start|delete'"
    request_cost_op2 = "type=~'upload|streaming_upload_start|s3_upload_part|streaming_upload_finish|delete_objects|list'"
    return [
        outer_panels.row_collapsed(
            "Object Storage",
            [
                panels.timeseries_bytes_per_sec(
                    "Operation Throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_read_bytes')}[$__rate_interval]))by(job,instance)",
                            "read - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_write_bytes')}[$__rate_interval]))by(job,instance)",
                            "write - {{job}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Operation Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('object_store_operation_latency_bucket')}[$__rate_interval])) by (le, type, job, instance))",
                                "{{type}}" + f" p{legend}" +
                                " - {{job}} @ {{instance}}",
                            ),
                            [50, 90, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, type, job, instance)(rate({metric('object_store_operation_latency_sum')}[$__rate_interval])) / sum by(le, type, job, instance) (rate({metric('object_store_operation_latency_count')}[$__rate_interval]))",
                            "{{type}} avg - {{job}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Operation Rate",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_operation_latency_count')}[$__rate_interval])) by (le, type, job, instance)",
                            "{{type}} - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_operation_latency_count', write_op_filter)}[$__rate_interval])) by (le, media_type, job, instance)",
                            "{{media_type}}-write - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_operation_latency_count', read_op_filter)}[$__rate_interval])) by (le, media_type, job, instance)",
                            "{{media_type}}-read - {{job}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Operation Size",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('object_store_operation_bytes_bucket')}[$__rate_interval])) by (le, type, job, instance))",
                            "{{type}}" + f" p{legend}" +
                            " - {{job}} @ {{instance}}",
                        ),
                        [50, 90, 99, "max"],
                    ),
                ),
                panels.timeseries_ops(
                    "Operation Failure Rate",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_failure_count')}[$__rate_interval])) by (instance, job, type)",
                            "{{type}} - {{job}} @ {{instance}}",
                        )
                    ],
                ),
                panels.timeseries_dollar(
                    "Estimated S3 Cost (Realtime)",
                    "There are two types of operations: 1. GET, SELECT, and DELETE, they cost 0.0004 USD per 1000 "
                    "requests. 2. PUT, COPY, POST, LIST, they cost 0.005 USD per 1000 requests."
                    "Reading from S3 across different regions impose extra cost. This metric assumes 0.01 USD per 1GB "
                    "data transfer. Please checkout AWS's pricing model for more accurate calculation.",
                    [
                        panels.target(
                            f"sum({metric('object_store_read_bytes')}) * 0.01 / 1000 / 1000 / 1000",
                            "(Cross Region) Data Transfer Cost",
                            True,
                        ),
                        panels.target(
                            f"sum({metric('object_store_operation_latency_count', request_cost_op1)}) * 0.0004 / 1000",
                            "GET, SELECT, and all other Requests Cost",
                        ),
                        panels.target(
                            f"sum({metric('object_store_operation_latency_count', request_cost_op2)}) * 0.005 / 1000",
                            "PUT, COPY, POST, LIST Requests Cost",
                        ),
                    ],
                ),
                panels.timeseries_dollar(
                    "Estimated S3 Cost (Monthly)",
                    "This metric uses the total size of data in S3 at this second to derive the cost of storing data "
                    "for a whole month. The price is 0.023 USD per GB. Please checkout AWS's pricing model for more "
                    "accurate calculation.",
                    [
                        panels.target(
                            f"sum({metric('storage_level_total_file_size')}) by (instance) * 0.023 / 1000 / 1000",
                            "Monthly Storage Cost",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_streaming(panels):
    mv_filter = "executor_identity=~\".*MaterializeExecutor.*\""
    sink_filter = "executor_identity=~\".*SinkExecutor.*\""
    return [
        panels.row("Streaming"),
        panels.timeseries_rowsps(
            "Source Throughput(rows/s)",
            "The figure shows the number of rows read by each source per second.",
            [
                panels.target(
                    f"rate({metric('stream_source_output_rows_counts')}[$__rate_interval])",
                    "source={{source_name}} actor={{actor_id}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_rowsps(
            "Source Throughput(rows/s) Per Partition",
            "Each query is executed in parallel with a user-defined parallelism. This figure shows the throughput of "
            "each parallelism. The throughput of all the parallelism added up is equal to Source Throughput(rows).",
            [
                panels.target(
                    f"rate({metric('partition_input_count')}[$__rate_interval])",
                    "actor={{actor_id}} source={{source_id}} partition={{partition}}",
                )
            ],
        ),
        panels.timeseries_bytesps(
            "Source Throughput(MB/s)",
            "The figure shows the number of bytes read by each source per second.",
            [
                panels.target(
                    f"(sum by (source_id)(rate({metric('partition_input_bytes')}[$__rate_interval])))/(1000*1000)",
                    "source={{source_id}}",
                )
            ],
        ),
        panels.timeseries_bytesps(
            "Source Throughput(MB/s) Per Partition",
            "Each query is executed in parallel with a user-defined parallelism. This figure shows the throughput of "
            "each parallelism. The throughput of all the parallelism added up is equal to Source Throughput(MB/s).",
            [
                panels.target(
                    f"(rate({metric('partition_input_bytes')}[$__rate_interval]))/(1000*1000)",
                    "actor={{actor_id}} source={{source_id}} partition={{partition}}",
                )
            ],
        ),
        panels.timeseries_rowsps(
            "Source Throughput(rows) per barrier",
            "RisingWave ingests barriers periodically to trigger computation and checkpoints. The frequency of "
            "barrier can be set by barrier_interval_ms. This metric shows how many rows are ingested between two "
            "consecutive barriers.",
            [
                panels.target(
                    f"rate({metric('stream_source_rows_per_barrier_counts')}[$__rate_interval])",
                    "actor={{actor_id}} source={{source_id}} @ {{instance}}"
                )
            ]
        ),
        panels.timeseries_count(
            "Source Upstream Status",
            "Monitor each source upstream, 0 means the upstream is not normal, 1 means the source is ready.",
            [
                panels.target(
                    f"{metric('source_status_is_up')}",
                    "source_id={{source_id}}, source_name={{source_name}} @ {{instance}}"
                )
            ]
        ),
        panels.timeseries_rowsps(
            "Sink Throughput(rows/s)",
            "The figure shows the number of rows output by each sink executor actor per second.",
            [
                panels.target(
                    f"rate({metric('stream_executor_row_count', filter=sink_filter)}[$__rate_interval])",
                    "sink={{executor_identity}} {{actor_id}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_rowsps(
            "Materialized View Throughput(rows/s)",
            "The figure shows the number of rows written into each materialized executor actor per second.",
            [
                panels.target(
                    f"rate({metric('stream_executor_row_count', filter=mv_filter)}[$__rate_interval])",
                    "MV={{executor_identity}} {{actor_id}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_rowsps(
            "Backfill Snapshot Read Throughput(rows)",
            "Total number of rows that have been read from the backfill snapshot",
            [
                panels.target(
                    f"rate({table_metric('stream_backfill_snapshot_read_row_count')}[$__rate_interval])",
                    "table_id={{table_id}} actor={{actor_id}} @ {{instance}}"
                ),
            ],
        ),
        panels.timeseries_rowsps(
            "Backfill Upstream Throughput(rows)",
            "Total number of rows that have been output from the backfill upstream",
            [
                panels.target(
                    f"rate({table_metric('stream_backfill_upstream_output_row_count')}[$__rate_interval])",
                    "table_id={{table_id}} actor={{actor_id}} @ {{instance}}"
                ),
            ],
        ),
        panels.timeseries_count(
            "Barrier Number",
            "The number of barriers that have been ingested but not completely processed. This metric reflects the "
            "current level of congestion within the system.",
            [
                panels.target(f"{metric('all_barrier_nums')}", "all_barrier"),
                panels.target(
                    f"{metric('in_flight_barrier_nums')}", "in_flight_barrier"),
            ],
        ),
        panels.timeseries_latency(
            "Barrier Send Latency",
            "The duration between the time point when the scheduled barrier needs to be sent and the time point when "
            "the barrier gets actually sent to all the compute nodes. Developers can thus detect any internal "
            "congestion.",
            quantile(
                lambda quantile, legend: panels.target(
                    f"histogram_quantile({quantile}, sum(rate({metric('meta_barrier_send_duration_seconds_bucket')}[$__rate_interval])) by (le))",
                    f"barrier_send_latency_p{legend}",
                ),
                [50, 90, 99, 999, "max"],
            ) + [
                panels.target(
                    f"rate({metric('meta_barrier_send_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_send_duration_seconds_count')}[$__rate_interval])",
                    "barrier_send_latency_avg",
                ),
            ],
        ),
        panels.timeseries_latency(
            "Barrier Latency",
            "The time that the data between two consecutive barriers gets fully processed, i.e. the computation "
            "results are made durable into materialized views or sink to external systems. This metric shows to users "
            "the freshness of materialized views.",
            quantile(
                lambda quantile, legend: panels.target(
                    f"histogram_quantile({quantile}, sum(rate({metric('meta_barrier_duration_seconds_bucket')}[$__rate_interval])) by (le))",
                    f"barrier_latency_p{legend}",
                ),
                [50, 90, 99, 999, "max"],
            ) + [
                panels.target(
                    f"rate({metric('meta_barrier_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_duration_seconds_count')}[$__rate_interval])",
                    "barrier_latency_avg",
                ),
            ],
        ),
        panels.timeseries_latency(
            "Barrier In-Flight Latency",
            "",
            quantile(
                lambda quantile, legend: panels.target(
                    f"histogram_quantile({quantile}, sum(rate({metric('stream_barrier_inflight_duration_seconds_bucket')}[$__rate_interval])) by (le))",
                    f"barrier_inflight_latency_p{legend}",
                ),
                [50, 90, 99, 999, "max"],
            ) + [
                panels.target(
                    f"max(sum by(le, instance)(rate({metric('stream_barrier_inflight_duration_seconds_sum')}[$__rate_interval]))  / sum by(le, instance)(rate({metric('stream_barrier_inflight_duration_seconds_count')}[$__rate_interval])))",
                    "barrier_inflight_latency_avg",
                ),
            ],
        ),
        panels.timeseries_latency(
            "Barrier Sync Latency",
            "",
            quantile(
                lambda quantile, legend: panels.target(
                    f"histogram_quantile({quantile}, sum(rate({metric('stream_barrier_sync_storage_duration_seconds_bucket')}[$__rate_interval])) by (le,instance))",
                    f"barrier_sync_latency_p{legend}" + " - {{instance}}",
                ),
                [50, 90, 99, 999, "max"],
            ) + [
                panels.target(
                    f"sum by(le, instance)(rate({metric('stream_barrier_sync_storage_duration_seconds_sum')}[$__rate_interval]))  / sum by(le, instance)(rate({metric('stream_barrier_sync_storage_duration_seconds_count')}[$__rate_interval]))",
                    "barrier_sync_latency_avg - {{instance}}",
                ),
            ],
        ),
        panels.timeseries_latency(
            "Barrier Wait Commit Latency",
            "",
            quantile(
                lambda quantile, legend: panels.target(
                    f"histogram_quantile({quantile}, sum(rate({metric('meta_barrier_wait_commit_duration_seconds_bucket')}[$__rate_interval])) by (le))",
                    f"barrier_wait_commit_latency_p{legend}",
                ),
                [50, 90, 99, 999, "max"],
            ) + [
                panels.target(
                    f"rate({metric('meta_barrier_wait_commit_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_wait_commit_duration_seconds_count')}[$__rate_interval])",
                    "barrier_wait_commit_avg",
                ),
            ],
        ),
    ]


def section_streaming_actors(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming Actors",
            [
                panels.timeseries_actor_rowsps(
                    "Executor Throughput",
                    "When enabled, this metric shows the input throughput of each executor.",
                    [
                        panels.target(
                            f"rate({metric('stream_executor_row_count')}[$__rate_interval]) > 0",
                            "{{actor_id}}->{{executor_identity}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Actor Backpressure",
                    "We first record the total blocking duration(ns) of output buffer of each actor. It shows how "
                    "much time it takes an actor to process a message, i.e. a barrier, a watermark or rows of data, "
                    "on average. Then we divide this duration by 1 second and show it as a percentage.",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_output_buffer_blocking_duration_ns')}[$__rate_interval]) / 1000000000",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Actor Memory Usage (TaskLocalAlloc)",
                    "",
                    [
                        panels.target(
                            "rate(actor_memory_usage[$__rate_interval])",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Executor Memory Usage",
                    "",
                    [
                        panels.target(
                            "rate(stream_memory_usage[$__rate_interval])",
                            "table {{table_id}} actor {{actor_id}} desc: {{desc}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Actor Input Blocking Time Ratio",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_input_buffer_blocking_duration_ns')}[$__rate_interval]) / 1000000000",
                            "{{actor_id}}->{{upstream_fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency(
                    "Actor Barrier Latency",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_barrier_time')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency(
                    "Actor Processing Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_processing_time')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency(
                    "Actor Execution Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_actor_execution_time')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_row(
                    "Actor Input Row",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_in_record_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_row(
                    "Actor Output Row",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_out_record_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Join Executor Cache",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_join_lookup_miss_count')}[$__rate_interval])",
                            "cache miss - {{side}} side, join_table_id {{join_table_id}} degree_table_id {{degree_table_id}} actor {{actor_id}} ",
                        ),
                        panels.target(
                            f"rate({metric('stream_join_lookup_total_count')}[$__rate_interval])",
                            "total lookups {{side}} side, join_table_id {{join_table_id}} degree_table_id {{degree_table_id}} actor {{actor_id}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_join_insert_cache_miss_count')}[$__rate_interval])",
                            "cache miss when insert {{side}} side, join_table_id {{join_table_id}} degree_table_id {{degree_table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),

                panels.timeseries_actor_ops(
                    "Temporal Join Executor Cache",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_temporal_join_cache_miss_count')}[$__rate_interval])",
                            "temporal join cache miss, table_id {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Materialize Executor Cache",
                    "",
                    [
                        panels.target(
                            f"rate({table_metric('stream_materialize_cache_hit_count')}[$__rate_interval])",
                            "cache hit count - table {{table_id}} - actor {{actor_id}}   {{instance}}",
                        ),
                        panels.target(
                            f"rate({table_metric('stream_materialize_cache_total_count')}[$__rate_interval])",
                            "total cached count - table {{table_id}} - actor {{actor_id}}   {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Executor Cache Miss Ratio",
                    "",
                    [
                        panels.target(
                            f"(sum(rate({metric('stream_join_lookup_miss_count')}[$__rate_interval])) by (side, join_table_id, degree_table_id, actor_id) ) / (sum(rate({metric('stream_join_lookup_total_count')}[$__rate_interval])) by (side, join_table_id, degree_table_id, actor_id))",
                            "join executor cache miss ratio - - {{side}} side, join_table_id {{join_table_id}} degree_table_id {{degree_table_id}} actor {{actor_id}}",
                        ),

                        panels.target(
                            f"(sum(rate({metric('stream_agg_lookup_miss_count')}[$__rate_interval])) by (table_id, actor_id) ) / (sum(rate({metric('stream_agg_lookup_total_count')}[$__rate_interval])) by (table_id, actor_id))",
                            "Agg cache miss ratio - table {{table_id}} actor {{actor_id}} ",
                        ),

                        panels.target(
                            f"(sum(rate({metric('stream_agg_distinct_cache_miss_count')}[$__rate_interval])) by (table_id, actor_id) ) / (sum(rate({metric('stream_agg_distinct_total_cache_count')}[$__rate_interval])) by (table_id, actor_id))",
                            "Distinct agg cache miss ratio - table {{table_id}} actor {{actor_id}} ",
                        ),

                        panels.target(
                            f"(sum(rate({metric('stream_group_top_n_cache_miss_count')}[$__rate_interval])) by (table_id, actor_id) ) / (sum(rate({metric('stream_group_top_n_total_query_cache_count')}[$__rate_interval])) by (table_id, actor_id))",
                            "Stream group top n cache miss ratio - table {{table_id}} actor {{actor_id}} ",
                        ),

                        panels.target(
                            f"(sum(rate({metric('stream_group_top_n_appendonly_cache_miss_count')}[$__rate_interval])) by (table_id, actor_id) ) / (sum(rate({metric('stream_group_top_n_appendonly_total_query_cache_count')}[$__rate_interval])) by (table_id, actor_id))",
                            "Stream group top n appendonly cache miss ratio - table {{table_id}} actor {{actor_id}} ",
                        ),

                        panels.target(
                            f"(sum(rate({metric('stream_lookup_cache_miss_count')}[$__rate_interval])) by (table_id, actor_id) ) / (sum(rate({metric('stream_lookup_total_query_cache_count')}[$__rate_interval])) by (table_id, actor_id))",
                            "Stream lookup cache miss ratio - table {{table_id}} actor {{actor_id}} ",
                        ),

                        panels.target(
                            f"(sum(rate({metric('stream_temporal_join_cache_miss_count')}[$__rate_interval])) by (table_id, actor_id) ) / (sum(rate({metric('stream_temporal_join_total_query_cache_count')}[$__rate_interval])) by (table_id, actor_id))",
                            "Stream temporal join cache miss ratio - table {{table_id}} actor {{actor_id}} ",
                        ),

                        panels.target(
                            f"1 - (sum(rate({metric('stream_materialize_cache_hit_count')}[$__rate_interval])) by (table_id, actor_id) ) / (sum(rate({metric('stream_materialize_cache_total_count')}[$__rate_interval])) by (table_id, actor_id))",
                            "materialize executor cache miss ratio - table {{table_id}} actor {{actor_id}}  {{instance}}",
                        ),

                    ],
                ),
                panels.timeseries_actor_latency(
                    "Join Executor Barrier Align",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('stream_join_barrier_align_duration_bucket')}[$__rate_interval])) by (le, actor_id, wait_side, job, instance))",
                                f"p{legend} {{{{actor_id}}}}.{{{{wait_side}}}} - {{{{job}}}} @ {{{{instance}}}}",
                            ),
                            [90, 99, 999, "max"],
                        ),
                        panels.target(
                            f"sum by(le, actor_id, wait_side, job, instance)(rate({metric('stream_join_barrier_align_duration_sum')}[$__rate_interval])) / sum by(le,actor_id,wait_side,job,instance) (rate({metric('stream_join_barrier_align_duration_count')}[$__rate_interval]))",
                            "avg {{actor_id}}.{{wait_side}} - {{job}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Join Actor Input Blocking Time Ratio",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_join_actor_input_waiting_duration_ns')}[$__rate_interval]) / 1000000000",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Join Actor Match Duration Per Second",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_join_match_duration_ns')}[$__rate_interval]) / 1000000000",
                            "{{actor_id}}.{{side}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Join Cached Entries",
                    "Multiple rows with distinct primary keys may have the same join key. This metric counts the "
                    "number of join keys in the executor cache.",
                    [
                        panels.target(f"{metric('stream_join_cached_entries')}",
                                      "{{actor_id}} {{side}}"),
                    ],
                ),
                panels.timeseries_count(
                    "Join Cached Rows",
                    "Multiple rows with distinct primary keys may have the same join key. This metric counts the "
                    "number of rows in the executor cache.",
                    [
                        panels.target(f"{metric('stream_join_cached_rows')}",
                                      "{{actor_id}} {{side}}"),
                    ],
                ),
                panels.timeseries_bytes(
                    "Join Cached Estimated Size",
                    "Multiple rows with distinct primary keys may have the same join key. This metric counts the "
                    "size of rows in the executor cache.",
                    [
                        panels.target(f"{metric('stream_join_cached_estimated_size')}",
                                      "{{actor_id}} {{side}}"),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Aggregation Executor Cache Statistics For Each Key/State",
                    "Lookup miss count counts the number of aggregation key's cache miss per second."
                    "Lookup total count counts the number of rows processed per second."
                    "By diving these two metrics, one can derive the cache miss rate per second.",
                    [
                        panels.target(
                            f"rate({metric('stream_agg_lookup_miss_count')}[$__rate_interval])",
                            "cache miss - table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_agg_distinct_cache_miss_count')}[$__rate_interval])",
                            "Distinct agg cache miss - table {{table_id}} actor {{actor_id}}",
                        ),

                        panels.target(
                            f"rate({metric('stream_group_top_n_cache_miss_count')}[$__rate_interval])",
                            "Group top n cache miss - table {{table_id}} actor {{actor_id}}",
                        ),

                        panels.target(
                            f"rate({metric('stream_group_top_n_appendonly_cache_miss_count')}[$__rate_interval])",
                            "Group top n appendonly cache miss - table {{table_id}} actor {{actor_id}}",
                        ),
           
                        panels.target(
                            f"rate({metric('stream_agg_lookup_total_count')}[$__rate_interval])",
                            "stream agg total lookups - table {{table_id}} actor {{actor_id}}",
                        ),

                        panels.target(
                            f"rate({metric('stream_lookup_cache_miss_count')}[$__rate_interval])",
                            "Lookup executor cache miss - table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Aggregation Executor Cache Statistics For Each StreamChunk",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_agg_chunk_lookup_miss_count')}[$__rate_interval])",
                            "chunk-level cache miss  - table {{table_id}} actor {{actor_id}}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_agg_chunk_lookup_total_count')}[$__rate_interval])",
                            "chunk-level total lookups  - table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Aggregation Cached Keys",
                    "The number of keys cached in each hash aggregation executor's executor cache.",
                    [
                        panels.target(f"{metric('stream_agg_cached_keys')}",
                                      "stream agg cached keys count | table {{table_id}} actor {{actor_id}}"),
                        panels.target(f"{metric('stream_agg_distinct_cached_keys')}",
                                      "stream agg distinct cached keys count |table {{table_id}} actor {{actor_id}}"),
                    ],
                ),
                panels.timeseries_count(
                    "TopN Cached Keys",
                    "The number of keys cached in each top_n executor's executor cache.",
                    [
                        panels.target(f"{metric('stream_group_top_n_cached_entry_count')}",
                                      "group top_n cached count | table {{table_id}} actor {{actor_id}}"),
                        panels.target(f"{metric('stream_group_top_n_appendonly_cached_entry_count')}",
                                      "group top_n appendonly cached count | table {{table_id}} actor {{actor_id}}"),
                    ],
                ),
                panels.timeseries_count(
                    "Temporal Join Cache Count",
                    "The number of keys cached in temporal join executor's executor cache.",
                    [
                        panels.target(f"{metric('stream_temporal_join_cached_entry_count')}",
                                      "Temporal Join cached count | table {{table_id}} actor {{actor_id}}"),
                       
                    ],
                ),

                panels.timeseries_count(
                    "Lookup Cached Keys",
                    "The number of keys cached in lookup executor's executor cache.",
                    [
                        panels.target(f"{metric('stream_lookup_cached_entry_count')}",
                                      "lookup cached count | table {{table_id}} actor {{actor_id}}"),
                       
                    ],
                ),
            ],
        )
    ]


def section_streaming_actors_tokio(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming Actors (Tokio)",
            [
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Fast Poll Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_fast_poll_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Fast Poll Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_fast_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Fast Poll Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_fast_poll_duration')}[$__rate_interval]) / rate({metric('stream_actor_fast_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Slow Poll Total Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_slow_poll_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Slow Poll Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_slow_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Slow Poll Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_slow_poll_duration')}[$__rate_interval]) / rate({metric('stream_actor_slow_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Poll Total Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_poll_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Poll Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Poll Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_poll_duration')}[$__rate_interval]) / rate({metric('stream_actor_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Idle Total Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_idle_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Idle Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_idle_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Idle Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_idle_duration')}[$__rate_interval]) / rate({metric('stream_actor_idle_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Scheduled Total Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_scheduled_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Scheduled Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_scheduled_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Scheduled Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_scheduled_duration')}[$__rate_interval]) / rate({metric('stream_actor_scheduled_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
            ]
        )
    ]


def section_streaming_exchange(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming Exchange",
            [
                panels.timeseries_bytes_per_sec(
                    "Fragment-level Remote Exchange Send Throughput",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_exchange_frag_send_size')}[$__rate_interval])",
                            "{{up_fragment_id}}->{{down_fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Fragment-level Remote Exchange Recv Throughput",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_exchange_frag_recv_size')}[$__rate_interval])",
                            "{{up_fragment_id}}->{{down_fragment_id}}",
                        ),
                    ],
                ),
            ],
        ),
    ]


def section_streaming_errors(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "User Streaming Errors",
            [
                panels.timeseries_count(
                    "Compute Errors by Type",
                    "",
                    [
                        panels.target(
                            f"sum({metric('user_compute_error_count')}) by (error_type, error_msg, fragment_id, executor_name)",
                            "{{error_type}}: {{error_msg}} ({{executor_name}}: fragment_id={{fragment_id}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Source Errors by Type",
                    "",
                    [
                        panels.target(
                            f"sum({metric('user_source_error_count')}) by (error_type, error_msg, fragment_id, table_id, executor_name)",
                            "{{error_type}}: {{error_msg}} ({{executor_name}}: table_id={{table_id}}, fragment_id={{fragment_id}})",
                        ),
                    ],
                ),
            ],
        ),
    ]


def section_batch(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Batch Metrics",
            [
                panels.timeseries_row(
                    "Exchange Recv Row Number",
                    "",
                    [
                        panels.target(
                            f"{metric('batch_exchange_recv_row_number')}",
                            "{{query_id}} : {{source_stage_id}}.{{source_task_id}} -> {{target_stage_id}}.{{target_task_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Batch Mpp Task Number",
                    "",
                    [
                        panels.target(
                            f"{metric('batch_task_num')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "Batch Mem Usage",
                    "All memory usage of batch executors in bytes",
                    [
                        panels.target(
                            f"{metric('batch_total_mem')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Batch Heartbeat Worker Number",
                    "",
                    [
                        panels.target(
                            f"{metric('batch_heartbeat_worker_num')}",
                            "",
                        ),
                    ],
                ),
            ],
        ),
    ]


def section_frontend(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Frontend",
            [
                panels.timeseries_query_per_sec(
                    "Query Per Second(Local Query Mode)",
                    "",
                    [
                        panels.target(
                            f"rate({metric('frontend_query_counter_local_execution')}[$__rate_interval])",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_query_per_sec(
                    "Query Per Second(Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"rate({metric('distributed_completed_query_counter')}[$__rate_interval])",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "The Number of Running Queries(Distributed Query Mode)",
                    "",
                    [
                        panels.target(f"{metric('distributed_running_query_num')}",
                                      "The number of running query in distributed execution mode"),
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "The Number of Rejected queries(Distributed Query Mode)",
                    "",
                    [
                        panels.target(f"{metric('distributed_rejected_query_counter')}",
                                      "The number of rejected query in distributed execution mode"),
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "The Number of Completed Queries(Distributed Query Mode)",
                    "",
                    [
                        panels.target(f"{metric('distributed_completed_query_counter')}",
                                      "The number of completed query in distributed execution mode"),
                    ],
                    ["last"],
                ),
                panels.timeseries_latency(
                    "Query Latency(Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.5, sum(rate({metric('distributed_query_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "p50 - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"histogram_quantile(0.9, sum(rate({metric('distributed_query_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "p90 - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"histogram_quantile(0.95, sum(rate({metric('distributed_query_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "p99 - {{job}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Query Latency(Local Query Mode)",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.5, sum(rate({metric('frontend_latency_local_execution_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "p50 - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"histogram_quantile(0.9, sum(rate({metric('frontend_latency_local_execution_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "p90 - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"histogram_quantile(0.95, sum(rate({metric('frontend_latency_local_execution_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "p99 - {{job}} @ {{instance}}",
                        ),
                    ],
                ),
            ],
        ),
    ]


def section_hummock(panels):
    meta_miss_filter = "type='meta_miss'"
    meta_total_filter = "type='meta_total'"
    data_miss_filter = "type='data_miss'"
    data_total_filter = "type='data_total'"
    file_cache_get_filter = "op='get'"
    return [
        panels.row("Hummock"),
        panels.timeseries_latency(
            "Build and Sync Sstable Duration",
            "Histogram of time spent on compacting shared buffer to remote storage.",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({metric('state_store_sync_duration_bucket')}[$__rate_interval])) by (le, job, instance))",
                        f"p{legend}" + " - {{job}} @ {{instance}}",
                    ),
                    [50, 90, 99, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance) (rate({metric('state_store_sync_duration_sum')}[$__rate_interval])) / sum by(le, job, instance) (rate({metric('state_store_sync_duration_count')}[$__rate_interval]))",
                    "avg - {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_ops(
            "Cache Ops",
            "",
            [
                panels.target(
                    f"sum(rate({table_metric('state_store_sst_store_block_request_counts')}[$__rate_interval])) by (job, instance, table_id, type)",
                    "{{table_id}} @ {{type}} - {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({metric('state_store_sst_store_block_request_counts', meta_miss_filter)}[$__rate_interval])) by (job, instance, type)",
                    "total_meta_miss_count - {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({metric('sstable_preload_io_count')}[$__rate_interval])) ",
                    "preload iops",
                ),
            ],
        ),
        panels.timeseries_ops(
            "File Cache Ops",
            "",
            [
                panels.target(
                    f"sum(rate({metric('file_cache_latency_count')}[$__rate_interval])) by (op, instance)",
                    "file cache {{op}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({metric('file_cache_miss')}[$__rate_interval])) by (instance)",
                    "file cache miss @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_ops(
            "Read Ops",
            "",
            [
                panels.target(
                    f"sum(rate({table_metric('state_store_get_duration_count')}[$__rate_interval])) by (job,instanc,table_id)",
                    "get - {{table_id}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({table_metric('state_store_range_reverse_scan_duration_count')}[$__rate_interval])) by (job,instance)",
                    "backward scan - {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({table_metric('state_store_get_shared_buffer_hit_counts')}[$__rate_interval])) by (job,instance,table_id)",
                    "shared_buffer hit - {{table_id}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({table_metric('state_store_iter_in_process_counts')}[$__rate_interval])) by(job,instance,table_id)",
                    "iter - {{table_id}} @ {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_latency(
            "Read Duration - Get",
            "Histogram of the latency of Get operations that have been issued to the state store.",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_get_duration_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"p{legend}" + " - {{table_id}} @ {{job}} @ {{instance}}",
                    ),
                    [50, 90, 99, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance, table_id)(rate({table_metric('state_store_get_duration_sum')}[$__rate_interval])) / sum by(le, job, instance, table_id) (rate({table_metric('state_store_get_duration_count')}[$__rate_interval]))",
                    "avg - {{table_id}} {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_latency(
            "Read Duration - Iter",
            "Histogram of the time spent on iterator initialization."
            "Histogram of the time spent on iterator scanning.",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_init_duration_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"create_iter_time p{legend} - {{{{table_id}}}} @ {{{{job}}}} @ {{{{instance}}}}",
                    ),
                    [90, 99, 999, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance)(rate({metric('state_store_iter_init_duration_sum')}[$__rate_interval])) / sum by(le, job,instance) (rate({metric('state_store_iter_init_duration_count')}[$__rate_interval]))",
                    "create_iter_time avg - {{job}} @ {{instance}}",
                ),
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_scan_duration_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"pure_scan_time p{legend} - {{{{table_id}}}} @ {{{{job}}}} @ {{{{instance}}}}",
                    ),
                    [90, 99, 999, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance)(rate({metric('state_store_iter_scan_duration_sum')}[$__rate_interval])) / sum by(le, job,instance) (rate({metric('state_store_iter_scan_duration_count')}[$__rate_interval]))",
                    "pure_scan_time avg - {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_bytes(
            "Read Item Size - Get",
            "",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_get_key_size_bucket')}[$__rate_interval])) by (le, job, instance, table_id)) + histogram_quantile({quantile}, sum(rate({table_metric('state_store_get_value_size_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"p{legend} - {{{{table_id}}}} {{{{job}}}} @ {{{{instance}}}}",
                    ),
                    [90, 99, 999, "max"],
                ),
            ],
        ),
        panels.timeseries_bytes(
            "Read Item Size - Iter",
            "",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_size_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"p{legend} - {{{{table_id}}}} @ {{{{job}}}} @ {{{{instance}}}}",
                    ),
                    [90, 99, 999, "max"],
                ),
            ],
        ),
        panels.timeseries_count(
            "Read Item Count - Iter",
            "",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_item_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"p{legend} - {{{{table_id}}}} @ {{{{job}}}} @ {{{{instance}}}}",
                    ),
                    [90, 99, 999, "max"],
                ),
            ],
        ),
        panels.timeseries_bytes_per_sec(
            "Read Throughput - Get",
            "The size of a single key-value pair when reading by operation Get."
            "Operation Get gets a single key-value pair with respect to a caller-specified key. If the key does not "
            "exist in the storage, the size of key is counted into this metric and the size of value is 0.",
            [
                panels.target(
                    f"sum(rate({metric('state_store_get_key_size_sum')}[$__rate_interval])) by(job, instance) + sum(rate({metric('state_store_get_value_size_sum')}[$__rate_interval])) by(job, instance)",
                    "{{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_bytes_per_sec(
            "Read Throughput - Iter",
            "The size of all the key-value paris when reading by operation Iter."
            "Operation Iter scans a range of key-value pairs.",
            [
                panels.target(
                    f"sum(rate({metric('state_store_iter_size_sum')}[$__rate_interval])) by(job, instance)",
                    "{{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_latency(
            "Read Duration - MayExist",
            "",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_may_exist_duration_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"p{legend}" + " - {{table_id}} @ {{job}} @ {{instance}}",
                    ),
                    [50, 90, 99, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance, table_id)(rate({table_metric('state_store_may_exist_duration_sum')}[$__rate_interval])) / sum by(le, job, instance, table_id) (rate({table_metric('state_store_may_exist_duration_count')}[$__rate_interval]))",
                    "avg - {{table_id}} {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_ops(
            "Read Bloom Filter",
            "",
            [
                panels.target(
                    f"sum(irate({table_metric('state_store_bloom_filter_true_negative_counts')}[$__rate_interval])) by (job,instance,table_id,type)",
                    "bloom filter true negative  - {{table_id}} - {{type}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(irate({table_metric('state_store_read_req_positive_but_non_exist_counts')}[$__rate_interval])) by (job,instance,table_id,type)",
                    "bloom filter false positive count  - {{table_id}} - {{type}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(irate({table_metric('state_store_read_req_bloom_filter_positive_counts')}[$__rate_interval])) by (job,instance,table_id,type)",
                    "read_req bloom filter positive - {{table_id}} - {{type}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(irate({table_metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by (job,instance,table_id,type)",
                    "read_req check bloom filter - {{table_id}} - {{type}} @ {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_ops(
            "Iter keys flow",
            "",
            [
                panels.target(
                    f"sum(rate({table_metric('state_store_iter_scan_key_counts')}[$__rate_interval])) by (instance, type, table_id)",
                    "iter keys flow - {{table_id}} @ {{type}} @ {{instance}} ",
                ),
            ],
        ),
        panels.timeseries_percentage(
            "Cache Miss Rate",
            "",
            [
                panels.target(
                    f"(sum(rate({table_metric('state_store_sst_store_block_request_counts', meta_miss_filter)}[$__rate_interval])) by (job,instance,table_id)) / (sum(rate({table_metric('state_store_sst_store_block_request_counts', meta_total_filter)}[$__rate_interval])) by (job,instance,table_id))",
                    "meta cache miss rate - {{table_id}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"(sum(rate({table_metric('state_store_sst_store_block_request_counts', data_miss_filter)}[$__rate_interval])) by (job,instance,table_id)) / (sum(rate({table_metric('state_store_sst_store_block_request_counts', data_total_filter)}[$__rate_interval])) by (job,instance,table_id))",
                    "block cache miss rate - {{table_id}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"(sum(rate({metric('file_cache_miss')}[$__rate_interval])) by (instance)) / (sum(rate({metric('file_cache_latency_count', file_cache_get_filter)}[$__rate_interval])) by (instance))",
                    "file cache miss rate @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_percentage(
            "Read Request Bloom-Filter Filtered Rate",
            "Negative / Total",
            [
                panels.target(
                    f"1 - (((sum(rate({table_metric('state_store_read_req_bloom_filter_positive_counts')}[$__rate_interval])) by (job,instance,table_id,type))) / (sum(rate({table_metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by (job,instance,table_id,type)))",
                    "read req bloom filter filter rate - {{table_id}} - {{type}} @ {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_percentage(
            "Read Request Bloom-Filter False-Positive Rate",
            "False-Positive / Positive",
            [
                panels.target(
                    f"1 - (((sum(rate({table_metric('state_store_read_req_positive_but_non_exist_counts')}[$__rate_interval])) by (job,instance,table_id,type))) / (sum(rate({table_metric('state_store_read_req_bloom_filter_positive_counts')}[$__rate_interval])) by (job,instance,table_id,type)))",
                    "read req bloom filter false positive rate - {{table_id}} - {{type}} @ {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_count(
            "Read Merged SSTs",
            "",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_merge_sstable_counts_bucket')}[$__rate_interval])) by (le, job, table_id, type))",
                        f"# merged ssts p{legend}" +
                        " - {{table_id}} @ {{job}} @ {{type}}",
                    ),
                    [90, 99, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance, table_id)(rate({table_metric('state_store_iter_merge_sstable_counts_sum')}[$__rate_interval]))  / sum by(le, job, instance, table_id)(rate({table_metric('state_store_iter_merge_sstable_counts_count')}[$__rate_interval]))",
                    "# merged ssts avg  - {{table_id}} @ {{job}} @ {{instance}}",
                ),
            ],
        ),

        panels.timeseries_count(
            "Uploader - Tasks Count",
            "",
            [
                panels.target(
                    f"sum(irate({table_metric('state_store_merge_imm_task_counts')}[$__rate_interval])) by (job,instance,table_id)",
                    "merge imm tasks - {{table_id}} @ {{instance}} ",
                ),
                panels.target(
                    f"sum(irate({metric('state_store_spill_task_counts')}[$__rate_interval])) by (job,instance,uploader_stage)",
                    "Uploader spill tasks - {{uploader_stage}} @ {{instance}} ",
                ),
            ],
        ),
        panels.timeseries_bytes(
            "Uploader - Task Size",
            "",
            [
                panels.target(
                    f"sum(rate({table_metric('state_store_merge_imm_memory_sz')}[$__rate_interval])) by (job,instance,table_id)",
                    "Merging tasks memory size - {{table_id}} @ {{instance}} ",
                ),
                panels.target(
                    f"sum(rate({metric('state_store_spill_task_size')}[$__rate_interval])) by (job,instance,uploader_stage)",
                    "Uploading tasks size - {{uploader_stage}} @ {{instance}} ",
                ),
            ],
        ),

        panels.timeseries_ops(
            "Write Ops",
            "",
            [
                panels.target(
                    f"sum(rate({table_metric('state_store_write_batch_duration_count')}[$__rate_interval])) by (job,instance,table_id)",
                    "write batch - {{table_id}} @ {{job}} @ {{instance}} ",
                ),
                panels.target(
                    f"sum(rate({metric('state_store_sync_duration_count')}[$__rate_interval])) by (job,instance)",
                    "l0 - {{job}} @ {{instance}} ",
                ),
            ],
        ),
        panels.timeseries_latency(
            "Write Duration",
            "",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_write_batch_duration_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"write to shared_buffer p{legend}" +
                        " - {{table_id}} @ {{job}} @ {{instance}}",
                    ),
                    [50, 90, 99, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance, table_id)(rate({table_metric('state_store_write_batch_duration_sum')}[$__rate_interval]))  / sum by(le, job, instance, table_id)(rate({table_metric('state_store_write_batch_duration_count')}[$__rate_interval]))",
                    "write to shared_buffer avg - {{table_id}} @ {{job}} @ {{instance}}",
                ),
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({metric('state_store_write_shared_buffer_sync_time_bucket')}[$__rate_interval])) by (le, job, instance))",
                        f"write to object_store p{legend}" +
                        " - {{job}} @ {{instance}}",
                    ),
                    [50, 90, 99, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance)(rate({metric('state_store_write_shared_buffer_sync_time_sum')}[$__rate_interval]))  / sum by(le, job, instance)(rate({metric('state_store_write_shared_buffer_sync_time_count')}[$__rate_interval]))",
                    "write to object_store - {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_ops(
            "Write Item Count",
            "",
            [
                panels.target(
                    f"sum(irate({table_metric('state_store_write_batch_tuple_counts')}[$__rate_interval])) by (job,instance,table_id)",
                    "write_batch_kv_pair_count - {{table_id}} @ {{instance}} ",
                ),
            ],
        ),
        panels.timeseries_bytes_per_sec(
            "Write Throughput",
            "",
            [
                panels.target(
                    f"sum(rate({table_metric('state_store_write_batch_size_sum')}[$__rate_interval]))by(job,instance,table_id) / sum(rate({table_metric('state_store_write_batch_size_count')}[$__rate_interval]))by(job,instance,table_id)",
                    "shared_buffer - {{table_id}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({metric('compactor_shared_buffer_to_sstable_size_sum')}[$__rate_interval]))by(job,instance) / sum(rate({metric('compactor_shared_buffer_to_sstable_size_count')}[$__rate_interval]))by(job,instance)",
                    "sync - {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_bytes(
            "Checkpoint Sync Size",
            "",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({metric('state_store_sync_size_bucket')}[$__rate_interval])) by (le, job, instance))",
                        f"p{legend}" + " - {{job}} @ {{instance}}",
                    ),
                    [50, 90, 99, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance) (rate({metric('state_store_sync_size_sum')}[$__rate_interval])) / sum by(le, job, instance) (rate({metric('state_store_sync_size_count')}[$__rate_interval]))",
                    "avg - {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_bytes(
            "Cache Size",
            "Hummock has three parts of memory usage: 1. Meta Cache 2. Block Cache 3. Uploader."
            "This metric shows the real memory usage of each of these three caches.",
            [
                panels.target(
                    f"avg({metric('state_store_meta_cache_size')}) by (job,instance)",
                    "meta cache - {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"avg({metric('state_store_block_cache_size')}) by (job,instance)",
                    "data cache - {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum({metric('state_store_limit_memory_size')}) by (job,instance)",
                    "Memory limiter usage - {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum({metric('state_store_uploader_uploading_task_size')}) by (job,instance)",
                    "uploading memory - {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_latency(
            "Row SeqScan Next Duration",
            "",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({metric('batch_row_seq_scan_next_duration_bucket')}[$__rate_interval])) by (le, job, instance))",
                        f"row_seq_scan next p{legend}" +
                        " - {{job}} @ {{instance}}",
                    ),
                    [50, 90, 99, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance) (rate({metric('batch_row_seq_scan_next_duration_sum')}[$__rate_interval])) / sum by(le, job, instance) (rate({metric('batch_row_seq_scan_next_duration_count')}[$__rate_interval]))",
                    "row_seq_scan next avg - {{job}} @ {{instance}}",
                ),
            ],
        ),

        panels.timeseries_latency(
            "Fetch Meta Duration",
            "",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_fetch_meta_duration_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"fetch_meta_duration p{legend}" +
                        " - {{table_id}} @ {{job}} @ {{instance}}",
                    ),
                    [50, 90, 99, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance, table_id) (rate({table_metric('state_store_iter_fetch_meta_duration_sum')}[$__rate_interval])) / sum by(le, job, instance, table_id) (rate({table_metric('state_store_iter_fetch_meta_duration_count')}[$__rate_interval]))",
                    "fetch_meta_duration avg - {{table_id}} @ {{job}} @ {{instance}}",
                ),
            ],
        ),

        panels.timeseries_count(
            "Fetch Meta Unhits",
            "",
            [
                panels.target(
                    f"{metric('state_store_iter_fetch_meta_cache_unhits')}",
                    "",
                ),
            ],
        ),

        panels.timeseries_count(
            "Slow Fetch Meta Unhits",
            "",
            [
                panels.target(
                    f"{metric('state_store_iter_slow_fetch_meta_cache_unhits')}",
                    "",
                ),
            ],
        ),
    ]


def section_hummock_tiered_cache(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Hummock Tiered Cache",
            [
                panels.timeseries_ops(
                    "Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('file_cache_latency_count')}[$__rate_interval])) by (op, instance)",
                            "file cache {{op}} @ {{instance}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('file_cache_miss')}[$__rate_interval])) by (instance)",
                            "file cache miss @ {{instance}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('file_cache_disk_latency_count')}[$__rate_interval])) by (op, instance)",
                            "file cache disk {{op}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Latency",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('file_cache_latency_bucket')}[$__rate_interval])) by (le, op, instance))",
                                f"p{legend} - file cache" +
                                " - {{op}} @ {{instance}}",
                            ),
                            [50, 90, 99, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('file_cache_disk_latency_bucket')}[$__rate_interval])) by (le, op, instance))",
                                f"p{legend} - file cache disk" +
                                " - {{op}} @ {{instance}}",
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('file_cache_disk_bytes')}[$__rate_interval])) by (op, instance)",
                            "disk {{op}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Disk IO Size",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('file_cache_disk_io_size_bucket')}[$__rate_interval])) by (le, op, instance))",
                                f"p{legend} - file cache disk" +
                                " - {{op}} @ {{instance}}",
                            ),
                            [50, 90, 99, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('file_cache_disk_read_entry_size_bucket')}[$__rate_interval])) by (le, op, instance))",
                                f"p{legend} - file cache disk read entry" +
                                " - {{op}} @ {{instance}}",
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
            ],
        )
    ]


def section_hummock_manager(outer_panels):
    panels = outer_panels.sub_panel()
    total_key_size_filter = "metric='total_key_size'"
    total_value_size_filter = "metric='total_value_size'"
    total_key_count_filter = "metric='total_key_count'"
    return [
        outer_panels.row_collapsed(
            "Hummock Manager",
            [
                panels.timeseries_latency(
                    "Lock Time",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('hummock_manager_lock_time_bucket')}[$__rate_interval])) by (le, lock_name, lock_type))",
                                f"Lock Time p{legend}" +
                                " - {{lock_type}} @ {{lock_name}}",
                            ),
                            [50, 99, 999, "max"],
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Real Process Time",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('meta_hummock_manager_real_process_time_bucket')}[$__rate_interval])) by (le, method))",
                                f"Real Process Time p{legend}" +
                                " - {{method}}",
                            ),
                            [50, 99, 999, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Version Size",
                    "",
                    [
                        panels.target(
                            f"{metric('storage_version_size')}", "version size"),
                    ],
                ),
                panels.timeseries_id(
                    "Version Id",
                    "",
                    [
                        panels.target(f"{metric('storage_current_version_id')}",
                                      "current version id"),
                        panels.target(f"{metric('storage_checkpoint_version_id')}",
                                      "checkpoint version id"),
                        panels.target(f"{metric('storage_min_pinned_version_id')}",
                                      "min pinned version id"),
                        panels.target(f"{metric('storage_min_safepoint_version_id')}",
                                      "min safepoint version id"),
                    ],
                ),
                panels.timeseries_id(
                    "Epoch",
                    "",
                    [
                        panels.target(f"{metric('storage_max_committed_epoch')}",
                                      "max committed epoch"),
                        panels.target(
                            f"{metric('storage_safe_epoch')}", "safe epoch"),
                        panels.target(f"{metric('storage_min_pinned_epoch')}",
                                      "min pinned epoch"),
                    ],
                ),
                panels.timeseries_kilobytes(
                    "Table KV Size",
                    "",
                    [
                        panels.target(f"{table_metric('storage_version_stats', total_key_size_filter)}/1024",
                                      "table{{table_id}} {{metric}}"),
                        panels.target(f"{table_metric('storage_version_stats', total_value_size_filter)}/1024",
                                      "table{{table_id}} {{metric}}"),
                    ],
                ),
                panels.timeseries_count(
                    "Table KV Count",
                    "",
                    [
                        panels.target(f"{table_metric('storage_version_stats', total_key_count_filter)}",
                                      "table{{table_id}} {{metric}}"),
                    ],
                ),
                panels.timeseries_count(
                    "Object Total Number",
                    """
Objects are classified into 3 groups:
- not referenced by versions: these object are being deleted from object store.
- referenced by non-current versions: these objects are stale (not in the latest version), but those old versions may still be in use (e.g. long-running pinning). Thus those objects cannot be deleted at the moment.
- referenced by current version: these objects are in the latest version.
                    """,
                    [
                        panels.target(f"{metric('storage_stale_object_count')}",
                                      "not referenced by versions"),
                        panels.target(f"{metric('storage_old_version_object_count')}",
                                      "referenced by non-current versions"),
                        panels.target(f"{metric('storage_current_version_object_count')}",
                                      "referenced by current version"),
                    ],
                ),
                panels.timeseries_bytes(
                    "Object Total Size",
                    "Refer to `Object Total Number` panel for classification of objects.",
                    [
                        panels.target(f"{metric('storage_stale_object_size')}",
                                      "not referenced by versions"),
                        panels.target(f"{metric('storage_old_version_object_size')}",
                                      "referenced by non-current versions"),
                        panels.target(f"{metric('storage_current_version_object_size')}",
                                      "referenced by current version"),
                    ],
                ),
                panels.timeseries_count(
                    "Delta Log Total Number",
                    "total number of hummock version delta log",
                    [
                        panels.target(f"{metric('storage_delta_log_count')}",
                                      "delta log total number"),
                    ],
                ),
                panels.timeseries_latency(
                    "Version Checkpoint Latency",
                    "hummock version checkpoint latency",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('storage_version_checkpoint_latency_bucket')}[$__rate_interval])) by (le))",
                            f"version_checkpoint_latency_p{legend}",
                        ),
                        [50, 90, 99, 999, "max"],
                    ) + [
                        panels.target(
                            f"rate({metric('storage_version_checkpoint_latency_sum')}[$__rate_interval]) / rate({metric('storage_version_checkpoint_latency_count')}[$__rate_interval])",
                            "version_checkpoint_latency_avg",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Write Stop Compaction Group Total Number",
                    "When certain per compaction group threshold is exceeded (e.g. number of level 0 sub-level in LSMtree), write op to that compaction group is stopped temporarily. Check log for detail reason of write stop.",
                    [
                        panels.target(f"{metric('storage_write_stop_compaction_group_num')}",
                                      "write_stop_compaction_group_num"),
                    ],
                ),
            ],
        )
    ]


def section_backup_manager(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Backup Manager",
            [
                panels.timeseries_count(
                    "Job Count",
                    "Total backup job count since the Meta node starts",
                    [
                        panels.target(
                            f"{metric('backup_job_count')}",
                            "job count",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Job Process Time",
                    "Latency of backup jobs since the Meta node starts",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('backup_job_latency_bucket')}[$__rate_interval])) by (le, state))",
                                f"Job Process Time p{legend}" +
                                " - {{state}}",
                            ),
                            [50, 99, 999, "max"],
                        ),
                    ],
                ),
            ],
        )
    ]


def grpc_metrics_target(panels, name, filter):
    return panels.timeseries_latency_small(
        f"{name} latency",
        "",
        [
            panels.target(
                f"histogram_quantile(0.5, sum(irate({metric('meta_grpc_duration_seconds_bucket', filter)}[$__rate_interval])) by (le))",
                f"{name}_p50",
            ),
            panels.target(
                f"histogram_quantile(0.9, sum(irate({metric('meta_grpc_duration_seconds_bucket', filter)}[$__rate_interval])) by (le))",
                f"{name}_p90",
            ),
            panels.target(
                f"histogram_quantile(0.99, sum(irate({metric('meta_grpc_duration_seconds_bucket', filter)}[$__rate_interval])) by (le))",
                f"{name}_p99",
            ),
            panels.target(
                f"sum(irate({metric('meta_grpc_duration_seconds_sum', filter)}[$__rate_interval])) / sum(irate({metric('meta_grpc_duration_seconds_count', filter)}[$__rate_interval]))",
                f"{name}_avg",
            ),
        ],
    )


def section_grpc_meta_catalog_service(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "gRPC Meta: Catalog Service",
            [
                grpc_metrics_target(
                    panels, "Create", "path='/meta.CatalogService/Create'"),
                grpc_metrics_target(
                    panels, "Drop", "path='/meta.CatalogService/Drop'"),
                grpc_metrics_target(panels, "GetCatalog",
                                    "path='/meta.CatalogService/GetCatalog'"),
            ],
        )
    ]


def section_grpc_meta_cluster_service(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "gRPC Meta: Cluster Service",
            [
                grpc_metrics_target(panels, "AddWorkerNode",
                                    "path='/meta.ClusterService/AddWorkerNode'"),
                grpc_metrics_target(panels, "ListAllNodes",
                                    "path='/meta.ClusterService/ListAllNodes'"),
            ],
        ),
    ]


def section_grpc_meta_stream_manager(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "gRPC Meta: Stream Manager",
            [
                grpc_metrics_target(panels, "CreateMaterializedView",
                                    "path='/meta.StreamManagerService/CreateMaterializedView'"),
                grpc_metrics_target(panels, "DropMaterializedView",
                                    "path='/meta.StreamManagerService/DropMaterializedView'"),
                grpc_metrics_target(
                    panels, "Flush", "path='/meta.StreamManagerService/Flush'"),
            ],
        ),
    ]


def section_grpc_meta_hummock_manager(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "gRPC Meta: Hummock Manager",
            [
                grpc_metrics_target(
                    panels, "UnpinVersionBefore", "path='/meta.HummockManagerService/UnpinVersionBefore'"),
                grpc_metrics_target(panels, "UnpinSnapshotBefore",
                                    "path='/meta.HummockManagerService/UnpinSnapshotBefore'"),
                grpc_metrics_target(panels, "ReportCompactionTasks",
                                    "path='/meta.HummockManagerService/ReportCompactionTasks'"),
                grpc_metrics_target(
                    panels, "GetNewSstIds", "path='/meta.HummockManagerService/GetNewSstIds'"),
            ],
        ),
    ]


def section_grpc_hummock_meta_client(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "gRPC: Hummock Meta Client",
            [
                panels.timeseries_count(
                    "compaction_count",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('state_store_report_compaction_task_counts')}[$__rate_interval])) by(job,instance)",
                            "report_compaction_task_counts - {{instance}} ",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "version_latency",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.5, sum(irate({metric('state_store_unpin_version_before_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "unpin_version_before_latency_p50 - {{instance}} ",
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(irate({metric('state_store_unpin_version_before_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "unpin_version_before_latency_p99 - {{instance}} ",
                        ),
                        panels.target(
                            f"sum(irate({metric('state_store_unpin_version_before_latency_sum')}[$__rate_interval])) / sum(irate({metric('state_store_unpin_version_before_latency_count')}[$__rate_interval]))",
                            "unpin_version_before_latency_avg",
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(irate({metric('state_store_unpin_version_before_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "unpin_version_before_latency_p90 - {{instance}} ",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "snapshot_latency",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.5, sum(irate({metric('state_store_pin_snapshot_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "pin_snapshot_latency_p50 - {{instance}} ",
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(irate({metric('state_store_pin_snapshot_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "pin_snapshot_latency_p99 - {{instance}} ",
                        ),
                        panels.target(
                            f"histogram_quantile(0.9, sum(irate({metric('state_store_pin_snapshot_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "pin_snapshot_latencyp90 - {{instance}} ",
                        ),
                        panels.target(
                            f"sum(irate({metric('state_store_pin_snapshot_latency_sum')}[$__rate_interval])) / sum(irate(state_store_pin_snapshot_latency_count[$__rate_interval]))",
                            "pin_snapshot_latency_avg",
                        ),
                        panels.target(
                            f"histogram_quantile(0.5, sum(irate({metric('state_store_unpin_version_snapshot_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "unpin_snapshot_latency_p50 - {{instance}} ",
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(irate({metric('state_store_unpin_version_snapshot_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "unpin_snapshot_latency_p99 - {{instance}} ",
                        ),
                        panels.target(
                            f"sum(irate({metric('state_store_unpin_snapshot_latency_sum')}[$__rate_interval])) / sum(irate(state_store_unpin_snapshot_latency_count[$__rate_interval]))",
                            "unpin_snapshot_latency_avg",
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(irate({metric('state_store_unpin_snapshot_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "unpin_snapshot_latency_p90 - {{instance}} ",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "snapshot_count",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('state_store_pin_snapshot_counts')}[$__rate_interval])) by(job,instance)",
                            "pin_snapshot_counts - {{instance}} ",
                        ),
                        panels.target(
                            f"sum(irate({metric('state_store_unpin_snapshot_counts')}[$__rate_interval])) by(job,instance)",
                            "unpin_snapshot_counts - {{instance}} ",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "table_latency",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.5, sum(irate({metric('state_store_get_new_sst_ids_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "get_new_sst_ids_latency_latency_p50 - {{instance}} ",
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(irate({metric('state_store_get_new_sst_ids_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "get_new_sst_ids_latency_latency_p99 - {{instance}} ",
                        ),
                        panels.target(
                            f"sum(irate({metric('state_store_get_new_sst_ids_latency_sum')}[$__rate_interval])) / sum(irate({metric('state_store_get_new_sst_ids_latency_count')}[$__rate_interval]))",
                            "get_new_sst_ids_latency_latency_avg",
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(irate({metric('state_store_get_new_sst_ids_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "get_new_sst_ids_latency_latency_p90 - {{instance}} ",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "table_count",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('state_store_get_new_sst_ids_latency_counts')}[$__rate_interval]))by(job,instance)",
                            "get_new_sst_ids_latency_counts - {{instance}} ",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "compaction_latency",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.5, sum(irate({metric('state_store_report_compaction_task_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "report_compaction_task_latency_p50 - {{instance}}",
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(irate({metric('state_store_report_compaction_task_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "report_compaction_task_latency_p99 - {{instance}}",
                        ),
                        panels.target(
                            f"sum(irate({metric('state_store_report_compaction_task_latency_sum')}[$__rate_interval])) / sum(irate(state_store_report_compaction_task_latency_count[$__rate_interval]))",
                            "report_compaction_task_latency_avg",
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(irate({metric('state_store_report_compaction_task_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "report_compaction_task_latency_p90 - {{instance}}",
                        ),
                    ],
                ),
            ],
        ),
    ]


def section_memory_manager(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Memory manager",
            [
                panels.timeseries_count(
                    "LRU manager loop count per sec",
                    "",
                    [
                        panels.target(
                            f"rate({metric('lru_runtime_loop_count')}[$__rate_interval])",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "LRU manager watermark steps",
                    "",
                    [
                        panels.target(
                            f"{metric('lru_watermark_step')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_ms(
                    "LRU manager diff between watermark_time and now (ms)",
                    "watermark_time is the current lower watermark of cached data. physical_now is the current time of the machine. The diff (physical_now - watermark_time) shows how much data is cached.",
                    [
                        panels.target(
                            f"{metric('lru_physical_now_ms')} - {metric('lru_current_watermark_time_ms')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The allocated memory of jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_allocated_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The active memory of jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_active_bytes')}",
                            "",
                        ),
                    ],
                ),
            ],
        ),
    ]


def section_connector_node(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Connector Node",
            [
                panels.timeseries_rowsps(
                    "Connector Source Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"rate({metric('connector_source_rows_received')}[$__interval])",
                            "{{source_type}} @ {{source_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Connector Sink Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"rate({metric('connector_sink_rows_received')}[$__interval])",
                            "{{connector_type}} @ {{sink_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]


templating_list = []
if dynamic_source_enabled:
    templating_list.append(
        {
            "hide": 0,
            "includeAll": False,
            "multi": False,
            "name": f"{datasource_const}",
            "options": [],
            "query": "prometheus",
            "queryValue": "",
            "refresh": 2,
            "skipUrlSync": False,
            "type": "datasource"
        }
    )

if namespace_filter_enabled:
    namespace_json = {
        "definition": "label_values(up{risingwave_name=~\".+\"}, namespace)",
        "description": "Kubernetes namespace.",
        "hide": 0,
        "includeAll": False,
        "label": "Namespace",
        "multi": False,
        "name": "namespace",
        "options": [],
        "query": {
            "query": "label_values(up{risingwave_name=~\".+\"}, namespace)",
            "refId": "StandardVariableQuery"
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 0,
        "type": "query",
    }

    name_json = {
        "current": {
            "selected": False,
            "text": "risingwave",
            "value": "risingwave"
        },
        "definition": "label_values(up{namespace=\"$namespace\", risingwave_name=~\".+\"}, risingwave_name)",
        "hide": 0,
        "includeAll": False,
        "label": "RisingWave",
        "multi": False,
        "name": "instance",
        "options": [],
        "query": {
            "query": "label_values(up{namespace=\"$namespace\", risingwave_name=~\".+\"}, risingwave_name)",
            "refId": "StandardVariableQuery"
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 6,
        "type": "query",
    }
    if dynamic_source_enabled:
        namespace_json = merge(namespace_json, {"datasource": datasource})
        name_json = merge(name_json, {"datasource": datasource})

    templating_list.append(namespace_json)
    templating_list.append(name_json)

node_json = {
    "current": {
        "selected": False,
        "text": "All",
        "value": "__all"
    },
    "definition": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, instance)",
    "description": "Reporting instance of the metric",
    "hide": 0,
    "includeAll": True,
    "label": "Node",
    "multi": True,
    "name": "node",
    "options": [],
    "query": {
        "query": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, instance)",
        "refId": "StandardVariableQuery"
    },
    "refresh": 2,
    "regex": "",
    "skipUrlSync": False,
    "sort": 6,
    "type": "query",
}

job_json = {
    "current": {
        "selected": False,
        "text": "All",
        "value": "__all"
    },
    "definition": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, job)",
    "description": "Reporting job of the metric",
    "hide": 0,
    "includeAll": True,
    "label": "Job",
    "multi": True,
    "name": "job",
    "options": [],
    "query": {
        "query": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, job)",
        "refId": "StandardVariableQuery"
    },
    "refresh": 2,
    "regex": "",
    "skipUrlSync": False,
    "sort": 6,
    "type": "query",
}

table_id_json = {
    "current": {
        "selected": False,
        "text": "All",
        "value": "__all"
    },
    "definition": f"label_values({metric('table_info', node_filter_enabled=False)}, table_id)",
    "description": "Reporting table id of the metric",
    "hide": 0,
    "includeAll": True,
    "label": "Table",
    "multi": True,
    "name": "table",
    "options": [],
    "query": {
        "query": f"label_values({metric('table_info', node_filter_enabled=False)}, table_id)",
        "refId": "StandardVariableQuery"
    },
    "refresh": 2,
    "regex": "",
    "skipUrlSync": False,
    "sort": 6,
    "type": "query",
}

if dynamic_source_enabled:
    node_json = merge(node_json, {"datasource": datasource})
    job_json = merge(job_json, {"datasource": datasource})
    table_id_json = merge(table_id_json, {"datasource": datasource})

templating_list.append(node_json)
templating_list.append(job_json)
templating_list.append(table_id_json)

templating = Templating(templating_list)

dashboard = Dashboard(
    title="risingwave_dev_dashboard",
    description="RisingWave Dev Dashboard",
    tags=["risingwave"],
    timezone="browser",
    editable=True,
    uid=dashboard_uid,
    time=Time(start="now-30m", end="now"),
    sharedCrosshair=True,
    templating=templating,
    version=dashboard_version,
    panels=[
        *section_actor_info(panels),
        *section_cluster_node(panels),
        *section_recovery_node(panels),
        *section_streaming(panels),
        *section_streaming_actors(panels),
        *section_streaming_actors_tokio(panels),
        *section_streaming_exchange(panels),
        *section_streaming_errors(panels),
        *section_batch(panels),
        *section_hummock(panels),
        *section_compaction(panels),
        *section_object_storage(panels),
        *section_hummock_tiered_cache(panels),
        *section_hummock_manager(panels),
        *section_backup_manager(panels),
        *section_grpc_meta_catalog_service(panels),
        *section_grpc_meta_cluster_service(panels),
        *section_grpc_meta_stream_manager(panels),
        *section_grpc_meta_hummock_manager(panels),
        *section_grpc_hummock_meta_client(panels),
        *section_frontend(panels),
        *section_memory_manager(panels),
        *section_connector_node(panels),
    ],
).auto_panel_ids()
