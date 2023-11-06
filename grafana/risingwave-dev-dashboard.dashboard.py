import logging
import os
import sys

p = os.path.dirname(__file__)
sys.path.append(p)

from jsonmerge import merge

from common import *

source_uid = os.environ.get(SOURCE_UID, "risedev-prometheus")
dashboard_uid = os.environ.get(DASHBOARD_UID, "Ecy3uV1nz")
dashboard_version = int(os.environ.get(DASHBOARD_VERSION, "0"))
datasource = {"type": "prometheus", "uid": f"{source_uid}"}

datasource_const = "datasource"
if dynamic_source_enabled:
    datasource = {"type": "prometheus", "uid": "${datasource}"}

panels = Panels(datasource)
logging.basicConfig(level=logging.WARN)


def section_actor_info(outer_panels):
    panels = outer_panels.sub_panel()
    excluded_cols = ["Time", "Value", "__name__", f"{COMPONENT_LABEL}", f"{NODE_LABEL}"]
    return [
        outer_panels.row_collapsed(
            "Actor/Table Id Info",
            [
                panels.table_info(
                    "Actor Id Info",
                    "Mapping from actor id to fragment id",
                    [panels.table_target(f"{metric('actor_info')}")],
                    excluded_cols,
                ),
                panels.table_info(
                    "Materialized View Info",
                    "Mapping from materialized view table id to it's internal table ids",
                    [panels.table_target(f"{metric('table_info')}")],
                    excluded_cols,
                ),
            ],
        )
    ]


def section_cluster_node(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Cluster Node",
            [
                panels.timeseries_count(
                    "Node Count",
                    "The number of each type of RisingWave components alive.",
                    [
                        panels.target(
                            f"sum({metric('worker_num')}) by (worker_type)",
                            "{{worker_type}}",
                        )
                    ],
                    ["last"],
                ),
                panels.timeseries_memory(
                    "Node Memory",
                    "The memory usage of each RisingWave component.",
                    [
                        panels.target(
                            f"avg({metric('process_resident_memory_bytes')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        )
                    ],
                ),
                panels.timeseries_cpu(
                    "Node CPU",
                    "The CPU usage of each RisingWave component.",
                    [
                        panels.target(
                            f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "cpu usage (total) - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / avg({metric('process_cpu_core_num')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "cpu usage (avg per core) - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Meta Cluster",
                    "RW cluster can configure multiple meta nodes to achieve high availability. One is the leader and the "
                    "rest are the followers.",
                    [
                        panels.target(
                            f"sum({metric('meta_num')}) by (worker_addr,role)",
                            "{{worker_addr}} @ {{role}}",
                        )
                    ],
                    ["last"],
                ),
            ],
        )
    ]


def section_recovery_node(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Recovery",
            [
                panels.timeseries_ops(
                    "Recovery Successful Rate",
                    "The rate of successful recovery attempts",
                    [
                        panels.target(
                            f"sum(rate({metric('recovery_latency_count')}[$__rate_interval])) by ({NODE_LABEL})",
                            "{{%s}}" % NODE_LABEL,
                        )
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "Failed recovery attempts",
                    "Total number of failed reocovery attempts",
                    [
                        panels.target(
                            f"sum({metric('recovery_failure_cnt')}) by ({NODE_LABEL})",
                            "{{%s}}" % NODE_LABEL,
                        )
                    ],
                    ["last"],
                ),
                panels.timeseries_latency(
                    "Recovery latency",
                    "Time spent in a successful recovery attempt",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('recovery_latency_bucket')}[$__rate_interval])) by (le, {NODE_LABEL}))",
                                f"recovery latency p{legend}"
                                + " - {{%s}}" % NODE_LABEL,
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by (le) (rate({metric('recovery_latency_sum')}[$__rate_interval])) / sum by (le) (rate({metric('recovery_latency_count')}[$__rate_interval]))",
                            "recovery latency avg",
                        ),
                    ],
                    ["last"],
                ),
            ],
        )
    ]


def section_compaction(outer_panels):
    successful_compaction_fiiler = "result='SUCCESS'"
    failed_compaction_filter = "result!='SUCCESS'"
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
                            f"sum({metric('storage_level_sst_num')}) by ({NODE_LABEL}, level_index)",
                            "L{{level_index}}",
                        ),
                    ],
                ),
                panels.timeseries_kilobytes(
                    "SSTable Size(KB)",
                    "The size(KB) of SSTables at each level",
                    [
                        panels.target(
                            f"sum({metric('storage_level_total_file_size')}) by ({NODE_LABEL}, level_index)",
                            "L{{level_index}}",
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "Commit Flush Bytes by Table",
                    "The  of bytes that have been written by commit epoch per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_commit_write_throughput')}[$__rate_interval])) by (table_id)",
                            "write - {{table_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Failure Count",
                    "The number of compactions from one level to another level that have completed or failed",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_frequency', failed_compaction_filter)}) by (compactor, group, task_type, result)",
                            "{{task_type}} - {{result}} - group-{{group}} @ {{compactor}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Success Count",
                    "The number of compactions from one level to another level that have completed or failed",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_frequency', successful_compaction_fiiler)}) by (compactor, group, task_type, result)",
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
                                f"p{legend}" + " - cg{{group}}@{{type}}",
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
                            f"avg({metric('storage_compact_task_pending_num')}) by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "compactor_task_split_count - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Compaction Duration",
                    "compact-task: The total time have been spent on compaction.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('compactor_compact_task_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}))",
                                f"compact-task p{legend}"
                                + " - {{%s}}" % COMPONENT_LABEL,
                            ),
                            [50, 90, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('compactor_compact_sst_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}))",
                                f"compact-key-range p{legend}"
                                + " - {{%s}}" % COMPONENT_LABEL,
                            ),
                            [90, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('compactor_get_table_id_total_time_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}))",
                                f"get-table-id p{legend}"
                                + " - {{%s}}" % COMPONENT_LABEL,
                            ),
                            [90, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('compactor_remote_read_time_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}))",
                                f"remote-io p{legend}" + " - {{%s}}" % COMPONENT_LABEL,
                            ),
                            [90, "max"],
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(rate({metric('compute_refill_cache_duration_bucket')}[$__rate_interval])) by (le))",
                            "compute_apply_version_duration_p99",
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
                            f"sum(rate({metric('storage_level_compact_read_next')}[$__rate_interval])) by({COMPONENT_LABEL}) + sum(rate("
                            f"{metric('storage_level_compact_read_curr')}[$__rate_interval])) by({COMPONENT_LABEL})",
                            "read - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_write')}[$__rate_interval])) by({COMPONENT_LABEL})",
                            "write - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('compactor_write_build_l0_bytes')}[$__rate_interval]))by ({COMPONENT_LABEL})",
                            "flush - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('compactor_fast_compact_bytes')}[$__rate_interval]))by ({COMPONENT_LABEL})",
                            "fast compact - {{%s}}" % COMPONENT_LABEL,
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
                            f"sum({metric('storage_level_compact_write')}) by ({COMPONENT_LABEL})",
                            "write - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum({metric('compactor_write_build_l0_bytes')}) by ({COMPONENT_LABEL})",
                            "flush - {{%s}}" % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Compaction Write Amplification",
                    "Write amplification is the amount of bytes written to the remote storage by compaction for each "
                    "one byte of flushed SSTable data. Write amplification is by definition higher than 1.0 because "
                    "we write each piece of data to L0, and then write it again to an SSTable, and then compaction "
                    "may read this piece of data and write it to a new SSTable, that's another write.",
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
                        panels.target(
                            f"{metric('storage_level_compact_cnt')}", "L{{level_index}}"
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compacting Task Count",
                    "num of compact_task",
                    [
                        panels.target(
                            f"{metric('storage_level_compact_task_cnt')}", "{{task}}"
                        ),
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
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_bloom_filter_size_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_bloom_filter_size_count')}[$__rate_interval]))",
                            "avg_meta - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_file_size_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_file_size_count')}[$__rate_interval]))",
                            "avg_file - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Hummock Sstable Item Size",
                    "Total bytes gotten from sstable_avg_key_size, for observing sstable_avg_key_size",
                    [
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_avg_key_size_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_avg_key_size_count')}[$__rate_interval]))",
                            "avg_key_size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_avg_value_size_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_avg_value_size_count')}[$__rate_interval]))",
                            "avg_value_size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Hummock Sstable Stat",
                    "Avg count gotten from sstable_distinct_epoch_count, for observing sstable_distinct_epoch_count",
                    [
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_distinct_epoch_count_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_distinct_epoch_count_count')}[$__rate_interval]))",
                            "avg_epoch_count - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Hummock Remote Read Duration",
                    "Total time of operations which read from remote storage when enable prefetch",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_remote_read_time_per_task_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"remote-io p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
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
                            f"sum(rate({metric('compactor_iter_scan_key_counts')}[$__rate_interval])) by ({NODE_LABEL}, type)",
                            "iter keys flow - {{type}} @ {{%s}} " % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Lsm Compact Pending Bytes",
                    "bytes of Lsm tree needed to reach balance",
                    [
                        panels.target(
                            f"sum({metric('storage_compact_pending_bytes')}) by ({NODE_LABEL}, group)",
                            "compact pending bytes - {{group}} @ {{%s}} " % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Lsm Level Compression Ratio",
                    "compression ratio of each level of the lsm tree",
                    [
                        panels.target(
                            f"sum({metric('storage_compact_level_compression_ratio')}) by ({NODE_LABEL}, group, level, algorithm)",
                            "lsm compression ratio - cg{{group}} @ L{{level}} - {{algorithm}} {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
            ],
        )
    ]


def section_object_storage(outer_panels):
    panels = outer_panels.sub_panel()
    operation_rate_blacklist = (
        "type!~'streaming_upload_write_bytes|streaming_read_read_bytes|streaming_read'"
    )
    operation_duration_blacklist = "type!~'streaming_upload_write_bytes|streaming_read'"
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
                            f"sum(rate({metric('object_store_read_bytes')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "read - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_write_bytes')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "write - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Operation Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('object_store_operation_latency_bucket', operation_duration_blacklist)}[$__rate_interval])) by (le, type, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                "{{type}}"
                                + f" p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, type, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('object_store_operation_latency_sum', operation_duration_blacklist)}[$__rate_interval])) / sum by(le, type, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('object_store_operation_latency_count')}[$__rate_interval]))",
                            "{{type}} avg - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Operation Rate",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_operation_latency_count', operation_rate_blacklist)}[$__rate_interval])) by (le, type, {COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_operation_latency_count', write_op_filter)}[$__rate_interval])) by (le, media_type, {COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{media_type}}-write - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_operation_latency_count', read_op_filter)}[$__rate_interval])) by (le, media_type, {COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{media_type}}-read - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Operation Size",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('object_store_operation_bytes_bucket')}[$__rate_interval])) by (le, type, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "{{type}}"
                            + f" p{legend}"
                            + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        [50, 99, "max"],
                    ),
                ),
                panels.timeseries_ops(
                    "Operation Failure Rate",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_failure_count')}[$__rate_interval])) by ({NODE_LABEL}, {COMPONENT_LABEL}, type)",
                            "{{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        )
                    ],
                ),
                panels.timeseries_ops(
                    "Operation Retry Rate",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('aws_sdk_retry_counts')}[$__rate_interval])) by ({NODE_LABEL}, {COMPONENT_LABEL}, type)",
                            "{{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(irate({metric('s3_read_request_retry_count')}[$__rate_interval])) by ({NODE_LABEL}, {COMPONENT_LABEL}, type)",
                            "{{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
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
                            f"sum({metric('storage_level_total_file_size')}) by ({NODE_LABEL}) * 0.023 / 1000 / 1000",
                            "Monthly Storage Cost",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_streaming(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming",
            [
                panels.timeseries_rowsps(
                    "Source Throughput(rows/s)",
                    "The figure shows the number of rows read by each source per second.",
                    [
                        panels.target(
                            f"rate({metric('stream_source_output_rows_counts')}[$__rate_interval])",
                            "source={{source_name}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
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
                            "actor={{actor_id}} source={{source_id}} @ {{%s}}"
                            % NODE_LABEL,
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Source Upstream Status",
                    "Monitor each source upstream, 0 means the upstream is not normal, 1 means the source is ready.",
                    [
                        panels.target(
                            f"{metric('source_status_is_up')}",
                            "source_id={{source_id}}, source_name={{source_name}} @ {{%s}}"
                            % NODE_LABEL,
                        )
                    ],
                ),
                panels.timeseries_ops(
                    "Source Split Change Events frequency(events/s)",
                    "Source Split Change Events frequency by source_id and actor_id",
                    [
                        panels.target(
                            f"rate({metric('stream_source_split_change_event_count')}[$__rate_interval])",
                            "source={{source_name}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Kafka Consumer Lag Size",
                    "Kafka Consumer Lag Size by source_id, partition and actor_id",
                    [
                        panels.target(
                            f"{metric('high_watermark')}",
                            "source={{source_id}} partition={{partition}}",
                        ),
                        panels.target(
                            f"{metric('latest_message_id')}",
                            "source={{source_id}} partition={{partition}} actor_id={{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Sink Throughput(rows/s)",
                    "The number of rows streamed into each sink per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_sink_input_row_count')}[$__rate_interval])) by (sink_id) * on(sink_id) group_left(sink_name) group({metric('sink_info')}) by (sink_id, sink_name)",
                            "sink {{sink_id}} {{sink_name}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Sink Throughput(rows/s) per Partition",
                    "The number of rows streamed into each sink per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_sink_input_row_count')}[$__rate_interval])) by (sink_id, actor_id) * on(actor_id) group_left(sink_name) {metric('sink_info')}",
                            "sink {{sink_id}} {{sink_name}} - actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Materialized View Throughput(rows/s)",
                    "The figure shows the number of rows written into each materialized view per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_mview_input_row_count')}[$__rate_interval])) by (table_id) * on(table_id) group_left(table_name) group({metric('table_info')}) by (table_id, table_name)",
                            "mview {{table_id}} {{table_name}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Materialized View Throughput(rows/s) per Partition",
                    "The figure shows the number of rows written into each materialized view per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_mview_input_row_count')}[$__rate_interval])) by (actor_id, table_id) * on(actor_id, table_id) group_left(table_name) {metric('table_info')}",
                            "mview {{table_id}} {{table_name}} - actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Backfill Snapshot Read Throughput(rows)",
                    "Total number of rows that have been read from the backfill snapshot",
                    [
                        panels.target(
                            f"rate({table_metric('stream_backfill_snapshot_read_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Backfill Upstream Throughput(rows)",
                    "Total number of rows that have been output from the backfill upstream",
                    [
                        panels.target(
                            f"rate({table_metric('stream_backfill_upstream_output_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
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
                            f"{metric('in_flight_barrier_nums')}", "in_flight_barrier"
                        ),
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
                    )
                    + [
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
                    )
                    + [
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
                    )
                    + [
                        panels.target(
                            f"max(sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_inflight_duration_seconds_sum')}[$__rate_interval]))  / sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_inflight_duration_seconds_count')}[$__rate_interval])))",
                            "barrier_inflight_latency_avg",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Barrier Sync Latency",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('stream_barrier_sync_storage_duration_seconds_bucket')}[$__rate_interval])) by (le, {NODE_LABEL}))",
                            f"barrier_sync_latency_p{legend}"
                            + " - {{%s}}" % NODE_LABEL,
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_sync_storage_duration_seconds_sum')}[$__rate_interval]))  / sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_sync_storage_duration_seconds_count')}[$__rate_interval]))",
                            "barrier_sync_latency_avg - {{%s}}" % NODE_LABEL,
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
                    )
                    + [
                        panels.target(
                            f"rate({metric('meta_barrier_wait_commit_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_wait_commit_duration_seconds_count')}[$__rate_interval])",
                            "barrier_wait_commit_avg",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Earliest In-Flight Barrier Progress",
                    "The number of actors that have processed the earliest in-flight barriers per second. "
                    "This metric helps users to detect potential congestion or stuck in the system.",
                    [
                        panels.target(
                            f"rate({metric('stream_barrier_manager_progress')}[$__rate_interval])",
                            "{{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
            ],
        )
    ]


def section_streaming_actors(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming Actors",
            [
                panels.timeseries_percentage(
                    "Actor Output Blocking Time Ratio (Backpressure)",
                    "We first record the total blocking duration(ns) of output buffer of each actor. It shows how "
                    "much time it takes an actor to process a message, i.e. a barrier, a watermark or rows of data, "
                    "on average. Then we divide this duration by 1 second and show it as a percentage.",
                    [
                        panels.target(
                            f"avg(rate({metric('stream_actor_output_buffer_blocking_duration_ns')}[$__rate_interval])) by (fragment_id, downstream_fragment_id) / 1000000000",
                            "fragment {{fragment_id}}->{{downstream_fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Actor Input Blocking Time Ratio",
                    "",
                    [
                        panels.target(
                            f"avg(rate({metric('stream_actor_input_buffer_blocking_duration_ns')}[$__rate_interval])) by (fragment_id, upstream_fragment_id) / 1000000000",
                            "fragment {{fragment_id}}<-{{upstream_fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_row(
                    "Actor Input Rows",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_actor_in_record_cnt')}[$__rate_interval])) by (fragment_id)",
                            "fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_actor_in_record_cnt')}[$__rate_interval])",
                            "actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_row(
                    "Actor Output Rows",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_actor_out_record_cnt')}[$__rate_interval])) by (fragment_id)",
                            "fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_actor_out_record_cnt')}[$__rate_interval])",
                            "actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Executor Cache Memory Usage",
                    "The operator-level memory usage statistics collected by each LRU cache",
                    [
                        panels.target(
                            f"sum({metric('stream_memory_usage')}) by (table_id, desc)",
                            "table {{table_id}} desc: {{desc}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_memory_usage')}",
                            "table {{table_id}} actor {{actor_id}} desc: {{desc}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Executor Cache Memory Usage of Materialized Views",
                    "Memory usage aggregated by materialized views",
                    [
                        panels.target(
                            f"sum({metric('stream_memory_usage')} * on(table_id, actor_id) group_left(materialized_view_id) {metric('table_info')}) by (materialized_view_id)",
                            "materialized view {{materialized_view_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Temporal Join Executor Cache",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_temporal_join_cache_miss_count')}[$__rate_interval])) by (fragment_id)",
                            "temporal join cache miss, table_id {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
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
                            f"sum(rate({table_metric('stream_materialize_cache_hit_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "cache hit count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_materialize_cache_total_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "total cached count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_materialize_cache_hit_count')}[$__rate_interval])",
                            "cache hit count - table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_materialize_cache_total_count')}[$__rate_interval])",
                            "total cached count - table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Over Window Executor Cache",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "cache lookup count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "cache miss count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_over_window_cache_lookup_count')}[$__rate_interval])",
                            "cache lookup count - table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_over_window_cache_miss_count')}[$__rate_interval])",
                            "cache miss count - table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Executor Cache Miss Ratio",
                    "",
                    [
                        panels.target(
                            f"(sum(rate({metric('stream_join_lookup_miss_count')}[$__rate_interval])) by (side, join_table_id, degree_table_id, fragment_id) ) / (sum(rate({metric('stream_join_lookup_total_count')}[$__rate_interval])) by (side, join_table_id, degree_table_id, fragment_id))",
                            "join executor cache miss ratio - - {{side}} side, join_table_id {{join_table_id}} degree_table_id {{degree_table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_agg_lookup_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_agg_lookup_total_count')}[$__rate_interval])) by (table_id, fragment_id))",
                            "Agg cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_agg_distinct_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_agg_distinct_total_cache_count')}[$__rate_interval])) by (table_id, fragment_id))",
                            "Distinct agg cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_group_top_n_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_group_top_n_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id))",
                            "Stream group top n cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_group_top_n_appendonly_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_group_top_n_appendonly_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id))",
                            "Stream group top n appendonly cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_lookup_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_lookup_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id))",
                            "Stream lookup cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_temporal_join_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_temporal_join_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id))",
                            "Stream temporal join cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"1 - (sum(rate({metric('stream_materialize_cache_hit_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_materialize_cache_total_count')}[$__rate_interval])) by (table_id, fragment_id))",
                            "materialize executor cache miss ratio - table {{table_id}} fragment {{fragment_id}}  {{%s}}"
                            % NODE_LABEL,
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_over_window_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_over_window_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id))",
                            "Over window cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency(
                    "Join Executor Barrier Align",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('stream_join_barrier_align_duration_bucket')}[$__rate_interval])) by (le, fragment_id, wait_side, {COMPONENT_LABEL}))",
                                f"p{legend} - fragment {{{{fragment_id}}}} {{{{wait_side}}}} - {{{{{COMPONENT_LABEL}}}}}",
                            ),
                            [90, 99, 999, "max"],
                        ),
                        panels.target(
                            f"sum by(le, fragment_id, wait_side, job)(rate({metric('stream_join_barrier_align_duration_sum')}[$__rate_interval])) / sum by(le,fragment_id,wait_side,{COMPONENT_LABEL}) (rate({metric('stream_join_barrier_align_duration_count')}[$__rate_interval]))",
                            "avg - fragment {{fragment_id}} {{wait_side}} - {{%s}}"
                            % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Join Actor Input Blocking Time Ratio",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_join_actor_input_waiting_duration_ns')}[$__rate_interval]) / 1000000000) by (fragment_id)",
                            "fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_join_actor_input_waiting_duration_ns')}[$__rate_interval]) / 1000000000",
                            "actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Join Actor Match Duration Per Second",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_join_match_duration_ns')}[$__rate_interval]) / 1000000000) by (fragment_id)",
                            "fragment {{fragment_id}} {{side}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_join_match_duration_ns')}[$__rate_interval]) / 1000000000",
                            "actor {{actor_id}} {{side}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Join Cached Keys",
                    "Multiple rows with distinct primary keys may have the same join key. This metric counts the "
                    "number of join keys in the executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_join_cached_entry_count')}) by (fragment_id, side)",
                            "fragment {{fragment_id}} {{side}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_join_cached_entry_count')}",
                            "actor {{actor_id}} {{side}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Join Executor Matched Rows",
                    "The number of matched rows on the opposite side",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('stream_join_matched_join_keys_bucket')}[$__rate_interval])) by (le, fragment_id, table_id, {COMPONENT_LABEL}))",
                                f"p{legend} - fragment {{{{fragment_id}}}} table_id {{{{table_id}}}} - {{{{{COMPONENT_LABEL}}}}}",
                            ),
                            [90, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, job, actor_id, table_id) (rate({metric('stream_join_matched_join_keys_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, fragment_id, table_id) (rate({table_metric('stream_join_matched_join_keys_count')}[$__rate_interval]))",
                            "avg - fragment {{fragment_id}} table_id {{table_id}} - {{%s}}"
                            % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Aggregation Executor Cache Statistics For Each StreamChunk",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_agg_chunk_lookup_miss_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "chunk-level cache miss - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('stream_agg_chunk_lookup_total_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "chunk-level total lookups - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_agg_chunk_lookup_miss_count')}[$__rate_interval])",
                            "chunk-level cache miss  - table {{table_id}} actor {{actor_id}}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_agg_chunk_lookup_total_count')}[$__rate_interval])",
                            "chunk-level total lookups  - table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Aggregation Cached Keys",
                    "The number of keys cached in each hash aggregation executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_agg_cached_entry_count')}) by (table_id, fragment_id)",
                            "stream agg cached keys count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum({metric('stream_agg_distinct_cached_entry_count')}) by (table_id, fragment_id)",
                            "stream agg distinct cached keys count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_agg_cached_entry_count')}",
                            "stream agg cached keys count | table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_agg_distinct_cached_entry_count')}",
                            "stream agg distinct cached keys count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Aggregation Dirty Groups Count",
                    "The number of dirty (unflushed) groups in each hash aggregation executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_agg_dirty_groups_count')}) by (table_id, fragment_id)",
                            "stream agg dirty groups count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_agg_dirty_groups_count')}",
                            "stream agg dirty groups count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Aggregation Dirty Groups Heap Size",
                    "The total heap size of dirty (unflushed) groups in each hash aggregation executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_agg_dirty_groups_heap_size')}) by (table_id, fragment_id)",
                            "stream agg dirty groups heap size | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_agg_dirty_groups_heap_size')}",
                            "stream agg dirty groups heap size | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "TopN Cached Keys",
                    "The number of keys cached in each top_n executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_group_top_n_cached_entry_count')}) by (table_id, fragment_id)",
                            "group top_n cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum({metric('stream_group_top_n_appendonly_cached_entry_count')}) by (table_id, fragment_id)",
                            "group top_n appendonly cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_group_top_n_cached_entry_count')}",
                            "group top_n cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_group_top_n_appendonly_cached_entry_count')}",
                            "group top_n appendonly cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Temporal Join Cache Keys",
                    "The number of keys cached in temporal join executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_temporal_join_cached_entry_count')}) by (table_id, fragment_id)",
                            "Temporal Join cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_temporal_join_cached_entry_count')}",
                            "Temporal Join cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Lookup Cached Keys",
                    "The number of keys cached in lookup executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_lookup_cached_entry_count')}) by (table_id, fragment_id)",
                            "lookup cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_lookup_cached_entry_count')}",
                            "lookup cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Over Window Cached Keys",
                    "The number of keys cached in over window executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_over_window_cached_entry_count')}) by (table_id, fragment_id)",
                            "over window cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_over_window_cached_entry_count')}",
                            "over window cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_rowsps(
                    "Executor Throughput",
                    "When enabled, this metric shows the input throughput of each executor.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_executor_row_count')}[$__rate_interval])) by (executor_identity, fragment_id)",
                            "{{executor_identity}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_executor_row_count')}[$__rate_interval])",
                            "{{executor_identity}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Actor Memory Usage (TaskLocalAlloc)",
                    "The actor-level memory usage statistics reported by TaskLocalAlloc. (Disabled by default)",
                    [
                        panels.target(
                            f"sum({metric('actor_memory_usage')}) by (fragment_id)",
                            "fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('actor_memory_usage')}",
                            "actor {{actor_id}}",
                        ),
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
            ],
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
                        panels.target(
                            f"sum({metric('user_compute_error')}) by (error_type, error_msg, fragment_id, executor_name)",
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
                        panels.target(
                            f"sum({metric('user_source_error')}) by (error_type, error_msg, fragment_id, table_id, executor_name)",
                            "{{error_type}}: {{error_msg}} ({{executor_name}}: table_id={{table_id}}, fragment_id={{fragment_id}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Source Reader Errors by Type",
                    "",
                    [
                        panels.target(
                            f"sum({metric('user_source_reader_error_count')}) by (error_type, error_msg, actor_id, source_id, executor_name)",
                            "{{error_type}}: {{error_msg}} ({{executor_name}}: actor_id={{actor_id}}, source_id={{source_id}})",
                        ),
                        panels.target(
                            f"sum({metric('user_source_reader_error')}) by (error_type, error_msg, actor_id, source_id, executor_name)",
                            "{{error_type}}: {{error_msg}} ({{executor_name}}: actor_id={{actor_id}}, source_id={{source_id}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Sink by Connector",
                    "",
                    [
                        panels.target(
                            f"sum({metric('user_sink_error')}) by (connector_name, executor_id, error_msg)",
                            "{{connector_name}}: {{error_msg}} ({{executor_id}})",
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
                panels.timeseries_bytes(
                    "Mem Table Size",
                    "This metric shows the memory usage of mem_table.",
                    [
                        panels.target(
                            f"sum({metric('state_store_mem_table_memory_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "mem_table size total - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"{metric('state_store_mem_table_memory_size')}",
                            "mem_table size - table id {{table_id}} instance id {{instance_id}} {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Mem Table Count",
                    "This metric shows the item counts in mem_table.",
                    [
                        panels.target(
                            f"sum({metric('state_store_mem_table_item_count')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "mem_table counts total - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"{metric('state_store_mem_table_item_count')}",
                            "mem_table count - table id {{table_id}} instance id {{instance_id}} {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Row SeqScan Next Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('batch_row_seq_scan_next_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"row_seq_scan next p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('batch_row_seq_scan_next_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('batch_row_seq_scan_next_duration_count')}[$__rate_interval]))",
                            "row_seq_scan next avg - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
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
                panels.timeseries_count(
                    "Active Sessions",
                    "Number of active sessions",
                    [
                        panels.target(
                            f"{metric('frontend_active_sessions')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_query_per_sec(
                    "Query Per Second (Local Query Mode)",
                    "",
                    [
                        panels.target(
                            f"rate({metric('frontend_query_counter_local_execution')}[$__rate_interval])",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_query_per_sec(
                    "Query Per Second (Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"rate({metric('distributed_completed_query_counter')}[$__rate_interval])",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "The Number of Running Queries (Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"{metric('distributed_running_query_num')}",
                            "The number of running query in distributed execution mode",
                        ),
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "The Number of Rejected queries (Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"{metric('distributed_rejected_query_counter')}",
                            "The number of rejected query in distributed execution mode",
                        ),
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "The Number of Completed Queries (Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"{metric('distributed_completed_query_counter')}",
                            "The number of completed query in distributed execution mode",
                        ),
                    ],
                    ["last"],
                ),
                panels.timeseries_latency(
                    "Query Latency (Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.5, sum(rate({metric('distributed_query_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "p50 - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"histogram_quantile(0.9, sum(rate({metric('distributed_query_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "p90 - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"histogram_quantile(0.95, sum(rate({metric('distributed_query_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "p99 - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Query Latency (Local Query Mode)",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.5, sum(rate({metric('frontend_latency_local_execution_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "p50 - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"histogram_quantile(0.9, sum(rate({metric('frontend_latency_local_execution_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "p90 - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"histogram_quantile(0.95, sum(rate({metric('frontend_latency_local_execution_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "p99 - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
            ],
        ),
    ]


def section_hummock_read(outer_panels):
    panels = outer_panels.sub_panel()
    meta_miss_filter = "type='meta_miss'"
    meta_total_filter = "type='meta_total'"
    data_miss_filter = "type='data_miss'"
    data_total_filter = "type='data_total'"
    file_cache_get_filter = "op='get'"

    return [
        outer_panels.row_collapsed(
            "Hummock (Read)",
            [
                panels.timeseries_ops(
                    "Cache Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_sst_store_block_request_counts')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, table_id, type)",
                            "{{table_id}} @ {{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('state_store_sst_store_block_request_counts', meta_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, type)",
                            "total_meta_miss_count - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Cache Size",
                    "Hummock has three parts of memory usage: 1. Meta Cache 2. Block Cache"
                    "This metric shows the real memory usage of each of these three caches.",
                    [
                        panels.target(
                            f"avg({metric('state_store_meta_cache_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "meta cache - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"avg({metric('state_store_block_cache_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "data cache - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Cache Miss Rate",
                    "",
                    [
                        panels.target(
                            f"(sum(rate({table_metric('state_store_sst_store_block_request_counts', meta_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)) / (sum(rate({table_metric('state_store_sst_store_block_request_counts', meta_total_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id))",
                            "meta cache miss rate - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"(sum(rate({table_metric('state_store_sst_store_block_request_counts', data_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)) / (sum(rate({table_metric('state_store_sst_store_block_request_counts', data_total_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id))",
                            "block cache miss rate - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Iter keys flow",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_iter_scan_key_counts')}[$__rate_interval])) by ({NODE_LABEL}, type, table_id)",
                            "iter keys flow - {{table_id}} @ {{type}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Read Merged SSTs",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_merge_sstable_counts_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, table_id, type))",
                                f"# merged ssts p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{type}}"
                                % COMPONENT_LABEL,
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_iter_merge_sstable_counts_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_iter_merge_sstable_counts_count')}[$__rate_interval]))",
                            "# merged ssts avg  - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Read Duration - Get",
                    "Histogram of the latency of Get operations that have been issued to the state store.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_get_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_get_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id) (rate({table_metric('state_store_get_duration_count')}[$__rate_interval]))",
                            "avg - {{table_id}} {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
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
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_init_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"create_iter_time p{legend} - {{{{table_id}}}} @ {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('state_store_iter_init_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_iter_init_duration_count')}[$__rate_interval]))",
                            "create_iter_time avg - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_scan_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"pure_scan_time p{legend} - {{{{table_id}}}} @ {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('state_store_iter_scan_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_iter_scan_duration_count')}[$__rate_interval]))",
                            "pure_scan_time avg - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Bloom Filter Ops",
                    "",
                    [
                        panels.target(
                            f"sum(irate({table_metric('state_store_read_req_positive_but_non_exist_counts')}[$__rate_interval])) by (table_id,type)",
                            "bloom filter false positive count  - {{table_id}} - {{type}}",
                        ),
                        panels.target(
                            f"sum(irate({table_metric('state_store_read_req_bloom_filter_positive_counts')}[$__rate_interval])) by (table_id,type)",
                            "bloom filter positive count - {{table_id}} - {{type}}",
                        ),
                        panels.target(
                            f"sum(irate({table_metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by (table_id,type)",
                            "bloom filter check count- {{table_id}} - {{type}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Bloom Filter Positive Rate",
                    "Positive / Total",
                    [
                        panels.target(
                            f"(sum(rate({table_metric('state_store_read_req_bloom_filter_positive_counts')}[$__rate_interval])) by (table_id,type)) / (sum(rate({table_metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by (table_id,type))",
                            "bloom filter positive rate - {{table_id}} - {{type}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Bloom Filter False-Positive Rate",
                    "False-Positive / Total",
                    [
                        panels.target(
                            f"(((sum(rate({table_metric('state_store_read_req_positive_but_non_exist_counts')}[$__rate_interval])) by (table_id,type))) / (sum(rate({table_metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by (table_id,type)))",
                            "read req bloom filter false positive rate - {{table_id}} - {{type}}",
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
                panels.timeseries_ops(
                    "Read Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_get_duration_count')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "get - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({table_metric('state_store_get_shared_buffer_hit_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "shared_buffer hit - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({table_metric('state_store_iter_in_process_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "iter - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Read Item Size - Get",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_get_key_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)) + histogram_quantile({quantile}, sum(rate({table_metric('state_store_get_value_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"p{legend} - {{{{table_id}}}} {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Read Item Size - Iter",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"p{legend} - {{{{table_id}}}} @ {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Materialized View Read Size",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f'sum(histogram_quantile({quantile}, sum(rate({metric("state_store_iter_size_bucket")}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)) * on(table_id) group_left(materialized_view_id) (group({metric("table_info")}) by (materialized_view_id, table_id))) by (materialized_view_id) + sum((histogram_quantile({quantile}, sum(rate({metric("state_store_get_key_size_bucket")}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)) + histogram_quantile({quantile}, sum(rate({metric("state_store_get_value_size_bucket")}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))) * on(table_id) group_left(materialized_view_id) (group({metric("table_info")}) by (materialized_view_id, table_id))) by (materialized_view_id)',
                                f"read p{legend} - materialized view {{{{materialized_view_id}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Read Item Count - Iter",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_item_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"p{legend} - {{{{table_id}}}} @ {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
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
                            f"sum(rate({metric('state_store_get_key_size_sum')}[$__rate_interval])) by({COMPONENT_LABEL}, {NODE_LABEL}) + sum(rate({metric('state_store_get_value_size_sum')}[$__rate_interval])) by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Read Throughput - Iter",
                    "The size of all the key-value paris when reading by operation Iter."
                    "Operation Iter scans a range of key-value pairs.",
                    [
                        panels.target(
                            f"sum(rate({metric('state_store_iter_size_sum')}[$__rate_interval])) by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Fetch Meta Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_fetch_meta_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"fetch_meta_duration p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id) (rate({table_metric('state_store_iter_fetch_meta_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id) (rate({table_metric('state_store_iter_fetch_meta_duration_count')}[$__rate_interval]))",
                            "fetch_meta_duration avg - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
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
            ],
        )
    ]


def section_hummock_write(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Hummock (Write)",
            [
                panels.timeseries_bytes(
                    "Uploader Memory Size",
                    "This metric shows the real memory usage of uploader.",
                    [
                        panels.target(
                            f"sum({metric('uploading_memory_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "uploading memory - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('state_store_uploader_uploading_task_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "uploading task size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Build and Sync Sstable Duration",
                    "Histogram of time spent on compacting shared buffer to remote storage.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_sync_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_sync_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_sync_duration_count')}[$__rate_interval]))",
                            "avg - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Materialized View Write Size",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f'sum(histogram_quantile({quantile}, sum(rate({metric("state_store_write_batch_size_bucket")}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)) * on(table_id) group_left(materialized_view_id) (group({metric("table_info")}) by (materialized_view_id, table_id))) by (materialized_view_id, table_name)',
                                f"write p{legend} - materialized view {{{{materialized_view_id}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Uploader - Tasks Count",
                    "",
                    [
                        panels.target(
                            f"sum(irate({table_metric('state_store_merge_imm_task_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "merge imm tasks - {{table_id}} @ {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(irate({metric('state_store_spill_task_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},uploader_stage)",
                            "Uploader spill tasks - {{uploader_stage}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Uploader - Task Size",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_merge_imm_memory_sz')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "Merging tasks memory size - {{table_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('state_store_spill_task_size')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},uploader_stage)",
                            "Uploading tasks size - {{uploader_stage}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Write Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_write_batch_duration_count')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "write batch - {{table_id}} @ {{%s}} @ {{%s}} "
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('state_store_sync_duration_count')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "l0 - {{%s}} @ {{%s}} " % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Write Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_write_batch_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"write to shared_buffer p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_write_batch_duration_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_write_batch_duration_count')}[$__rate_interval]))",
                            "write to shared_buffer avg - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_write_shared_buffer_sync_time_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"write to object_store p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('state_store_write_shared_buffer_sync_time_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('state_store_write_shared_buffer_sync_time_count')}[$__rate_interval]))",
                            "write to object_store - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Write Item Count",
                    "",
                    [
                        panels.target(
                            f"sum(irate({table_metric('state_store_write_batch_tuple_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "write_batch_kv_pair_count - {{table_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Write Throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_write_batch_size_sum')}[$__rate_interval]))by({COMPONENT_LABEL},{NODE_LABEL},table_id) / sum(rate({table_metric('state_store_write_batch_size_count')}[$__rate_interval]))by({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "shared_buffer - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('compactor_shared_buffer_to_sstable_size_sum')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL}) / sum(rate({metric('compactor_shared_buffer_to_sstable_size_count')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "sync - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Write Batch Size",
                    "This metric shows the statistics of mem_table size on flush. By default only max (p100) is shown.",
                    [
                        panels.target(
                            f"histogram_quantile(1.0, sum(rate({metric('state_store_write_batch_size_bucket')}[$__rate_interval])) by (le, table_id, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "pmax - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_write_batch_size_sum')}[$__rate_interval])) / sum by(le, table_id, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_write_batch_size_count')}[$__rate_interval]))",
                            "avg - {{table_id}} {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Checkpoint Sync Size",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_sync_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_sync_size_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_sync_size_count')}[$__rate_interval]))",
                            "avg - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
            ],
        )
    ]


def section_hummock_tiered_cache(outer_panels):
    panels = outer_panels.sub_panel()
    file_cache_hit_filter = 'op="lookup",extra="hit"'
    file_cache_miss_filter = 'op="lookup",extra="miss"'
    return [
        outer_panels.row_collapsed(
            "Hummock Tiered Cache",
            [
                panels.timeseries_ops(
                    "Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_storage_op_duration_count')}[$__rate_interval])) by (foyer, op, extra, {NODE_LABEL})",
                            "{{foyer}} file cache {{op}} {{extra}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('foyer_storage_op_duration_bucket')}[$__rate_interval])) by (le, foyer, op, extra, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{foyer}} file cache - {{op}} {{extra}} @ {{%s}}"
                                % NODE_LABEL,
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_storage_op_bytes')}[$__rate_interval])) by (foyer, op, extra, {NODE_LABEL})",
                            "{{foyer}} file cache - {{op}} {{extra}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Size",
                    "",
                    [
                        panels.target(
                            f"sum({metric('foyer_storage_total_bytes')}) by (foyer, {NODE_LABEL})",
                            "{{foyer}} size @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Cache Hit Ratio",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_storage_op_duration_count', file_cache_hit_filter)}[$__rate_interval])) by (foyer, {NODE_LABEL}) / (sum(rate({metric('foyer_storage_op_duration_count', file_cache_hit_filter)}[$__rate_interval])) by (foyer, {NODE_LABEL}) + sum(rate({metric('foyer_storage_op_duration_count', file_cache_miss_filter)}[$__rate_interval])) by (foyer, {NODE_LABEL}))",
                            "{{foyer}} file cache hit ratio @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Refill Queue Length",
                    "",
                    [
                        panels.target(
                            f"sum(refill_queue_total) by ({NODE_LABEL})",
                            "refill queue length @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Refill Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_duration_count')}[$__rate_interval])) by (type, op, {NODE_LABEL})",
                            "{{type}} file cache refill - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('refill_total')}[$__rate_interval])) by (type, op, {NODE_LABEL})",
                            "{{type}} file cache refill - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Refill Latency",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('refill_duration_bucket')}[$__rate_interval])) by (le, type, op, {NODE_LABEL}))",
                                f"p{legend} - "
                                + "{{type}} file cache refill - {{op}} @ {{%s}}"
                                % NODE_LABEL,
                            ),
                            [50, 99, "max"],
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
    mv_total_size_filter = "metric='materialized_view_total_size'"
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
                                f"Lock Time p{legend}"
                                + " - {{lock_type}} @ {{lock_name}}",
                            ),
                            [50, 99, "max"],
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
                                f"Real Process Time p{legend}" + " - {{method}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Version Size",
                    "",
                    [
                        panels.target(
                            f"{metric('storage_version_size')}", "version size"
                        ),
                    ],
                ),
                panels.timeseries_id(
                    "Version Id",
                    "",
                    [
                        panels.target(
                            f"{metric('storage_current_version_id')}",
                            "current version id",
                        ),
                        panels.target(
                            f"{metric('storage_checkpoint_version_id')}",
                            "checkpoint version id",
                        ),
                        panels.target(
                            f"{metric('storage_min_pinned_version_id')}",
                            "min pinned version id",
                        ),
                        panels.target(
                            f"{metric('storage_min_safepoint_version_id')}",
                            "min safepoint version id",
                        ),
                    ],
                ),
                panels.timeseries_id(
                    "Epoch",
                    "",
                    [
                        panels.target(
                            f"{metric('storage_max_committed_epoch')}",
                            "max committed epoch",
                        ),
                        panels.target(f"{metric('storage_safe_epoch')}", "safe epoch"),
                        panels.target(
                            f"{metric('storage_min_pinned_epoch')}", "min pinned epoch"
                        ),
                    ],
                ),
                panels.timeseries_kilobytes(
                    "Table Size",
                    "",
                    [
                        panels.target(
                            f"{table_metric('storage_version_stats', total_key_size_filter)}/1024",
                            "table{{table_id}} {{metric}}",
                        ),
                        panels.target(
                            f"{table_metric('storage_version_stats', total_value_size_filter)}/1024",
                            "table{{table_id}} {{metric}}",
                        ),
                    ],
                ),
                panels.timeseries_kilobytes(
                    "Materialized View Size",
                    "",
                    [
                        panels.target(
                            f"{table_metric('storage_materialized_view_stats', mv_total_size_filter)}/1024",
                            "{{metric}}, mv id - {{table_id}} ",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Table KV Count",
                    "",
                    [
                        panels.target(
                            f"{table_metric('storage_version_stats', total_key_count_filter)}",
                            "table{{table_id}} {{metric}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Object Total Number",
                    """
Objects are classified into 3 groups:
- not referenced by versions: these object are being deleted from object store.
- referenced by non-current versions: these objects are stale (not in the latest version), but those old versions may still be in use (e.g. long-running pinning). Thus those objects cannot be deleted at the moment.
- referenced by current version: these objects are in the latest version.

Additionally, a metric on all objects (including dangling ones) is updated with low-frequency. The metric is updated right before full GC. So subsequent full GC may reduce the actual value significantly, without updating the metric.
                    """,
                    [
                        panels.target(
                            f"{metric('storage_stale_object_count')}",
                            "not referenced by versions",
                        ),
                        panels.target(
                            f"{metric('storage_old_version_object_count')}",
                            "referenced by non-current versions",
                        ),
                        panels.target(
                            f"{metric('storage_current_version_object_count')}",
                            "referenced by current version",
                        ),
                        panels.target(
                            f"{metric('storage_total_object_count')}",
                            "all objects (including dangling ones)",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Object Total Size",
                    "Refer to `Object Total Number` panel for classification of objects.",
                    [
                        panels.target(
                            f"{metric('storage_stale_object_size')}",
                            "not referenced by versions",
                        ),
                        panels.target(
                            f"{metric('storage_old_version_object_size')}",
                            "referenced by non-current versions",
                        ),
                        panels.target(
                            f"{metric('storage_current_version_object_size')}",
                            "referenced by current version",
                        ),
                        panels.target(
                            f"{metric('storage_total_object_size')}",
                            "all objects, including dangling ones",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Delta Log Total Number",
                    "total number of hummock version delta log",
                    [
                        panels.target(
                            f"{metric('storage_delta_log_count')}",
                            "delta log total number",
                        ),
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
                    )
                    + [
                        panels.target(
                            f"rate({metric('storage_version_checkpoint_latency_sum')}[$__rate_interval]) / rate({metric('storage_version_checkpoint_latency_count')}[$__rate_interval])",
                            "version_checkpoint_latency_avg",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Write Stop Compaction Groups",
                    "When certain per compaction group threshold is exceeded (e.g. number of level 0 sub-level in LSMtree), write op to that compaction group is stopped temporarily. Check log for detail reason of write stop.",
                    [
                        panels.target(
                            f"{metric('storage_write_stop_compaction_groups')}",
                            "compaction_group_{{compaction_group_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Full GC Trigger Count",
                    "total number of attempts to trigger full GC",
                    [
                        panels.target(
                            f"{metric('storage_full_gc_trigger_count')}",
                            "full_gc_trigger_count",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Full GC Last Watermark",
                    "the object id watermark used in last full GC",
                    [
                        panels.target(
                            f"{metric('storage_full_gc_last_object_id_watermark')}",
                            "full_gc_last_object_id_watermark",
                        ),
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Compaction Event Loop Time",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('storage_compaction_event_consumed_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"meta consumed latency p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('storage_compaction_event_loop_iteration_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"meta iteration latency p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('compactor_compaction_event_consumed_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"compactor consumed latency p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('compactor_compaction_event_loop_iteration_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"compactor iteration latency p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Move State Table Count",
                    "The times of move_state_table occurs",
                    [
                        panels.target(
                            f"sum({table_metric('storage_move_state_table_count')}) by (group)",
                            "move table cg{{group}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "State Table Count",
                    "The number of state_tables in each CG",
                    [
                        panels.target(
                            f"sum(irate({table_metric('storage_state_table_count')}[$__rate_interval])) by (group)",
                            "state table cg{{group}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Branched SST Count",
                    "The number of branched_sst in each CG",
                    [
                        panels.target(
                            f"sum(irate({table_metric('storage_branched_sst_count')}[$__rate_interval])) by (group)",
                            "branched sst cg{{group}}",
                        ),
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
                                f"Job Process Time p{legend}" + " - {{state}}",
                            ),
                            [50, 99, "max"],
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
                    panels, "Create", "path='/meta.CatalogService/Create'"
                ),
                grpc_metrics_target(panels, "Drop", "path='/meta.CatalogService/Drop'"),
                grpc_metrics_target(
                    panels, "GetCatalog", "path='/meta.CatalogService/GetCatalog'"
                ),
            ],
        )
    ]


def section_grpc_meta_cluster_service(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "gRPC Meta: Cluster Service",
            [
                grpc_metrics_target(
                    panels, "AddWorkerNode", "path='/meta.ClusterService/AddWorkerNode'"
                ),
                grpc_metrics_target(
                    panels, "ListAllNodes", "path='/meta.ClusterService/ListAllNodes'"
                ),
            ],
        ),
    ]


def section_grpc_meta_stream_manager(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "gRPC Meta: Stream Manager",
            [
                grpc_metrics_target(
                    panels,
                    "CreateMaterializedView",
                    "path='/meta.StreamManagerService/CreateMaterializedView'",
                ),
                grpc_metrics_target(
                    panels,
                    "DropMaterializedView",
                    "path='/meta.StreamManagerService/DropMaterializedView'",
                ),
                grpc_metrics_target(
                    panels, "Flush", "path='/meta.StreamManagerService/Flush'"
                ),
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
                    panels,
                    "UnpinVersionBefore",
                    "path='/meta.HummockManagerService/UnpinVersionBefore'",
                ),
                grpc_metrics_target(
                    panels,
                    "UnpinSnapshotBefore",
                    "path='/meta.HummockManagerService/UnpinSnapshotBefore'",
                ),
                grpc_metrics_target(
                    panels,
                    "ReportCompactionTasks",
                    "path='/meta.HummockManagerService/ReportCompactionTasks'",
                ),
                grpc_metrics_target(
                    panels,
                    "GetNewSstIds",
                    "path='/meta.HummockManagerService/GetNewSstIds'",
                ),
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
                            f"sum(irate({metric('state_store_report_compaction_task_counts')}[$__rate_interval])) by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "report_compaction_task_counts - {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "version_latency",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.5, sum(irate({metric('state_store_unpin_version_before_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "unpin_version_before_latency_p50 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(irate({metric('state_store_unpin_version_before_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "unpin_version_before_latency_p99 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(irate({metric('state_store_unpin_version_before_latency_sum')}[$__rate_interval])) / sum(irate({metric('state_store_unpin_version_before_latency_count')}[$__rate_interval]))",
                            "unpin_version_before_latency_avg",
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(irate({metric('state_store_unpin_version_before_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "unpin_version_before_latency_p90 - {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "snapshot_latency",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.5, sum(irate({metric('state_store_pin_snapshot_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "pin_snapshot_latency_p50 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(irate({metric('state_store_pin_snapshot_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "pin_snapshot_latency_p99 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"histogram_quantile(0.9, sum(irate({metric('state_store_pin_snapshot_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "pin_snapshot_latencyp90 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(irate({metric('state_store_pin_snapshot_latency_sum')}[$__rate_interval])) / sum(irate(state_store_pin_snapshot_latency_count[$__rate_interval]))",
                            "pin_snapshot_latency_avg",
                        ),
                        panels.target(
                            f"histogram_quantile(0.5, sum(irate({metric('state_store_unpin_version_snapshot_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "unpin_snapshot_latency_p50 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(irate({metric('state_store_unpin_version_snapshot_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "unpin_snapshot_latency_p99 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(irate({metric('state_store_unpin_snapshot_latency_sum')}[$__rate_interval])) / sum(irate(state_store_unpin_snapshot_latency_count[$__rate_interval]))",
                            "unpin_snapshot_latency_avg",
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(irate({metric('state_store_unpin_snapshot_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "unpin_snapshot_latency_p90 - {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "snapshot_count",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('state_store_pin_snapshot_counts')}[$__rate_interval])) by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "pin_snapshot_counts - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(irate({metric('state_store_unpin_snapshot_counts')}[$__rate_interval])) by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "unpin_snapshot_counts - {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "table_latency",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.5, sum(irate({metric('state_store_get_new_sst_ids_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "get_new_sst_ids_latency_latency_p50 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(irate({metric('state_store_get_new_sst_ids_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "get_new_sst_ids_latency_latency_p99 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(irate({metric('state_store_get_new_sst_ids_latency_sum')}[$__rate_interval])) / sum(irate({metric('state_store_get_new_sst_ids_latency_count')}[$__rate_interval]))",
                            "get_new_sst_ids_latency_latency_avg",
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(irate({metric('state_store_get_new_sst_ids_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "get_new_sst_ids_latency_latency_p90 - {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "table_count",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('state_store_get_new_sst_ids_latency_counts')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "get_new_sst_ids_latency_counts - {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "compaction_latency",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.5, sum(irate({metric('state_store_report_compaction_task_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "report_compaction_task_latency_p50 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(irate({metric('state_store_report_compaction_task_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "report_compaction_task_latency_p99 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(irate({metric('state_store_report_compaction_task_latency_sum')}[$__rate_interval])) / sum(irate(state_store_report_compaction_task_latency_count[$__rate_interval]))",
                            "report_compaction_task_latency_avg",
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(irate({metric('state_store_report_compaction_task_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "report_compaction_task_latency_p90 - {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
            ],
        ),
    ]


def section_kafka_native_metrics(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Kafka Native Metrics",
            [
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
                panels.timeseries_memory(
                    "The allocated memory of jvm",
                    "",
                    [
                        panels.target(
                            f"{metric('jvm_allocated_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The active memory of jvm",
                    "",
                    [
                        panels.target(
                            f"{metric('jvm_active_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_ms(
                    "LRU manager diff between current watermark and evicted watermark time (ms) for actors",
                    "",
                    [
                        panels.target(
                            f"{metric('lru_current_watermark_time_ms')} - on() group_right() {metric('lru_evicted_watermark_time_ms')}",
                            "table {{table_id}} actor {{actor_id}} desc: {{desc}}",
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
                            f"rate({metric('connector_source_rows_received')}[$__rate_interval])",
                            "source={{source_type}} @ {{source_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Connector Sink Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"rate({metric('connector_sink_rows_received')}[$__rate_interval])",
                            "sink={{connector_type}} @ {{sink_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_sink_metrics(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Sink Metrics",
            [
                panels.timeseries_latency(
                    "Commit Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('sink_commit_duration_bucket')}[$__rate_interval])) by (le, connector, sink_id))",
                                f"p{legend}" + " @ {{connector}} {{sink_id}}",
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, connector, sink_id)(rate({metric('sink_commit_duration_sum')}[$__rate_interval])) / sum by(le, type, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('sink_commit_duration_count')}[$__rate_interval]))",
                            "avg - {{connector}} @ {{sink_id}}",
                        ),
                    ],
                ),
                panels.timeseries_id(
                    "Log Store Read/Write Epoch",
                    "",
                    [
                        panels.target(
                            f"{metric('log_store_latest_write_epoch')}",
                            "latest write epoch @ {{connector}} {{sink_id}} {{executor_id}}",
                        ),
                        panels.target(
                            f"{metric('log_store_latest_read_epoch')}",
                            "latest read epoch @ {{connector}} {{sink_id}} {{executor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Log Store Lag",
                    "",
                    [
                        panels.target(
                            f"(max({metric('log_store_latest_write_epoch')}) by (connector, sink_id, executor_id)"
                            + f"- max({metric('log_store_latest_read_epoch')}) by (connector, sink_id, executor_id)) / (2^16) / 1000",
                            "Consume lag @ {{connector}} {{sink_id}} {{executor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Log Store Consume Persistent Log Lag",
                    "",
                    [
                        panels.target(
                            f"clamp_min((max({metric('log_store_first_write_epoch')}) by (connector, sink_id, executor_id)"
                            + f"- max({metric('log_store_latest_read_epoch')}) by (connector, sink_id, executor_id)) / (2^16) / 1000, 0)",
                            "Consume persistent log lag @ {{connector}} {{sink_id}} {{executor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Log Store Consume Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('log_store_read_rows')}[$__rate_interval])) by (connector, sink_id)",
                            "sink={{connector}} {{sink_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Executor Log Store Consume Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('log_store_read_rows')}[$__rate_interval])) by ({NODE_LABEL}, connector, sink_id, executor_id)",
                            "sink={{connector}} {{sink_id}} @ {{executor_id}} {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Log Store Write Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('log_store_write_rows')}[$__rate_interval])) by (connector, sink_id)",
                            "sink={{connector}} {{sink_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Executor Log Store Write Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('log_store_write_rows')}[$__rate_interval])) by ({NODE_LABEL}, connector, sink_id, executor_id)",
                            "sink={{connector}} {{sink_id}} @ {{executor_id}} {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Kv Log Store Read Storage Row Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('kv_log_store_storage_read_count')}[$__rate_interval])) by (executor_id, connector, sink_id)",
                            "{{executor_id}} - {{connector}} @ {{sink_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Kv Log Store Read Storage Size",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('kv_log_store_storage_read_size')}[$__rate_interval])) by (executor_id, connector, sink_id)",
                            "{{executor_id}} - {{connector}} @ {{sink_id}}",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Kv Log Store Write Storage Row Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('kv_log_store_storage_write_count')}[$__rate_interval])) by (executor_id, connector, sink_id)",
                            "{{executor_id}} - {{connector}} @ {{sink_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Kv Log Store Write Storage Size",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('kv_log_store_storage_write_size')}[$__rate_interval])) by (executor_id, connector, sink_id)",
                            "{{executor_id}} - {{connector}} @ {{sink_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_network_connection(outer_panels):
    panels = outer_panels.sub_panel()
    s3_filter = 'connection_type="S3"'
    grpc_filter = 'connection_type=~"grpc.*"'
    return [
        outer_panels.row_collapsed(
            "Network connection",
            [
                panels.timeseries_bytesps(
                    "Network throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('connection_read_rate')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} read @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_write_rate')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} write @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "S3 throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('connection_read_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} read @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_write_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} write @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "gRPC throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('connection_read_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type) / (1024*1024)",
                            "{{%s}} {{connection_type}} read @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_write_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type) / (1024*1024)",
                            "{{%s}} {{connection_type}} write @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_read_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} total read @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_write_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} total write @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "IO error rate",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('connection_io_err_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, op_type, error_kind)",
                            "{{%s}} S3 {{op_type}} err[{{error_kind}}] @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_io_err_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, op_type, error_kind)",
                            "{{%s}} grpc {{op_type}} err[{{error_kind}}] @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_io_err_rate')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, op_type, error_kind)",
                            "{{%s}} total {{op_type}} err[{{error_kind}}] @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Existing connection count",
                    "",
                    [
                        panels.target(
                            f"sum({metric('connection_count', filter=s3_filter)}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} S3 @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('connection_count', filter=grpc_filter)}) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type)",
                            "{{%s}} {{connection_type}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Create new connection rate",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('connection_create_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} S3 @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(irate({metric('connection_create_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type)",
                            "{{%s}} {{connection_type}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Create new connection err rate",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('connection_err_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} S3 @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(irate({metric('connection_err_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type)",
                            "{{%s}} {{connection_type}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
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
            "type": "datasource",
        }
    )

if namespace_filter_enabled:
    namespace_json = {
        "definition": 'label_values(up{risingwave_name=~".+"}, namespace)',
        "description": "Kubernetes namespace.",
        "hide": 0,
        "includeAll": False,
        "label": "Namespace",
        "multi": False,
        "name": "namespace",
        "options": [],
        "query": {
            "query": 'label_values(up{risingwave_name=~".+"}, namespace)',
            "refId": "StandardVariableQuery",
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 0,
        "type": "query",
    }

    name_json = {
        "current": {"selected": False, "text": "risingwave", "value": "risingwave"},
        "definition": 'label_values(up{namespace="$namespace", risingwave_name=~".+"}, risingwave_name)',
        "hide": 0,
        "includeAll": False,
        "label": "RisingWave",
        "multi": False,
        "name": "instance",
        "options": [],
        "query": {
            "query": 'label_values(up{namespace="$namespace", risingwave_name=~".+"}, risingwave_name)',
            "refId": "StandardVariableQuery",
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
    "current": {"selected": False, "text": "All", "value": "__all"},
    "definition": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {NODE_LABEL})",
    "description": "Reporting instance of the metric",
    "hide": 0,
    "includeAll": True,
    "label": f"{NODE_VARIABLE_LABEL}",
    "multi": True,
    "name": f"{NODE_VARIABLE}",
    "options": [],
    "query": {
        "query": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {NODE_LABEL})",
        "refId": "StandardVariableQuery",
    },
    "refresh": 2,
    "regex": "",
    "skipUrlSync": False,
    "sort": 6,
    "type": "query",
}

job_json = {
    "current": {"selected": False, "text": "All", "value": "__all"},
    "definition": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {COMPONENT_LABEL})",
    "description": "Reporting job of the metric",
    "hide": 0,
    "includeAll": True,
    "label": f"{COMPONENT_VARIABLE_LABEL}",
    "multi": True,
    "name": f"{COMPONENT_VARIABLE}",
    "options": [],
    "query": {
        "query": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {COMPONENT_LABEL})",
        "refId": "StandardVariableQuery",
    },
    "refresh": 2,
    "regex": "",
    "skipUrlSync": False,
    "sort": 6,
    "type": "query",
}

table_id_json = {
    "current": {"selected": False, "text": "All", "value": "__all"},
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
        "refId": "StandardVariableQuery",
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
        *section_hummock_read(panels),
        *section_hummock_write(panels),
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
        *section_sink_metrics(panels),
        *section_kafka_native_metrics(panels),
        *section_network_connection(panels),
    ],
).auto_panel_ids()
