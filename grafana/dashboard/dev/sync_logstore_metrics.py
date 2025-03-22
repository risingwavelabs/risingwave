from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Sync Log Store Metrics",
            [
                panels.timeseries_epoch(
                    "Log Store Read/Write Epoch",
                    "",
                    [
                        panels.target(
                            f"{metric('sync_log_store_latest_write_epoch')}",
                            "latest write epoch @ {{fragment_id}} {{relation}} @ actor {{actor_id}}",
                        ),
                        panels.target(
                            f"{metric('sync_log_store_latest_read_epoch')}",
                            "latest read epoch @ {{fragment_id}} {{relation}} ({{target}}) actor {{actor_id}}",
                        ),
                        panels.target(
                            f"{metric('sync_kv_log_store_buffer_unconsumed_min_epoch')}",
                            "Sync Kv log store unconsumed min epoch @ {{fragment_id}} {{relation}} ({{target}}) actor {{actor_id}}",
                        ),
                    ],
                ),
                # FIXME(kwannoel): These are missing metrics
                panels.timeseries_latency(
                    "Log Store Lag",
                    "",
                    [
                        panels.target(
                            f"(max({metric('log_store_latest_write_epoch')}) by (fragment_id, actor_id, relation)"
                            + f"- max({metric('log_store_latest_read_epoch')}) by (fragment_id, actor_id, relation)) / (2^16) / 1000",
                            "{{fragment_id}} {{relation}} @ actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Log Store Backpressure Ratio",
                    "",
                    [
                        panels.target(
                            f"avg(rate({metric('sync_kv_log_store_wait_next_poll_ns')}[$__rate_interval])) by (target, fragment_id, actor_id, relation) / 1000000000",
                            "Backpressure @ {{fragment_id}} {{relation}} ({{target}}) actor {{actor_id}}",
                        ),
                    ],
                ),
                # FIXME(kwannoel): These are missing metrics
                panels.timeseries_latency(
                    "Log Store Consume Persistent Log Lag",
                    "",
                    [
                        panels.target(
                            f"clamp_min((max({metric('log_store_first_write_epoch')}) by (fragment_id, actor_id, relation)"
                            + f"- max({metric('log_store_latest_read_epoch')}) by (fragment_id, actor_id, relation)) / (2^16) / 1000, 0)",
                            "{{fragment_id}} {{relation}} @ actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Log Store Consume Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_total_read_count')}[$__rate_interval])) by (target, fragment_id, relation)",
                            "total {{fragment_id}} {{relation}} ({{target}})",
                        ),
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_storage_read_count')}[$__rate_interval])) by (read_type, actor_id, target, fragment_id, relation)",
                            "{{read_type}} {{fragment_id}} {{relation}} actor {{actor_id}} ({{target}})",
                        ),
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_buffer_read_count')}[$__rate_interval])) by (actor_id, target, fragment_id, relation)",
                            "buffer {{fragment_id}} {{relation}} actor {{actor_id}} ({{target}})",
                        )
                    ],
                ),
                panels.timeseries_rowsps(
                    "Executor Log Store Consume Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_total_read_count')}[$__rate_interval])) by ({NODE_LABEL}, target, fragment_id, actor_id, relation)",
                            "{{fragment_id}} {{relation}} ({{target}}) actor {{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "Log Store Consume Throughput(MB/s)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_total_read_size')}[$__rate_interval])) by (target, fragment_id, relation) / (1000*1000)",
                            "{{fragment_id}} {{relation}} ({{target}})",
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "Executor Log Store Consume Throughput(MB/s)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_total_read_size')}[$__rate_interval])) by ({NODE_LABEL}, target, fragment_id, actor_id, relation) / (1000*1000)",
                            "{{fragment_id}} {{relation}} ({{target}}) actor {{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Log Store Write Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_storage_write_count')}[$__rate_interval])) by (fragment_id, relation)",
                            "{{fragment_id}} {{relation}}",
                        ),
                        panels.target_hidden(
                            f"sum(rate({metric('sync_kv_log_store_storage_write_count')}[$__rate_interval])) by ({NODE_LABEL}, fragment_id, actor_id, relation)",
                            "{{fragment_id}} {{relation}} @ actor {{actor_id}} {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Kv Log Store Buffer State (0-clean, 1-unclean)",
                    "",
                    [
                        panels.target(
                            f"sum({metric('sync_kv_log_store_unclean_state')}) by (fragment_id, relation)",
                            "unclean {{fragment_id}} {{relation}}",
                        ),
                        panels.target(
                            f"sum({metric('sync_kv_log_store_clean_state')}) by (fragment_id, relation)",
                            "clean {{fragment_id}} {{relation}}",
                        ),
                        panels.target(
                            f"sum({metric('sync_kv_log_store_unclean_state')}) by (fragment_id, relation) - sum({metric('sync_kv_log_store_clean_state')}) by (fragment_id, relation)",
                            "current state {{fragment_id}} {{relation}}",
                        ),
                    ]
                ),
                panels.timeseries_ops(
                    "Kv Log Store Read Storage Row Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_storage_read_count')}[$__rate_interval])) by (actor_id, target, fragment_id, relation)",
                            "{{fragment_id}} {{relation}} actor {{actor_id}} ({{target}})",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Kv Log Store Read Storage Size",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_storage_read_size')}[$__rate_interval])) by (actor_id, target, fragment_id, relation)",
                            "{{fragment_id}} {{relation}} actor {{actor_id}} ({{target}})",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Kv Log Store Write Storage Row Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_storage_write_count')}[$__rate_interval])) by (actor_id, target, fragment_id, relation)",
                            "{{fragment_id}} {{relation}} actor {{actor_id}} ({{target}})",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Kv Log Store Write Storage Size",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('sync_kv_log_store_storage_write_size')}[$__rate_interval])) by (actor_id, target, fragment_id, relation)",
                            "{{fragment_id}} {{relation}} actor {{actor_id}} ({{target}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Kv Log Store Buffer State",
                    "",
                    [
                        panels.target(
                            f"{metric('sync_kv_log_store_buffer_unconsumed_item_count')}",
                            "Unconsumed item count @ {{fragment_id}} {{relation}} ({{target}}) actor {{actor_id}}",
                        ),
                        panels.target(
                            f"{metric('sync_kv_log_store_buffer_unconsumed_row_count')}",
                            "Unconsumed row count @ {{fragment_id}} {{relation}} ({{target}}) actor {{actor_id}}",
                        ),
                        panels.target(
                            f"{metric('sync_kv_log_store_buffer_unconsumed_epoch_count')}",
                            "Unconsumed epoch count @ {{fragment_id}} {{relation}} ({{target}}) actor {{actor_id}}",
                        ),
                    ],
                ),
                # FIXME(kwannoel): These are missing metrics
                panels.timeseries_bytes(
                    "Chunk Buffer Size",
                    "Total size of chunks buffered in a barrier",
                    [
                        panels.target(
                            f"sum({metric('stream_sink_chunk_buffer_size')}) by (fragment_id, actor_id, relation) * on(actor_id) group_left(relation) {metric('sink_info')}",
                            "sink {{fragment_id}} {{relation}} - actor {{actor_id}}",
                        ),
                    ],
                ),
                # pause duration
                panels.timeseries_latency(
                    "Log Store Pause Duration",
                    "",
                    [
                        panels.target(
                            f"max({metric('sync_kv_log_store_write_pause_duration_ns')}) by (fragment_id, relation) / 1000000",
                            "{{fragment_id}} {{relation}}",
                        ),
                    ],
                ),
                # duration we have to wait before each poll
                panels.timeseries_latency(
                    "Log Store Wait Next Poll Duration",
                    "",
                    [
                        panels.target(
                            f"max({metric('sync_kv_log_store_wait_next_poll_ns')}) by (fragment_id, relation) / 1000000",
                            "{{fragment_id}} {{relation}}",
                        ),
                    ]
                )
            ],
        )
    ]
