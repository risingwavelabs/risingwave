from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Sink Metrics",
            [
                panels.timeseries_rowsps(
                    "Remote Sink (Java) Throughput",
                    "The rows sent by remote sink to the Java connector process",
                    [
                        panels.target(
                            f"sum(rate({metric('connector_sink_rows_received')}[$__rate_interval])) by (sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Commit Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('sink_commit_duration_bucket')}[$__rate_interval])) by (le, connector, sink_id, sink_name))",
                                f"p{legend}"
                                + " @ {{sink_id}} {{sink_name}} ({{connector}})",
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, type, {COMPONENT_LABEL}, {NODE_LABEL}, sink_id, sink_name)(rate({metric('sink_commit_duration_sum')}[$__rate_interval])) / sum by(le, type, {COMPONENT_LABEL}, {NODE_LABEL}, sink_id, sink_name) (rate({metric('sink_commit_duration_count')}[$__rate_interval])) > 0",
                            "avg @ {{sink_id}} {{sink_name}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_epoch(
                    "Log Store Read/Write Epoch",
                    "",
                    [
                        panels.target(
                            f"{metric('log_store_latest_write_epoch')}",
                            "latest write epoch @ {{sink_id}} {{sink_name}} @ actor {{actor_id}}",
                        ),
                        panels.target(
                            f"{metric('log_store_latest_read_epoch')}",
                            "latest read epoch @ {{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}}",
                        ),
                        panels.target(
                            f"{metric('kv_log_store_buffer_unconsumed_min_epoch')}",
                            "Kv log store unconsumed min epoch @ {{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Log Store Lag",
                    "",
                    [
                        panels.target(
                            f"(max({metric('log_store_latest_write_epoch')}) by (sink_id, actor_id, sink_name)"
                            + f"- max({metric('log_store_latest_read_epoch')}) by (sink_id, actor_id, sink_name)) / (2^16) / 1000",
                            "{{sink_id}} {{sink_name}} @ actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Log Store Backpressure Ratio",
                    "",
                    [
                        panels.target(
                            f"avg(rate({metric('log_store_reader_wait_new_future_duration_ns')}[$__rate_interval])) by (connector, sink_id, actor_id, sink_name) / 1000000000",
                            "Backpressure @ {{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Log Store Consume Persistent Log Lag",
                    "",
                    [
                        panels.target(
                            f"clamp_min((max({metric('log_store_first_write_epoch')}) by (sink_id, actor_id, sink_name)"
                            + f"- max({metric('log_store_latest_read_epoch')}) by (sink_id, actor_id, sink_name)) / (2^16) / 1000, 0)",
                            "{{sink_id}} {{sink_name}} @ actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Log Store Consume Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('log_store_read_rows')}[$__rate_interval])) by (connector, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Executor Log Store Consume Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('log_store_read_rows')}[$__rate_interval])) by ({NODE_LABEL}, connector, sink_id, actor_id, sink_name)",
                            "{{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "Log Store Consume Throughput(MB/s)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('log_store_read_bytes')}[$__rate_interval])) by (connector, sink_id, sink_name) / (1000*1000)",
                            "{{sink_id}} {{sink_name}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "Executor Log Store Consume Throughput(MB/s)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('log_store_read_bytes')}[$__rate_interval])) by ({NODE_LABEL}, connector, sink_id, actor_id, sink_name) / (1000*1000)",
                            "{{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Log Store Write Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('log_store_write_rows')}[$__rate_interval])) by (sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}}",
                        ),
                        panels.target_hidden(
                            f"sum(rate({metric('log_store_write_rows')}[$__rate_interval])) by ({NODE_LABEL}, sink_id, actor_id, sink_name)",
                            "{{sink_id}} {{sink_name}} @ actor {{actor_id}} {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Kv Log Store Read Storage Row Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('kv_log_store_storage_read_count')}[$__rate_interval])) by (actor_id, connector, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Kv Log Store Read Storage Size",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('kv_log_store_storage_read_size')}[$__rate_interval])) by (actor_id, connector, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Kv Log Store Write Storage Row Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('kv_log_store_storage_write_count')}[$__rate_interval])) by (actor_id, connector, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Kv Log Store Write Storage Size",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('kv_log_store_storage_write_size')}[$__rate_interval])) by (actor_id, connector, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Kv Log Store Buffer State",
                    "",
                    [
                        panels.target(
                            f"{metric('kv_log_store_buffer_unconsumed_item_count')}",
                            "Unconsumed item count @ {{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}}",
                        ),
                        panels.target(
                            f"{metric('kv_log_store_buffer_unconsumed_row_count')}",
                            "Unconsumed row count @ {{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}}",
                        ),
                        panels.target(
                            f"{metric('kv_log_store_buffer_unconsumed_epoch_count')}",
                            "Unconsumed epoch count @ {{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Kv Log Store Rewind Rate",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('kv_log_store_rewind_count')}[$__rate_interval])) by (actor_id, connector, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Rewind delay (second)",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(1.0, sum(rate({metric('kv_log_store_rewind_delay_bucket')}[$__rate_interval])) by (le, actor_id, connector, sink_id, sink_name))",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Chunk Buffer Size",
                    "Total size of chunks buffered in a barrier",
                    [
                        panels.target(
                            f"sum({metric('stream_sink_chunk_buffer_size')}) by (sink_id, actor_id, sink_name) * on(actor_id) group_left(sink_name) {metric('sink_info')}",
                            "sink {{sink_id}} {{sink_name}} - actor {{actor_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]
