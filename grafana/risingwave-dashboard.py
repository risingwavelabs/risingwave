from grafanalib.core import (
    Dashboard, TimeSeries,
    Target, GridPos, RowPanel, Time
)
import logging

datasource = {
    "type": "prometheus",
    "uid": "risedev-prometheus"
}


class Layout:
    def __init__(self):
        self.x = 0
        self.y = 0
        self.w = 0
        self.h = 0

    def next_row(self):
        self.y += self.h
        self.x = 0
        self.w = 24
        self.h = 1
        (x, y) = (self.x, self.y)
        return GridPos(h=1, w=24, x=x, y=y)

    def next_half_width_graph(self):
        if self.x + self.w > 24 - 12:
            self.y += self.h
            self.x = 0
        else:
            self.x += self.w
        (x, y) = (self.x, self.y)
        self.h = 8
        self.w = 12
        return GridPos(h=8, w=12, x=x, y=y)

    def next_one_third_width_graph(self):
        if self.x + self.w > 24 - 8:
            self.y += self.h
            self.x = 0
        else:
            self.x += self.w
        (x, y) = (self.x, self.y)
        self.h = 8
        self.w = 8
        return GridPos(h=8, w=8, x=x, y=y)


class Panels:
    def __init__(self, datasource):
        self.layout = Layout()
        self.datasource = datasource

    def row(self, title):
        gridPos = self.layout.next_row()
        return RowPanel(title=title, gridPos=gridPos)

    def row_collapsed(self, title, panels):
        gridPos = self.layout.next_row()
        return RowPanel(title=title, gridPos=gridPos, collapsed=True, panels=panels)

    def target(self, expr, legendFormat, hide=False):
        return Target(expr=expr, legendFormat=legendFormat, datasource=self.datasource, hide=hide)

    def timeseries(self, title, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(title=title, targets=targets, gridPos=gridPos, fillOpacity=10)

    def timeseries_count(self, title, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(title=title, targets=targets, gridPos=gridPos, fillOpacity=10, legendDisplayMode="table",
                          legendPlacement="right")

    def timeseries_latency(self, title, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(title=title, targets=targets, gridPos=gridPos, unit="s", fillOpacity=10,
                          legendDisplayMode="table", legendPlacement="right", legendCalcs=["max"])

    def timeseries_bytes_per_sec(self, title, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(title=title, targets=targets, gridPos=gridPos, unit="Bps", fillOpacity=10)

    def timeseries_bytes(self, title, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(title=title, targets=targets, gridPos=gridPos, unit="decbytes", fillOpacity=10)

    def timeseries_kilobytes(self, title, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(title=title, targets=targets, gridPos=gridPos, unit="deckbytes", fillOpacity=10,
                          legendDisplayMode="table", legendPlacement="right", legendCalcs=["max"])

    def timeseries_ops(self, title, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(title=title, targets=targets, gridPos=gridPos, unit="ops", fillOpacity=10)

    def timeseries_rowsps(self, title, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(title=title, targets=targets, gridPos=gridPos, unit="rows/s", fillOpacity=10)

    def timeseries_memory(self, title, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(title=title, targets=targets, gridPos=gridPos, unit="decbytes", fillOpacity=10)

    def timeseries_cpu(self, title, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(title=title, targets=targets, gridPos=gridPos, unit="percentunit", fillOpacity=10)

    def timeseries_latency_small(self, title, targets):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(title=title, targets=targets, gridPos=gridPos, unit="s", fillOpacity=10)

    def sub_panel(self):
        return Panels(self.datasource)


panels = Panels(datasource)

logging.basicConfig(level=logging.WARN)


def section_cluster_node(panels):
    return [
        panels.row("Cluster Node"),
        panels.timeseries_memory("Node Memory", [
            panels.target(
                "avg(process_resident_memory_bytes) by (job,instance)", "{{job}} @ {{instance}}"
            )]),
        panels.timeseries_cpu("Node CPU", [
            panels.target(
                "sum(rate(process_cpu_seconds_total[1m])) by (job,instance)", "{{job}} @ {{instance}}"
            )]),
    ]


def section_compaction(panels):
    return [
        panels.row("Compaction"),
        panels.timeseries_count("SST Counts", [
            panels.target(
                "sum(storage_level_sst_num) by (instance, level_index)", "L{{level_index}}"
            ),
        ]),
        panels.timeseries_kilobytes("KBs level sst", [
            panels.target(
                "sum(storage_level_total_file_size) by (instance, level_index)", "L{{level_index}}"
            ),
        ]),
        panels.timeseries_count("Compaction Count", [
            panels.target(
                "storage_level_compact_frequency", "{{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_latency("Compaction Duration", [
            panels.target(
                "histogram_quantile(0.5, sum(rate(state_store_compact_task_duration_bucket[1m])) by (le, job, instance))", "compact-task p50 - {{job}} @ {{instance}}"
            ),
            panels.target(
                "histogram_quantile(0.9, sum(rate(state_store_compact_task_duration_bucket[1m])) by (le, job, instance))", "compact-task p90 - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum by(le)(rate(state_store_compact_task_duration_sum[1m])) / sum by(le) (rate(state_store_compact_task_duration_count[1m]))", "compact-task avg"
            ),
            panels.target(
                "sum by(le)(rate(state_store_compact_sst_duration_sum[1m])) / sum by(le) (rate(state_store_compact_sst_duration_count[1m]))", "compact-key-range avg"
            ),
            panels.target(
                "histogram_quantile(0.9, sum(rate(state_store_compact_sst_duration_bucket[1m])) by (le, job, instance))", "compact-key-range p90 - {{job}} @ {{instance}}"
            ),
            panels.target(
                "histogram_quantile(0.9, sum(rate(state_store_get_table_id_total_time_duration_bucket[1m])) by (le, job, instance))", "get-table-id p90 {{job}} @ {{instance}}"
            ),
            panels.target(
                "histogram_quantile(0.9, sum(rate(state_store_remote_read_time_per_task_bucket[1m])) by (le, job, instance))", "remote-io p90 - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_bytes_per_sec("Compaction Throughput", [
            panels.target(
                "sum(rate(state_store_compaction_read_bytes[1m]))by(job,instance)", "read - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum(rate(state_store_compaction_write_bytes[1m]))by(job,instance)", "write - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum(rate(state_store_write_build_l0_bytes[1m]))by (job,instance)", "flush - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_count("Compacting SST Count", [
            panels.target(
                "storage_level_compact_cnt", "L{{level_index}}"
            ),
        ]),
        panels.timeseries_bytes("Hummock Version Size", [
            panels.target(
                "version_size", "version size"
            ),
        ]),
        panels.timeseries_bytes("GBs Read from Next Level", [
            panels.target(
                "histogram_quantile(0.5, sum(rate(storage_level_compact_read_next_bucket[1m])) by (le, level_index))", "L{{level_index}} read bytes p50"
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(storage_level_compact_read_next_bucket[1m])) by (le, level_index))", "L{{level_index}} read bytes p99"
            ),
        ]),
        panels.timeseries_bytes("GBs Read from Current Level", [
            panels.target(
                "histogram_quantile(0.5, sum(rate(storage_level_compact_read_curr_bucket[1m])) by (le, level_index))", "L{{level_index}} read bytes p50"
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(storage_level_compact_read_curr_bucket[1m])) by (le, level_index))", "L{{level_index}} read bytes p99"
            ),
        ]),
        panels.timeseries_count("Count of SSTs Read from Current Level", [
            panels.target(
                "histogram_quantile(0.5, sum(rate(storage_level_compact_read_sstn_curr_bucket[1m])) by (le, level_index))", "L{{level_index}} p50"
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(storage_level_compact_read_sstn_curr_bucket[1m])) by (le, level_index))", "L{{level_index}} p99"
            ),
        ]),
        panels.timeseries_bytes("GBs Written to Next Level", [
            panels.target(
                "histogram_quantile(0.5, sum(rate(storage_level_compact_write_bucket[1m])) by (le, level_index))", "L{{level_index}} write bytes p50"
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(storage_level_compact_write_bucket[1m])) by (le, level_index))", "L{{level_index}} write bytes p99"
            ),
        ]),
        panels.timeseries_count("Count of SSTs Written to Next Level", [
            panels.target(
                "histogram_quantile(0.5, sum(rate(storage_level_compact_write_sstn_bucket[1m])) by (le, level_index))", "L{{level_index}} write count p50"
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(storage_level_compact_write_sstn_bucket[1m])) by (le, level_index))", "L{{level_index}} write count p99"
            ),
        ]),
        panels.timeseries_count("Count of SSTs Read from Next Level", [
            panels.target(
                "histogram_quantile(0.5, sum(rate(storage_level_compact_read_sstn_next_bucket[1m])) by (le, level_index))", "L{{level_index}} read count p50"
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(storage_level_compact_read_sstn_next_bucket[1m])) by (le, level_index))", "L{{level_index}} read count p99"
            ),
        ]),
    ]


def section_object_storage(panels):
    return [
        panels.row("Object Storage"),
        panels.timeseries_bytes_per_sec("Throughput", [
            panels.target(
                "sum(rate(object_store_read_bytes[1m]))by(job,instance)", "read - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum(rate(object_store_write_bytes[1m]))by(job,instance)", "write - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_latency("Operation Duration", [
            panels.target(
                "histogram_quantile(0.5, sum(rate(object_store_operation_latency_bucket[1m])) by (le, type))", "{{type}} p50"
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(object_store_operation_latency_bucket[1m])) by (le, type, job, instance))", "{{type}} p99 - {{job}} @ {{instance}}"
            ),
            panels.target(
                "histogram_quantile(0.9, sum(rate(object_store_operation_latency_bucket[1m])) by (le, type, job, instance))", "{{type}} p90 - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum by(le, type)(rate(object_store_operation_latency_sum[1m])) / sum by(le, type) (rate(object_store_operation_latency_count[1m]))", "{{type}} avg"
            ),
        ]),
        panels.timeseries_ops("Operation", [
            panels.target(
                "sum(rate(object_store_operation_latency_count[1m])) by (le, type, job, instance)", "{{type}} - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_bytes("Op Size", [
            panels.target(
                "histogram_quantile(0.9, sum(rate(object_store_operation_bytes_bucket[1m])) by (le, type))", "{{type}}  p90"
            ),
            panels.target(
                "histogram_quantile(0.80, sum(rate(object_store_operation_bytes_bucket[1m])) by (le, type))", "{{type}} p80"
            ),
        ]),
    ]


def quantile(f, percentiles):
    return list(map(lambda p: f(p / 100.0, str(p)), percentiles))


def section_streaming(panels):
    return [
        panels.row("Streaming"),
        panels.timeseries_latency(
            "Barrier Latency",
            quantile(lambda quantile, legend: panels.target(
                f"histogram_quantile({quantile}, sum(rate(meta_barrier_duration_seconds_bucket[1m])) by (le))", f"barrier_latency_p{legend}"
            ), [50, 90, 99, 999]) + [
                panels.target(
                    "rate(meta_barrier_duration_seconds_sum[1m]) / rate(meta_barrier_duration_seconds_count[1m])", "barrier_latency_avg"
                ),
            ]),
        panels.timeseries_rowsps("Source Throughput", [
            panels.target(
                "rate(stream_source_output_rows_counts[15s])", "source_id = {{source_id}}"
            ),
        ]),
    ]


def section_streaming_actors(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed("Streaming Actors", [
            panels.timeseries_rowsps("Actor Throughput", [
                panels.target(
                    "rate(stream_actor_row_count[15s]) > 0", "{{actor_id}}"
                ),
            ]),
            panels.timeseries_latency("Actor Barrier Latency", [
                panels.target(
                    "rate(stream_actor_barrier_time[1m]) > 0", "{{actor_id}}"
                ),
            ]),
            panels.timeseries_latency("Actor Processing Time", [
                panels.target(
                    "rate(stream_actor_processing_time[1m]) > 0", "{{actor_id}}"
                ),
            ]),
        ])
    ]


def section_streaming_exchange(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed("Streaming Exchange", [
            panels.timeseries_bytes_per_sec("Exchange Send Throughput", [
                panels.target(
                    "rate(stream_exchange_send_size[15s])", "{{up_actor_id}}->{{down_actor_id}}"
                ),
            ]),
            panels.timeseries_bytes_per_sec("Exchange Recv Throughput", [
                panels.target(
                    "rate(stream_exchange_recv_size[15s])", "{{up_actor_id}}->{{down_actor_id}}"
                ),
            ]),
        ]),
    ]


def section_hummock(panels):
    return [
        panels.row("Hummock"),

        panels.timeseries_ops("Read Ops", [
            panels.target(
                "sum(rate(state_store_get_duration_count[1m])) by (job,instance)", "get - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum(rate(state_store_range_scan_duration_count[1m])) by (job,instance)", "range_scan - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum(rate(state_store_range_reverse_scan_duration_count[1m])) by (job,instance)", "reverse_range_scan - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum(rate(state_store_get_shared_buffer_hit_counts[1m])) by (job,instance)", "shared_buffer hit - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_latency("Read Duration", [
            panels.target(
                "histogram_quantile(0.5, sum(rate(state_store_get_duration_bucket[1m])) by (le, job, instance))", "get p50 - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(state_store_get_duration_bucket[1m])) by (le, job, instance))", "get p99 - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "histogram_quantile(0.9, sum(rate(state_store_get_duration_bucket[1m])) by (le, job, instance))", "get p90 - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "sum by(le, job, instance)(rate(state_store_get_duration_sum[1m])) / sum by(le, job, instance) (rate(state_store_get_duration_count[1m]))", "get avg - {{job}} @ {{instance}}"
            ),
            panels.target(
                "histogram_quantile(0.5, sum(rate(state_store_range_scan_duration_bucket[1m])) by (le, job, instance))", "range_scan p50 - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(state_store_range_scan_duration_bucket[1m])) by (le, job, instance))", "range_scan p99 - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "histogram_quantile(0.9, sum(rate(state_store_range_scan_duration_bucket[1m])) by (le,job, instance))", "range_scan p90 - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "sum by(le, job, instance)(rate(state_store_range_scan_duration_sum[1m])) / sum by(le, job,instance) (rate(state_store_range_scan_duration_count[1m]))", "range_scan avg - {{job}} @ {{instance}}"
            ),
            panels.target(
                "histogram_quantile(0.5, sum(rate(state_store_range_reverse_scan_duration_bucket[1m])) by (le, job, instance))", "reverse_range_scan p50 - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(state_store_range_reverse_scan_duration_bucket[1m])) by (le, job, instance))", "reverse_range_scan p99 - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "sum by(le, job, instance)(rate(state_store_range_reverse_scan_duration_sum[1m])) / sum by(le, job, instance) (rate(state_store_range_reverse_scan_duration_count[1m]))", "reverse_range_scan avg - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_ops("Block Ops", [
            panels.target(
                "sum(rate(state_store_sst_store_block_request_counts[1m])) by (instance, type)", "{{type}} - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_bytes("Cache Size", [
            panels.target(
                "avg(state_store_meta_cache_size) by (job,instance)", "meta cache - {{job}} @ {{instance}}"
            ),
            panels.target(
                "avg(state_store_block_cache_size) by (job,instance)", "data cache - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_ops("Write Ops", [
            panels.target(
                "sum(rate(state_store_write_batch_duration_count[1m])) by (job,instance)", "write batch - {{job}} @ {{instance}} "
            ),
            panels.target(
                "sum(rate(state_store_shared_buffer_to_l0_duration_count[1m])) by (job,instance)", "l0 - {{job}} @ {{instance}} "
            ),
        ]),
        panels.timeseries_latency("write duration", [
            panels.target(
                "histogram_quantile(0.5, sum(rate(state_store_write_batch_duration_bucket[1m])) by (le, job, instance))", "shared_buffer p50 - {{job}} @ {{instance}}"
            ),
            panels.target(
                "histogram_quantile(0.9, sum(rate(state_store_write_batch_duration_bucket[1m])) by (le, job, instance))", "shared_buffer p90 - {{job}} @ {{instance}}"
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(state_store_write_batch_duration_bucket[1m])) by (le, job, instance))", "shared_buffer p99 - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum by(le, job, instance)(rate(state_store_write_batch_duration_sum[1m]))  / sum by(le, job, instance)(rate(state_store_write_batch_duration_count[1m]))", "shared_buffer avg - {{job}} @ {{instance}}"
            ),
            panels.target(
                "histogram_quantile(0.5, sum(rate(state_store_write_shared_buffer_sync_time_bucket[1m])) by (le, job, instance))", "sync_remote p50 - {{job}} @ {{instance}}"
            ),
            panels.target(
                "histogram_quantile(0.9, sum(rate(state_store_write_shared_buffer_sync_time_bucket[1m])) by (le, job, instance))", "sync_remote p90 - {{job}} @ {{instance}}"
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(state_store_write_shared_buffer_sync_time_bucket[1m])) by (le, job, instance))", "sync_remote p99 - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum by(le, job, instance)(rate(state_store_write_shared_buffer_sync_time_sum[1m]))  / sum by(le, job, instance)(rate(state_store_write_shared_buffer_sync_time_count[1m]))", "sync_remote avg - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_count("sst read counters", [
            panels.target(
                "sum(rate(state_store_bloom_filter_true_negative_counts[1m])) by (job,instance)", "bloom filter true negative  - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum(rate(state_store_bloom_filter_might_positive_counts[1m])) by (job,instance)", "bloom filter might positive  - {{job}} @ {{instance}}"
            ),
            panels.target(
                "histogram_quantile(0.9, sum(rate(state_store_iter_merge_sstable_counts_bucket[1m])) by (le, job, instance))", "# merged ssts p90  - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(state_store_iter_merge_sstable_counts_bucket[1m])) by (le, job, instance))", "# merged ssts p99  - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "sum by(le, job, instance)(rate(state_store_iter_merge_sstable_counts_sum[1m]))  / sum by(le, job, instance)(rate(state_store_iter_merge_sstable_counts_count[1m]))", "# merged ssts avg  - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_bytes_per_sec("read throughput", [
            panels.target(
                "sum by(job,instance)(rate(state_store_get_key_size_sum[1m])+rate(state_store_get_value_size_sum[1m]))/sum by(job,instance)(rate(state_store_get_key_size_count[1m]))", "get  - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum by(job,instance)(rate(state_store_get_key_size_sum[1m]))/sum by(job,instance)(rate(state_store_get_key_size_count[1m]))", "get key  - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum by(job,instance)(rate(state_store_get_value_size_sum[1m]))/sum by(job,instance)(rate(state_store_get_value_size_count[1m]))", "get value  - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum by(job,instance)(rate(state_store_range_scan_size_sum[1m]))/sum by(job,instance)(rate(state_store_range_scan_size_count[1m]))", "range_scan  - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum by(job,instance)(rate(state_store_range_reverse_scan_size_sum[1m]))/sum by(job,instance)(rate(state_store_range_reverse_scan_size_count[1m]))", "reverse_range_scan  - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_ops("write kv pair counts", [
            panels.target(
                "sum(rate(state_store_write_batch_tuple_counts[1m])) by (job,instance)", "write_batch_kv_pair_count - {{instance}} "
            ),
        ]),
        panels.timeseries_bytes_per_sec("write throughput", [
            panels.target(
                "sum(rate(state_store_write_batch_size_sum[1m]))by(job,instance) / sum(rate(state_store_write_batch_size_count[1m]))by(job,instance)", "shared_buffer - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum(rate(state_store_shared_buffer_to_sstable_size_sum[1m]))by(job,instance) / sum(rate(state_store_shared_buffer_to_sstable_size_count[1m]))by(job,instance)", "sync - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_latency("build sstable duration", [
            panels.target(
                "histogram_quantile(0.5, sum(rate(state_store_shared_buffer_to_l0_duration_bucket[1m])) by (le, job, instance))", "p50 - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "histogram_quantile(0.9, sum(rate(state_store_shared_buffer_to_l0_duration_bucket[1m])) by (le, job, instance))", "p90 - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(state_store_shared_buffer_to_l0_duration_bucket[1m])) by (le, job, instance))", "p99 - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "sum by(le, job, instance) (rate(state_store_shared_buffer_to_l0_duration_sum[1m])) / sum by(le, job, instance) (rate(state_store_shared_buffer_to_l0_duration_count[1m]))", "avg - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_ops("merge iterators ops", [
            panels.target(
                "sum(rate(state_store_iter_merge_seek_duration_count[1m])) by (job,instance)", "MI seek  - {{job}} @ {{instance}}"
            ),
            panels.target(
                "sum(rate(state_store_iter_merge_next_duration_count[1m])) by (job,instance)", "MI next  - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_latency("row seq scan next duration", [
            panels.target(
                "histogram_quantile(0.5, sum(rate(batch_row_seq_scan_next_duration_bucket[1m])) by (le, job, instance))", "row_seq_scan next p50 - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "histogram_quantile(0.9, sum(rate(batch_row_seq_scan_next_duration_bucket[1m])) by (le, job, instance))", "p90 - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(batch_row_seq_scan_next_duration_bucket[1m])) by (le, job, instance))", "p99 - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "sum by(le, job, instance) (rate(batch_row_seq_scan_next_duration_sum[1m])) / sum by(le, job, instance) (rate(batch_row_seq_scan_next_duration_count[1m]))", "row_seq_scan next avg - {{job}} @ {{instance}}"
            ),
        ]),
        panels.timeseries_latency("merge iterators duration", [
            panels.target(
                "histogram_quantile(0.5, sum(rate(state_store_iter_merge_seek_duration_bucket[1m])) by (le, job, instance))", "mi_seek p50  - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "histogram_quantile(0.9, sum(rate(state_store_iter_merge_seek_duration_bucket[1m])) by (le, job, instance))", "mi_seek p90  - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "histogram_quantile(0.99, sum(rate(state_store_iter_merge_seek_duration_bucket[1m])) by (le, job, instance))", "mi_seek p99  - {{job}} @ {{instance}}", True
            ),
            panels.target(
                "sum by(le, job, instance) (rate(state_store_iter_merge_seek_duration_sum[1m])) / sum by(le, job, instance) (rate(state_store_iter_merge_seek_duration_count[1m]))", "mi_seek avg  - {{job}} @ {{instance}}"
            ),
        ]),
    ]


def section_hummock_table_comparison(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed("Streaming Exchange", [
            panels.timeseries("Exchange Send Throughput", [
                panels.target(
                    "rate(stream_exchange_send_size[15s]) / 1024", "{{up_actor_id}}->{{down_actor_id}}"
                ),
            ]),
            panels.timeseries("Exchange Recv Throughput", [
                panels.target(
                    "rate(stream_exchange_recv_size[15s]) / 1024", "{{up_actor_id}}->{{down_actor_id}}"
                ),
            ]),
        ]),
    ]


def section_hummock_table_comparison(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed("gRPC Hummock Table Comparison", [
            panels.timeseries_latency_small("get new TableID latency p50", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/GetNewTableId\"}[1m])) by (le))", "hummock_manager_ GetNewTableId_p50"
                ),
                panels.target(
                    "histogram_quantile(0.5, sum(irate(state_store_get_new_table_id_latency_bucket[1m])) by (le, job, instance)) ", "hummock_client_ GetNewTableId_p50 - {{instance}} "
                ),
            ]),
            panels.timeseries_latency_small("get new TableID latency p90", [
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/GetNewTableId\"}[1m])) by (le))", "hummock_manager_ GetNewTableId_p90"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(state_store_get_new_table_id_latency_bucket[1m])) by (le, job, instance))", "hummock_client_ GetNewTableId_p90 - {{instance}} "
                ),
            ]),
            panels.timeseries_latency_small("get new TableID latency p99", [
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/GetNewTableId\"}[1m])) by (le))", "hummock_manager_ GetNewTableId_p99"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(state_store_get_new_table_id_latency_bucket[1m])) by (le, job, instance))", "hummock_client_GetNewTableId_p99 - {{instance}} "
                ),
            ]),
            panels.timeseries_latency_small("add tables latency p50", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/AddTables\"}[1m])) by (le))", "hummock_manager_ AddTables_p50"
                ),
                panels.target(
                    "histogram_quantile(0.5, sum(irate(state_store_add_tables_latency_bucket[1m])) by (le, job, instance))", "hummock_client_ AddTables_p50 - {{instance}} "
                ),
            ]),
            panels.timeseries_latency_small("add tables latency p90", [
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/AddTables\"}[1m])) by (le))", "hummock_manager_ AddTables_p90"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(state_store_add_tables_latency_bucket[1m])) by (le, job, instance))", "hummock_client_ AddTables_p90 - {{instance}} "
                ),
            ]),
            panels.timeseries_latency_small("add tables latency p99", [
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/AddTables\"}[1m])) by (le))", "hummock_manager_ AddTables_p99"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(state_store_add_tables_latency_bucket[1m])) by (le, job, instance))", "hummock_client_ AddTables_p99 - {{instance}} "
                ),
            ]),
        ]),
    ]


def section_hummock_compaction_comparison(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "gRPC Hummock Compaction Comparison",
            quantile(lambda quantile, legend: panels.timeseries_latency_small("report compation latency p50", [
                panels.target(
                    f"histogram_quantile({quantile}, sum(irate(meta_grpc_duration_seconds_bucket{{path=\"/hummock.HummockManagerService/ReportCompactionTasks\"}}[1m])) by (le))", f"hummock_manager_ ReportCompactionTasks_p{legend}"
                ),
                panels.target(
                    f"histogram_quantile({quantile}, sum(irate(state_store_report_compaction_task_latency_bucket[1m])) by (le, job, instance))", f"hummock_client_ ReportCompactionTasks_p{legend} - {{{{instance}}}}"
                ),
            ]), [50, 90, 99])
        )
    ]


def section_grpc_hummock_version_comparison(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed("gRPC Hummock Version Comparison", [
            panels.timeseries_latency_small("pin version latency p50", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/PinVersion\"}[1m])) by (le, job, instance))", "hummock_manager_pinVersion_p50 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.5, sum(irate(state_store_pin_version_latency_bucket[1m])) by (le, job, instance))", "hummock_client_pinVersion_p50 - {{instance}} "
                ),
            ]),
            panels.timeseries_latency_small("pin version latency p90", [
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/PinVersion\"}[1m])) by (le, job, instance))", "hummock_manager_pinVersion_p90 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(state_store_pin_version_latency_bucket[1m])) by (le, job, instance))", "hummock_client_pinVersion_p90 - {{instance}} "
                ),
            ]),
            panels.timeseries_latency_small("pin version latency p90", [
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/PinVersion\"}[1m])) by (le, job, instance))", "hummock_manager_pinVersion_p99 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(state_store_pin_version_latency_bucket[1m])) by (le, job, instance))", "hummock_client_pinVersion_p99 - {{instance}} "
                ),
            ]),
            panels.timeseries_latency_small("unpin version latency p50", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/UnpinVersion\"}[1m])) by (le, job, instance))", "hummock_manager_unpinVersion_p50 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.5, sum(irate(state_store_unpin_version_latency_bucket[1m])) by (le, job, instance))", "hummock_client_unpinVersion_p50 - {{instance}} "
                ),
            ]),
            panels.timeseries_latency_small("unpin version latency p90", [
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/UnpinVersion\"}[1m])) by (le, job, instance))", "hummock_manager_unpinVersion_p90 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(state_store_unpin_version_latency_bucket[1m])) by (le, job, instance))", "hummock_client_unpinVersion_p90 - {{instance}} "
                ),
            ]),
            panels.timeseries_latency_small("unpin version latency p99", [
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/UnpinVersion\"}[1m])) by (le, job, instance))", "hummock_manager_unpinVersion_p99 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(state_store_unpin_version_latency_bucket[1m])) by (le, job, instance))", "hummock_client_unpinVersion_p99 - {{instance}} "
                ),
            ]),
        ])
    ]


def section_grpc_meta_catalog_service(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed("gRPC Meta: Catalog Service", [
            panels.timeseries_latency_small("create latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.CatalogService/Create\"}[1m])) by (le))", "Create_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.CatalogService/Create\"}[1m])) by (le))", "Create_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.CatalogService/Create\"}[1m])) by (le))", "Create_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/meta.CatalogService/Create\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/meta.CatalogService/Create\"}[1m]))", "Create_avg"
                ),
            ]),
            panels.timeseries_latency_small("drop latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.CatalogService/Drop\"}[1m])) by (le))", "Drop_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.CatalogService/Drop\"}[1m])) by (le))", "Drop_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.CatalogService/Drop\"}[1m])) by (le))", "Drop_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/meta.CatalogService/Drop\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/meta.CatalogService/Drop\"}[1m]))", "Drop_avg"
                ),
            ]),
            panels.timeseries_latency_small("get catalog latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.CatalogService/GetCatalog\"}[1m])) by (le))", "GetCatalog_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.CatalogService/GetCatalog\"}[1m])) by (le))", "GetCatalog_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.CatalogService/GetCatalog\"}[1m])) by (le))", "GetCatalog_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/meta.CatalogService/GetCatalog\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/meta.CatalogService/GetCatalog\"}[1m]))", "GetCatalog_avg"
                ),
            ]),
        ])
    ]


def section_grpc_meta_cluster_service(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed("gRPC Meta: Cluster Service", [
            panels.timeseries_latency_small("add worker node latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.ClusterService/AddWorkerNode\"}[1m])) by (le))", "AddWorkerNode_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.ClusterService/AddWorkerNode\"}[1m])) by (le))", "AddWorkerNode_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.ClusterService/AddWorkerNode\"}[1m])) by (le))", "AddWorkerNode_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/meta.ClusterService/AddWorkerNode\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/meta.ClusterService/AddWorkerNode\"}[1m]))", "AddWorkerNode_avg"
                ),
            ]),
            panels.timeseries_latency_small("list all node latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.ClusterService/ListAllNodes\"}[1m])) by (le))", "ListAllNodes_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.ClusterService/ListAllNodes\"}[1m])) by (le))", "ListAllNodes_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.ClusterService/ListAllNodes\"}[1m])) by (le))", "ListAllNodes_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/meta.ClusterService/ListAllNodes\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/meta.ClusterService/ListAllNodes\"}[1m]))", "ListAllNodes_avg"
                ),
            ]),
        ]),
    ]


def section_grpc_meta_stream_manager(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed("gRPC Meta: Stream Manager", [
            panels.timeseries_latency_small("create materialized view latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.StreamManagerService/CreateMaterializedView\"}[1m])) by (le))", "CreateMaterializedView_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.StreamManagerService/CreateMaterializedView\"}[1m])) by (le))", "CreateMaterializedView_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.StreamManagerService/CreateMaterializedView\"}[1m])) by (le))", "CreateMaterializedView_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/meta.StreamManagerService/CreateMaterializedView\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/meta.StreamManagerService/CreateMaterializedView\"}[1m]))", "CreateMaterializedView_avg"
                ),
            ]),
            panels.timeseries_latency_small("drop materialized view latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.StreamManagerService/DropMaterializedView\"}[1m])) by (le))", "DropMaterializedView_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.StreamManagerService/DropMaterializedView\"}[1m])) by (le))", "DropMaterializedView_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.StreamManagerService/DropMaterializedView\"}[1m])) by (le))", "DropMaterializedView_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/meta.StreamManagerService/DropMaterializedView\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/meta.StreamManagerService/DropMaterializedView\"}[1m]))", "DropMaterializedView_avg"
                ),
            ]),
            panels.timeseries_latency_small("flush latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.StreamManagerService/Flush\"}[1m])) by (le))", "Flush_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.StreamManagerService/Flush\"}[1m])) by (le))", "Flush_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/meta.StreamManagerService/Flush\"}[1m])) by (le))", "Flush_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/meta.StreamManagerService/Flush\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/meta.StreamManagerService/Flush\"}[1m]))", "Flush_avg"
                ),
            ]),
        ]),
    ]


def section_grpc_meta_hummock_manager(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed("gRPC Meta: Hummock Manager", [
            panels.timeseries_latency_small("version latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/UnpinVersion\"}[1m])) by (le))", "UnpinVersion_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/UnpinVersion\"}[1m])) by (le))", "UnpinVersion_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/UnpinVersion\"}[1m])) by (le))", "UnpinVersion_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/hummock.HummockManagerService/UnpinVersion\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/hummock.HummockManagerService/UnpinVersion\"}[1m]))", "UnpinVersion_avg"
                ),
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/PinVersion\"}[1m])) by (le))", "PinVersion_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/PinVersion\"}[1m])) by (le))", "PinVersion_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/PinVersion\"}[1m])) by (le))", "PinVersion_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/hummock.HummockManagerService/PinVersion\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/hummock.HummockManagerService/PinVersion\"}[1m]))", "PinVersion_avg"
                ),
            ]),
            panels.timeseries_latency_small("snapshot latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/UnpinSnapshot\"}[1m])) by (le))", "UnpinSnapshot_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/UnpinSnapshot\"}[1m])) by (le))", "UnpinSnapshot_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/UnpinSnapshot\"}[1m])) by (le))", "UnpinSnapshot_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/hummock.HummockManagerService/UnpinSnapshot\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/hummock.HummockManagerService/UnpinSnapshot\"}[1m]))", "UnpinSnapshot_avg"
                ),
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/PinSnapshot\"}[1m])) by (le))", "PinSnapshot_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/PinSnapshot\"}[1m])) by (le))", "PinSnapshot_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/PinSnapshot\"}[1m])) by (le))", "PinSnapshot_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/hummock.HummockManagerService/PinSnapshot\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/hummock.HummockManagerService/PinSnapshot\"}[1m]))", "PinSnapshot_avg"
                ),
            ]),
            panels.timeseries_latency_small("compation latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/ReportCompactionTasks\"}[1m])) by (le))", "ReportCompactionTasks_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/ReportCompactionTasks\"}[1m])) by (le))", "ReportCompactionTasks_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/ReportCompactionTasks\"}[1m])) by (le))", "ReportCompactionTasks_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/hummock.HummockManagerService/ReportCompactionTasks\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/hummock.HummockManagerService/ReportCompactionTasks\"}[1m]))", "ReportCompactionTasks_avg"
                ),
            ]),
            panels.timeseries_latency_small("table latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/AddTables\"}[1m])) by (le))", "AddTables_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/AddTables\"}[1m])) by (le))", "AddTables_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/AddTables\"}[1m])) by (le))", "AddTables_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/hummock.HummockManagerService/AddTables\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/hummock.HummockManagerService/AddTables\"}[1m]))", "AddTables_avg"
                ),
                panels.target(
                    "histogram_quantile(0.5, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/GetNewTableId\"}[1m])) by (le))", "GetNewTableId_p50"
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/GetNewTableId\"}[1m])) by (le))", "GetNewTableId_p90"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(meta_grpc_duration_seconds_bucket{path=\"/hummock.HummockManagerService/GetNewTableId\"}[1m])) by (le))", "GetNewTableId_p99"
                ),
                panels.target(
                    "sum(irate(meta_grpc_duration_seconds_sum{path=\"/hummock.HummockManagerService/GetNewTableId\"}[1m])) / sum(irate(meta_grpc_duration_seconds_count{path=\"/hummock.HummockManagerService/GetNewTableId\"}[1m]))", "GetNewTableId_avg"
                ),
            ]),
        ]),
    ]


def section_grpc_hummock_meta_client(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed("gRPC: Hummock Meta Client", [
            panels.timeseries_count("compaction_count", [
                panels.target(
                    "sum(irate(state_store_report_compaction_task_counts[1m])) by(job,instance)", "report_compaction_task_counts - {{instance}} "
                ),
            ]),
            panels.timeseries_latency("version_latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(state_store_pin_version_latency_bucket[1m])) by (le, job, instance))", "pin_version_latency_p50 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(state_store_pin_version_latency_bucket[1m])) by (le, job, instance))", "pin_version_latency_p99 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(state_store_pin_version_latency_bucket[1m])) by (le, job, instance))", "pin_version_latencyp90 - {{instance}} "
                ),
                panels.target(
                    "sum(irate(state_store_pin_version_latency_sum[1m])) / sum(irate(state_store_pin_version_latency_count[1m]))", "pin_version_latency_avg"
                ),
                panels.target(
                    "histogram_quantile(0.5, sum(irate(state_store_unpin_version_latency_bucket[1m])) by (le, job, instance))", "unpin_version_latency_p50 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(state_store_unpin_version_latency_bucket[1m])) by (le, job, instance))", "unpin_version_latency_p99 - {{instance}} "
                ),
                panels.target(
                    "sum(irate(state_store_unpin_version_latency_sum[1m])) / sum(irate(state_store_unpin_version_latency_count[1m]))", "unpin_version_latency_avg"
                ),
                panels.target(
                    "histogram_quantile(0.90, sum(irate(state_store_unpin_version_latency_bucket[1m])) by (le, job, instance))", "unpin_version_latency_p90 - {{instance}} "
                ),
            ]),
            panels.timeseries_count("version_count", [
                panels.target(
                    "sum(irate(state_store_pin_version_counts[1m])) by(job,instance)", "pin_version_counts - {{instance}} "
                ),
                panels.target(
                    "sum(irate(state_store_unpin_version_counts[1m])) by(job,instance)", "unpin_version_counts - {{instance}} "
                ),
            ]),
            panels.timeseries_latency("snapshot_latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(state_store_pin_snapshot_latency_bucket[1m])) by (le, job, instance))", "pin_snapshot_latency_p50 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(state_store_pin_snapshot_latency_bucket[1m])) by (le, job, instance))", "pin_snapshot_latency_p99 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(state_store_pin_snapshot_latency_bucket[1m])) by (le, job, instance))", "pin_snapshot_latencyp90 - {{instance}} "
                ),
                panels.target(
                    "sum(irate(state_store_pin_snapshot_latency_sum[1m])) / sum(irate(state_store_pin_snapshot_latency_count[1m]))", "pin_snapshot_latency_avg"
                ),
                panels.target(
                    "histogram_quantile(0.5, sum(irate(state_store_unpin_version_snapshot_bucket[1m])) by (le, job, instance))", "unpin_snapshot_latency_p50 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(state_store_unpin_version_snapshot_bucket[1m])) by (le, job, instance))", "unpin_snapshot_latency_p99 - {{instance}} "
                ),
                panels.target(
                    "sum(irate(state_store_unpin_snapshot_latency_sum[1m])) / sum(irate(state_store_unpin_snapshot_latency_count[1m]))", "unpin_snapshot_latency_avg"
                ),
                panels.target(
                    "histogram_quantile(0.90, sum(irate(state_store_unpin_snapshot_latency_bucket[1m])) by (le, job, instance))", "unpin_snapshot_latency_p90 - {{instance}} "
                ),
            ]),
            panels.timeseries_count("snapshot_count", [
                panels.target(
                    "sum(irate(state_store_pin_snapshot_counts[1m])) by(job,instance)", "pin_snapshot_counts - {{instance}} "
                ),
                panels.target(
                    "sum(irate(state_store_unpin_snapshot_counts[1m])) by(job,instance)", "unpin_snapshot_counts - {{instance}} "
                ),
            ]),
            panels.timeseries_latency("table_latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(state_store_add_tables_latency_bucket[1m])) by (le,instance))", "pin_snapshot_latency_p50 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(state_store_add_tables_latency_bucket[1m])) by (le, job, instance))", "add_table_latency_p99 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.9, sum(irate(state_store_add_tables_latency_bucket[1m])) by (le, job, instance))", "add_table_latency_p90 - {{instance}} "
                ),
                panels.target(
                    "sum(irate(state_store_add_tables_latency_sum[1m])) / sum(irate(state_store_add_tables_latency_count[1m]))", "add_table_latency_avg"
                ),
                panels.target(
                    "histogram_quantile(0.5, sum(irate(state_store_get_new_table_id_latency_bucket[1m])) by (le, job, instance))", "get_new_table_id_latency_p50 - {{instance}} "
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(state_store_get_new_table_id_latency_bucket[1m])) by (le, job, instance))", "get_new_table_id_latency_p99 - {{instance}} "
                ),
                panels.target(
                    "sum(irate(state_store_get_new_table_id_latency_sum[1m])) / sum(irate(state_store_get_new_table_id_latency_count[1m]))", "get_new_table_id_latency_avg"
                ),
                panels.target(
                    "histogram_quantile(0.90, sum(irate(state_store_get_new_table_id_latency_bucket[1m])) by (le, job, instance))", "get_new_table_id_latency_p90 - {{instance}} "
                ),
            ]),
            panels.timeseries_count("table_count", [
                panels.target(
                    "sum(irate(state_store_add_tables_counts[1m]))by(job,instance)", "add_tables_counts - {{instance}} "
                ),
                panels.target(
                    "sum(irate(state_store_get_new_table_id_counts[1m]))by(job,instance)", "get_new_table_id_counts - {{instance}} "
                ),
            ]),
            panels.timeseries_latency("compation_latency", [
                panels.target(
                    "histogram_quantile(0.5, sum(irate(state_store_report_compaction_task_latency_bucket[1m])) by (le, job, instance))", "report_compaction_task_latency_p50 - {{instance}}"
                ),
                panels.target(
                    "histogram_quantile(0.99, sum(irate(state_store_report_compaction_task_latency_bucket[1m])) by (le, job, instance))", "report_compaction_task_latency_p99 - {{instance}}"
                ),
                panels.target(
                    "sum(irate(state_store_report_compaction_task_latency_sum[1m])) / sum(irate(state_store_report_compaction_task_latency_count[1m]))", "report_compaction_task_latency_avg"
                ),
                panels.target(
                    "histogram_quantile(0.90, sum(irate(state_store_report_compaction_task_latency_bucket[1m])) by (le, job, instance))", "report_compaction_task_latency_p90 - {{instance}}"
                ),
            ]),
        ]),
    ]


dashboard = Dashboard(
    title="risingwave_dashboard",
    description="RisingWave Dashboard",
    tags=[
        'risingwave'
    ],
    timezone="browser",
    editable=True,
    uid="Ecy3uV1nz",
    time=Time(start="now-30m", end="now"),
    sharedCrosshair=True,
    panels=[
        *section_cluster_node(panels),
        *section_compaction(panels),
        *section_object_storage(panels),
        *section_streaming(panels),
        *section_streaming_actors(panels),
        *section_streaming_exchange(panels),
        *section_hummock(panels),
        *section_hummock_table_comparison(panels),
        *section_hummock_compaction_comparison(panels),
        *section_grpc_hummock_version_comparison(panels),
        *section_grpc_meta_catalog_service(panels),
        *section_grpc_meta_cluster_service(panels),
        *section_grpc_meta_stream_manager(panels),
        *section_grpc_meta_hummock_manager(panels),
        *section_grpc_hummock_meta_client(panels),
    ],


).auto_panel_ids()
