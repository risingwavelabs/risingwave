from grafanalib.core import Dashboard, TimeSeries, Target, GridPos, RowPanel, Time, Templating
import logging
import os

# We use DASHBOARD_NAMESPACE_ENABLED env variable to indicate whether to add
# a filter for the namespace field in the prometheus metric.
NAMESPACE_FILTER_ENABLED = "DASHBOARD_NAMESPACE_FILTER_ENABLED"
# We use RISINGWAVE_NAME_FILTER_ENABLED env variable to indicate whether to add
# a filter for the namespace_filter field in the prometheus metric.
RISINGWAVE_NAME_FILTER_ENABLED = "DASHBOARD_RISINGWAVE_NAME_FILTER_ENABLED"
# We use DASHBOARD_SOURCE_UID env variable to pass custom source uid
SOURCE_UID = "DASHBOARD_SOURCE_UID"
# We use DASHBOARD_UID env variable to pass custom dashboard uid
DASHBOARD_UID = "DASHBOARD_UID"
# We use DASHBOARD_VERSION env variable to pass custom version
DASHBOARD_VERSION = "DASHBOARD_VERSION"

namespace_filter_enabled = os.environ.get(
    NAMESPACE_FILTER_ENABLED, "") == "true"
if namespace_filter_enabled:
    print("Enable filter for namespace field in the generated prometheus query")
risingwave_name_filter_enabled = os.environ.get(
    RISINGWAVE_NAME_FILTER_ENABLED, "") == "true"
if risingwave_name_filter_enabled:
    print("Enable filter for namespace_filter field in the generated prometheus query")
source_uid = os.environ.get(SOURCE_UID, "risedev-prometheus")
dashboard_uid = os.environ.get(DASHBOARD_UID, "Ecy3uV1nz")
dashboard_version = int(os.environ.get(DASHBOARD_VERSION, "0"))


datasource = {"type": "prometheus", "uid": f"{source_uid}"}


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

    def row(
        self,
        title,
    ):
        gridPos = self.layout.next_row()
        return RowPanel(title=title, gridPos=gridPos)

    def row_collapsed(self, title, panels):
        gridPos = self.layout.next_row()
        return RowPanel(title=title,
                        gridPos=gridPos,
                        collapsed=True,
                        panels=panels)

    def target(self, expr, legendFormat, hide=False):
        return Target(expr=expr,
                      legendFormat=legendFormat,
                      datasource=self.datasource,
                      hide=hide)

    def timeseries(self, title, description, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            fillOpacity=10,
        )

    def timeseries_count(self,
                         title,
                         description,
                         targets,
                         legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            fillOpacity=10,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_percentage(self,
                              title,
                              description,
                              targets,
                              legendCols=["mean"]):
        # Percentage should fall into 0.0-1.0
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="percentunit",
            fillOpacity=10,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_latency(self,
                           title,
                           description,
                           targets,
                           legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="s",
            fillOpacity=10,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_actor_latency(self,
                                 title,
                                 description,
                                 targets,
                                 legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="s",
            fillOpacity=0,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_actor_latency_small(self,
                                       title,
                                       description,
                                       targets,
                                       legendCols=["mean"]):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="s",
            fillOpacity=0,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_query_per_sec(self,
                                 title,
                                 description,
                                 targets,
                                 legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="Qps",
            fillOpacity=10,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_bytes_per_sec(self,
                                 title,
                                 description,
                                 targets,
                                 legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="Bps",
            fillOpacity=10,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_bytes(self,
                         title,
                         description,
                         targets,
                         legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="bytes",
            fillOpacity=10,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_row(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="row",
            fillOpacity=10,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_ms(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            fillOpacity=10,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_kilobytes(self,
                             title,
                             description,
                             targets,
                             legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="kbytes",
            fillOpacity=10,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_dollar(self,
                          title,
                          description,
                          targets,
                          legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="$",
            fillOpacity=10,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_ops(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="ops",
            fillOpacity=10,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_actor_ops(self,
                             title,
                             description,
                             targets,
                             legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="ops",
            fillOpacity=0,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_actor_ops_small(self,
                                   title,
                                   description,
                                   targets,
                                   legendCols=["mean"]):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="ops",
            fillOpacity=0,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_rowsps(self,
                          title,
                          description,
                          targets,
                          legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="rows/s",
            fillOpacity=10,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_bytesps(self,
                          title,
                          description,
                          targets,
                          legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="MB/s",
            fillOpacity=10,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
        )

    def timeseries_actor_rowsps(self, title, description, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="rows/s",
            fillOpacity=0,
            legendDisplayMode="table",
            legendPlacement="right",
        )

    def timeseries_memory(self, title, description, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="bytes",
            fillOpacity=10,
        )

    def timeseries_cpu(self, title, description, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="percentunit",
            fillOpacity=10,
        )

    def timeseries_latency_small(self, title, description, targets):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="s",
            fillOpacity=10,
        )

    def timeseries_id(self, title, description, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            fillOpacity=10,
            legendDisplayMode="table",
            legendPlacement="right",
        )

    def sub_panel(self):
        return Panels(self.datasource)


panels = Panels(datasource)

logging.basicConfig(level=logging.WARN)


def metric(name, filter=None):
    filters = [filter] if filter else []
    if namespace_filter_enabled:
        filters.append("namespace=~\"$namespace\"")
    if risingwave_name_filter_enabled:
        filters.append("risingwave_name=~\"$instance\"")
    if filters:
        return f"{name}{{{','.join(filters)}}}"
    else:
        return name


def quantile(f, percentiles):
    quantile_map = {
        "60": ["0.6", "60"],
        "50": ["0.5", "50"],
        "90": ["0.9", "90"],
        "99": ["0.99", "99"],
        "999": ["0.999", "999"],
        "max": ["1.0", "max"],
    }
    return list(
        map(lambda p: f(quantile_map[str(p)][0], quantile_map[str(p)][1]),
            percentiles))


def section_cluster_node(panels):
    return [
        panels.row("Cluster Node"),
        panels.timeseries_count(
            "Node Count",
            "",
            [
                panels.target(f"sum({metric('worker_num')}) by (worker_type)",
                              "{{worker_type}}")
            ],
            ["last"],
        ),
        panels.timeseries_memory(
            "Node Memory",
            "",
            [
                panels.target(
                    f"avg({metric('process_resident_memory_bytes')}) by (job,instance)",
                    "{{job}} @ {{instance}}",
                )
            ],
        ),
        panels.timeseries_cpu(
            "Node CPU",
            "",
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
            "",
            [
                panels.target(f"sum({metric('meta_num')}) by (worker_addr,role)",
                              "{{worker_addr}} @ {{role}}")
            ],
            ["last"],
        ),
    ]


def section_compaction(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Compaction",
            [
                panels.timeseries_count(
                    "SST Count",
                    "num of SSTs in each level",
                    [
                        panels.target(
                            f"sum({metric('storage_level_sst_num')}) by (instance, level_index)",
                            "L{{level_index}}",
                        ),
                    ],
                ),
                panels.timeseries_kilobytes(
                    "KBs level sst",
                    "KBs total file bytes in each level",
                    [
                        panels.target(
                            f"sum({metric('storage_level_total_file_size')}) by (instance, level_index)",
                            "L{{level_index}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "scale compactor core count",
                    "compactor core resource need to scale out",
                    [
                        panels.target(
                            f"sum({metric('storage_compactor_suggest_core_count')})",
                            "suggest-core-count"
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Success & Failure Count",
                    "num of compactions from each level to next level",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_frequency')}) by (compactor, group, task_type, result)",
                            "{{task_type}} - {{result}} - group-{{group}} @ {{compactor}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Skip Count",
                    "num of compaction task which does not trigger",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_skip_compact_frequency')}[$__rate_interval])) by (level, type)",
                            "{{level}}-{{type}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compactor Running Task Count",
                    "num of compactions from each level to next level",
                    [
                        panels.target(
                            f"avg({metric('storage_compact_task_pending_num')}) by(job, instance)",
                            "compactor_task_split_count - {{job}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Compaction Duration",
                    "Total time of compact that have been issued to state store",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('compactor_compact_task_duration_bucket')}[$__rate_interval])) by (le, job, instance))",
                                f"compact-task p{legend}" +
                                " - {{job}} @ {{instance}}",
                            ),
                            [50, 90, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('compactor_compact_sst_duration_bucket')}[$__rate_interval])) by (le, job, instance))",
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
                    "Compaction Write Bytes",
                    "num of SSTs written into next level during history compactions to next level",
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
                    "num of SSTs written into next level during history compactions to next level",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_write')}) / sum({metric('compactor_write_build_l0_bytes')})",
                            "write amplification",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compacting SST Count",
                    "num of SSTs to be merged to next level in each level",
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
                    "KBs Read from Next Level",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_read_next')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read",
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "KBs Read from Current Level",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_read_curr')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Count of SSTs Read from Current Level",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_read_sstn_curr')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read",
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "KBs Written to Next Level",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_write')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} write",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Count of SSTs Written to Next Level",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_write_sstn')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} write",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Count of SSTs Read from Next Level",
                    "num of SSTs read from next level during history compactions to next level",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_read_sstn_next')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read",
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
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_remote_read_time_per_task_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
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
                    "",
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
                    "",
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
    return [
        panels.row("Streaming"),
        panels.timeseries_rowsps(
            "Source Throughput(rows)",
            "",
            [
                panels.target(
                    f"rate({metric('stream_source_output_rows_counts')}[$__rate_interval])",
                    "source={{source_name}} {{source_id}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_rowsps(
            "Source Throughput(rows) Per Partition",
            "",
            [
                panels.target(
                    f"rate({metric('partition_input_count')}[$__rate_interval])",
                    "actor={{actor_id}} source={{source_id}} partition={{partition}}",
                )
            ],
        ),
        panels.timeseries_bytesps(
            "Source Throughput(bytes)",
            "",
            [
                panels.target(
                    f"(sum by (source_id)(rate({metric('partition_input_bytes')}[$__rate_interval])))/(1000*1000)",
                    "source={{source_id}}",
                )
            ],
        ),
        panels.timeseries_bytesps(
            "Source Throughput(bytes) Per Partition",
            "",
            [
                panels.target(
                    f"(rate({metric('partition_input_bytes')}[$__rate_interval]))/(1000*1000)",
                    "actor={{actor_id}} source={{source_id}} partition={{partition}}",
                )
            ],
        ),
        panels.timeseries_rowsps(
            "Source Throughput(rows) per barrier",
            "",
            [
                panels.target(
                    f"rate({metric('stream_source_rows_per_barrier_counts')}[$__rate_interval])",
                    "actor={{actor_id}} source={{source_id}} @ {{instance}}"
                )
            ]
        ),
        panels.timeseries_rowsps(
            "Backfill Snapshot Read Throughput(rows)",
            "Total number of rows that have been read from the backfill snapshot",
            [
                panels.target(
                    f"rate({metric('stream_backfill_snapshot_read_row_count')}[$__rate_interval])",
                    "table_id={{table_id}} actor={{actor_id}} @ {{instance}}"
                ),
            ],
        ),
        panels.timeseries_rowsps(
            "Backfill Upstream Throughput(rows)",
            "Total number of rows that have been output from the backfill upstream",
            [
                panels.target(
                    f"rate({metric('stream_backfill_upstream_output_row_count')}[$__rate_interval])",
                    "table_id={{table_id}} actor={{actor_id}} @ {{instance}}"
                ),
            ],
        ),
        panels.timeseries_count(
            "Barrier Number",
            "",
            [
                panels.target(f"{metric('all_barrier_nums')}", "all_barrier"),
                panels.target(
                    f"{metric('in_flight_barrier_nums')}", "in_flight_barrier"),
            ],
        ),
        panels.timeseries_latency(
            "Barrier Send Latency",
            "",
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
            "",
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
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_executor_row_count')}[$__rate_interval]) > 0",
                            "{{actor_id}}->{{executor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Actor Backpressure",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_output_buffer_blocking_duration_ns')}[$__rate_interval]) / 1000000000",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Actor Memory Usage",
                    "",
                    [
                        panels.target(
                            "rate(actor_memory_usage[$__rate_interval])",
                            "{{actor_id}}",
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
                panels.timeseries_actor_ops(
                    "Join Executor Cache",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_join_lookup_miss_count')}[$__rate_interval])",
                            "cache miss {{actor_id}} {{side}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_join_lookup_total_count')}[$__rate_interval])",
                            "total lookups {{actor_id}} {{side}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_join_insert_cache_miss_count')}[$__rate_interval])",
                            "cache miss when insert{{actor_id}} {{side}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Materialize Executor Cache",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_materialize_cache_hit_count')}[$__rate_interval])",
                            "cache miss {{actor_id}} ",
                        ),
                        panels.target(
                            f"rate({metric('stream_materialize_cache_total_count')}[$__rate_interval])",
                            "total lookups {{actor_id}}",
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
                    "",
                    [
                        panels.target(f"{metric('stream_join_cached_entries')}",
                                      "{{actor_id}} {{side}}"),
                    ],
                ),
                panels.timeseries_count(
                    "Join Cached Rows",
                    "",
                    [
                        panels.target(f"{metric('stream_join_cached_rows')}",
                                      "{{actor_id}} {{side}}"),
                    ],
                ),
                panels.timeseries_bytes(
                    "Join Cached Estimated Size",
                    "",
                    [
                        panels.target(f"{metric('stream_join_cached_estimated_size')}",
                                      "{{actor_id}} {{side}}"),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Aggregation Executor Cache Statistics For Each Key/State",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_agg_lookup_miss_count')}[$__rate_interval])",
                            "cache miss {{actor_id}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_agg_lookup_total_count')}[$__rate_interval])",
                            "total lookups {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Aggregation Executor Cache Statistics For Each StreamChunk",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_agg_chunk_lookup_miss_count')}[$__rate_interval])",
                            "chunk-level cache miss {{actor_id}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_agg_chunk_lookup_total_count')}[$__rate_interval])",
                            "chunk-level total lookups {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Aggregation Cached Keys",
                    "",
                    [
                        panels.target(f"{metric('stream_agg_cached_keys')}",
                                      "{{actor_id}}"),
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


def section_batch_exchange(outer_panels):
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
                            f"{metric('batch_task_exchange_recv_row_number')}",
                            "{{query_id}} : {{source_stage_id}}.{{source_task_id}} -> {{target_stage_id}}.{{target_task_id}}",
                        ),
                    ],
                ),
                panels.timeseries_row(
                    "Batch Mpp Task Number",
                    "",
                    [
                        panels.target(
                            f"{metric('batch_task_num')}",
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
                    "Query Per second in Loacl Execution Mode",
                    "",
                    [
                        panels.target(
                            f"rate({metric('frontend_query_counter_local_execution')}[$__rate_interval])",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_query_per_sec(
                    "Query Per second in Distributed Execution Mode",
                    "",
                    [
                        panels.target(
                            f"rate({metric('distributed_completed_query_counter')}[$__rate_interval])",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Running query in distributed execution mode",
                    "",
                    [
                        panels.target(f"{metric('distributed_running_query_num')}",
                            "The number of running query in distributed execution mode"),
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "Rejected query in distributed execution mode",
                    "",
                    [
                        panels.target(f"{metric('distributed_rejected_query_counter')}",
                            "The number of rejected query in distributed execution mode"),
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "Completed query in distributed execution mode",
                    "",
                    [
                        panels.target(f"{metric('distributed_completed_query_counter')}",
                            "The number of completed query in distributed execution mode"),
                    ],
                    ["last"],
                ),
                panels.timeseries_latency(
                    "Query Latency in Distributed Execution Mode",
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
                    "Query Latency in Local Execution Mode",
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
    mete_miss_filter = "type='meta_miss'"
    meta_total_filter = "type='meta_total'"
    data_miss_filter = "type='data_miss'"
    data_total_filter = "type='data_total'"
    file_cache_get_filter = "op='get'"
    return [
        panels.row("Hummock"),
        panels.timeseries_latency(
            "Build and Sync Sstable Duration",
            "",
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
                    f"sum(rate({metric('state_store_sst_store_block_request_counts')}[$__rate_interval])) by (job, instance, table_id, type)",
                    "{{table_id}} @ {{type}} - {{job}} @ {{instance}}",
                ),
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
                    f"sum(rate({metric('state_store_get_duration_count')}[$__rate_interval])) by (job,instanc,table_id)",
                    "get - {{table_id}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({metric('state_store_range_reverse_scan_duration_count')}[$__rate_interval])) by (job,instance)",
                    "backward scan - {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({metric('state_store_get_shared_buffer_hit_counts')}[$__rate_interval])) by (job,instance,table_id)",
                    "shared_buffer hit - {{table_id}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({metric('state_store_iter_in_process_counts')}[$__rate_interval])) by(job,instance,table_id)",
                    "iter - {{table_id}} @ {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_latency(
            "Read Duration - Get",
            "",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({metric('state_store_get_duration_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"p{legend}" + " - {{table_id}} @ {{job}} @ {{instance}}",
                    ),
                    [50, 90, 99, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance, table_id)(rate({metric('state_store_get_duration_sum')}[$__rate_interval])) / sum by(le, job, instance, table_id) (rate({metric('state_store_get_duration_count')}[$__rate_interval]))",
                    "avg - {{table_id}} {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_latency(
            "Read Duration - Iter",
            "",
            [
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({metric('state_store_iter_duration_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"create_iter_time p{legend} - {{{{table_id}}}} @ {{{{job}}}} @ {{{{instance}}}}",
                    ),
                    [90, 99, 999, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance)(rate({metric('state_store_iter_duration_sum')}[$__rate_interval])) / sum by(le, job,instance) (rate({metric('state_store_iter_duration_count')}[$__rate_interval]))",
                    "create_iter_time avg - {{job}} @ {{instance}}",
                ),
                *quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({metric('state_store_iter_scan_duration_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"pure_scan_time p{legend} - {{{{table_id}}}} @ {{{{job}}}} @ {{{{instance}}}}",
                    ),
                    [90, 99, 999, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance)(rate({metric('state_store_scan_iter_duration_sum')}[$__rate_interval])) / sum by(le, job,instance) (rate({metric('state_store_iter_scan_duration_count')}[$__rate_interval]))",
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
                        f"histogram_quantile({quantile}, sum(rate({metric('state_store_get_key_size_bucket')}[$__rate_interval])) by (le, job, instance, table_id)) + histogram_quantile({quantile}, sum(rate({metric('state_store_get_value_size_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
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
                        f"histogram_quantile({quantile}, sum(rate({metric('state_store_iter_size_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
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
                        f"histogram_quantile({quantile}, sum(rate({metric('state_store_iter_item_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"p{legend} - {{{{table_id}}}} @ {{{{job}}}} @ {{{{instance}}}}",
                    ),
                    [90, 99, 999, "max"],
                ),
            ],
        ),
        panels.timeseries_bytes_per_sec(
            "Read Throughput - Get",
            "",
            [
                panels.target(
                    f"sum(rate({metric('state_store_get_key_size_sum')}[$__rate_interval])) by(job, instance) + sum(rate({metric('state_store_get_value_size_sum')}[$__rate_interval])) by(job, instance)",
                    "{{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_bytes_per_sec(
            "Read Throughput - Iter",
            "",
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
                        f"histogram_quantile({quantile}, sum(rate({metric('state_store_may_exist_duration_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"p{legend}" + " - {{table_id}} @ {{job}} @ {{instance}}",
                    ),
                    [50, 90, 99, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance, table_id)(rate({metric('state_store_may_exist_duration_sum')}[$__rate_interval])) / sum by(le, job, instance, table_id) (rate({metric('state_store_may_exist_duration_count')}[$__rate_interval]))",
                    "avg - {{table_id}} {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_ops(
            "Read Bloom Filter",
            "",
            [
                panels.target(
                    f"sum(rate({metric('state_store_bloom_filter_true_negative_counts')}[$__rate_interval])) by (job,instance,table_id,type)",
                    "bloom filter true negative  - {{table_id}} - {{type}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({metric('state_store_read_req_positive_but_non_exist_counts')}[$__rate_interval])) by (job,instance,table_id,type)",
                    "bloom filter false positive count  - {{table_id}} - {{type}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({metric('state_store_read_req_bloom_filter_positive_counts')}[$__rate_interval])) by (job,instance,table_id,type)",
                    "read_req bloom filter positive - {{table_id}} - {{type}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by (job,instance,table_id,type)",
                    "read_req check bloom filter - {{table_id}} - {{type}} @ {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_ops(
            "Iter keys flow",
            "",
            [
                panels.target(
                    f"sum(rate({metric('state_store_iter_scan_key_counts')}[$__rate_interval])) by (instance, type, table_id)",
                    "iter keys flow - {{table_id}} @ {{type}} @ {{instance}} ",
                ),
            ],
        ),
        panels.timeseries_percentage(
            " Filter/Cache Miss Rate",
            "",
            [
                panels.target(
                    f"1 - (sum(rate({metric('state_store_bloom_filter_true_negative_counts')}[$__rate_interval])) by (job,instance,table_id,type)) / (sum(rate({metric('state_bloom_filter_check_counts')}[$__rate_interval])) by (job,instance,table_id,type))",
                    "bloom filter miss rate - {{table_id}} - {{type}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"(sum(rate({metric('state_store_sst_store_block_request_counts', mete_miss_filter)}[$__rate_interval])) by (job,instance,table_id)) / (sum(rate({metric('state_store_sst_store_block_request_counts', meta_total_filter)}[$__rate_interval])) by (job,instance,table_id))",
                    "meta cache miss rate - {{table_id}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"(sum(rate({metric('state_store_sst_store_block_request_counts', data_miss_filter)}[$__rate_interval])) by (job,instance,table_id)) / (sum(rate({metric('state_store_sst_store_block_request_counts', data_total_filter)}[$__rate_interval])) by (job,instance,table_id))",
                    "block cache miss rate - {{table_id}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"(sum(rate({metric('file_cache_miss')}[$__rate_interval])) by (instance)) / (sum(rate({metric('file_cache_latency_count', file_cache_get_filter)}[$__rate_interval])) by (instance))",
                    "file cache miss rate @ {{instance}}",
                ),

                panels.target(
                    f"1 - (((sum(rate({metric('state_store_read_req_bloom_filter_positive_counts')}[$__rate_interval])) by (job,instance,table_id,type))) / (sum(rate({metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by (job,instance,table_id,type)))",
                    "read req bloom filter filter rate - {{table_id}} - {{type}} @ {{job}} @ {{instance}}",
                ),

                panels.target(
                    f"1 - (((sum(rate({metric('state_store_read_req_positive_but_non_exist_counts')}[$__rate_interval])) by (job,instance,table_id,type))) / (sum(rate({metric('state_store_read_req_bloom_filter_positive_counts')}[$__rate_interval])) by (job,instance,table_id,type)))",
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
                        f"histogram_quantile({quantile}, sum(rate({metric('state_store_iter_merge_sstable_counts_bucket')}[$__rate_interval])) by (le, job, table_id, type))",
                        f"# merged ssts p{legend}" +
                        " - {{table_id}} @ {{job}} @ {{type}}",
                    ),
                    [90, 99, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance, table_id)(rate({metric('state_store_iter_merge_sstable_counts_sum')}[$__rate_interval]))  / sum by(le, job, instance, table_id)(rate({metric('state_store_iter_merge_sstable_counts_count')}[$__rate_interval]))",
                    "# merged ssts avg  - {{table_id}} @ {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_ops(
            "Write Ops",
            "",
            [
                panels.target(
                    f"sum(rate({metric('state_store_write_batch_duration_count')}[$__rate_interval])) by (job,instance,table_id)",
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
                        f"histogram_quantile({quantile}, sum(rate({metric('state_store_write_batch_duration_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"write to shared_buffer p{legend}" +
                        " - {{table_id}} @ {{job}} @ {{instance}}",
                    ),
                    [50, 90, 99, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance, table_id)(rate({metric('state_store_write_batch_duration_sum')}[$__rate_interval]))  / sum by(le, job, instance, table_id)(rate({metric('state_store_write_batch_duration_count')}[$__rate_interval]))",
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
                    f"sum(rate({metric('state_store_write_batch_tuple_counts')}[$__rate_interval])) by (job,instance,table_id)",
                    "write_batch_kv_pair_count - {{table_id}} @ {{instance}} ",
                ),
            ],
        ),
        panels.timeseries_bytes_per_sec(
            "Write Throughput",
            "",
            [
                panels.target(
                    f"sum(rate({metric('state_store_write_batch_size_sum')}[$__rate_interval]))by(job,instance) / sum(rate({metric('state_store_write_batch_size_count')}[$__rate_interval]))by(job,instance,table_id)",
                    "shared_buffer - {{table_id}} @ {{job}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({metric('compactor_shared_buffer_to_sstable_size')}[$__rate_interval]))by(job,instance) / sum(rate({metric('state_store_shared_buffer_to_sstable_size_count')}[$__rate_interval]))by(job,instance)",
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
            "",
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
                    f"sum({metric('state_store_limit_memory_size')}) by (job)",
                    "uploading memory - {{job}}",
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
                        f"histogram_quantile({quantile}, sum(rate({metric('state_store_iter_fetch_meta_duration_bucket')}[$__rate_interval])) by (le, job, instance, table_id))",
                        f"fetch_meta_duration p{legend}" +
                        " - {{table_id}} @ {{job}} @ {{instance}}",
                    ),
                    [50, 90, 99, "max"],
                ),
                panels.target(
                    f"sum by(le, job, instance, table_id) (rate({metric('state_store_iter_fetch_meta_duration_sum')}[$__rate_interval])) / sum by(le, job, instance, table_id) (rate({metric('state_store_iter_fetch_meta_duration_count')}[$__rate_interval]))",
                    "fetch_meta_duration avg - {{table_id}} @ {{job}} @ {{instance}}",
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
                        panels.target(f"{metric('storage_version_stats', total_key_size_filter)}/1024",
                                      "table{{table_id}} {{metric}}"),
                        panels.target(f"{metric('storage_version_stats', total_value_size_filter)}/1024",
                                      "table{{table_id}} {{metric}}"),
                    ],
                ),
                panels.timeseries_count(
                    "Table KV Count",
                    "",
                    [
                        panels.target(f"{metric('storage_version_stats', total_key_count_filter)}",
                                      "table{{table_id}} {{metric}}"),
                    ],
                ),
                panels.timeseries_count(
                    "Stale SST Total Number",
                    "total number of SSTs that is no longer referenced by versions but is not yet deleted from storage",
                    [
                        panels.target(f"{metric('storage_stale_ssts_count')}",
                                      "stale SST total number"),
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
                    "",
                    [
                        panels.target(
                            f"{metric('backup_job_count')}",
                            "job count",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Job Process Time",
                    "",
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
                    "The memory allocated by jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_allocated_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The memory allocated by streaming",
                    "",
                    [
                        panels.target(
                            f"{metric('stream_total_mem_usage')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The memory allocated by batch",
                    "",
                    [
                        panels.target(
                            f"{metric('batch_total_mem_usage')}",
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
            ],
        )
    ]

templating = Templating()
if namespace_filter_enabled:
    templating = Templating(
        list=[
            {
                "definition": "label_values(up{risingwave_name=~\".+\"}, namespace)",
                "description": "Kubernetes namespace.",
                "hide": 0,
                "includeAll": False,
                "label": "Namespace",
                "multi": True,
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
                "type": "query"
            }
        ]
    )

dashboard = Dashboard(
    title="risingwave_dashboard",
    description="RisingWave Dashboard",
    tags=["risingwave"],
    timezone="browser",
    editable=True,
    uid=dashboard_uid,
    time=Time(start="now-30m", end="now"),
    sharedCrosshair=True,
    templating=templating,
    version=dashboard_version,
    panels=[
        *section_cluster_node(panels),
        *section_streaming(panels),
        *section_streaming_actors(panels),
        *section_streaming_exchange(panels),
        *section_streaming_errors(panels),
        *section_batch_exchange(panels),
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
