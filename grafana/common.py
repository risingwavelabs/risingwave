from grafanalib.core import Dashboard, TimeSeries, Target, GridPos, RowPanel, Time, Templating, Table
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

    common_options = {
        "fillOpacity": 10,
        "interval": "1s",
        "maxDataPoints": 1000,
    }

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

    def table_target(self, expr, hide=False):
        return Target(expr=expr,
                      datasource=self.datasource,
                      hide=hide,
                      instant=True,
                      format='table')

    def timeseries(self, title, description, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            **self.common_options,
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
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
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
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
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
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
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
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
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
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
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
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
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
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
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
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_row(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="row",
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_ms(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
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
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
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
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_ops(self, title, description, targets, legendCols=["mean"]):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="ops",
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
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
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
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
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
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
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
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
            legendDisplayMode="table",
            legendPlacement="right",
            legendCalcs=legendCols,
            **self.common_options,
        )

    def timeseries_actor_rowsps(self, title, description, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="rows/s",
            legendDisplayMode="table",
            legendPlacement="right",
            **self.common_options,
        )

    def timeseries_memory(self, title, description, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="bytes",
            **self.common_options,
        )

    def timeseries_cpu(self, title, description, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="percentunit",
            **self.common_options,
        )

    def timeseries_latency_small(self, title, description, targets):
        gridPos = self.layout.next_one_third_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            unit="s",
            **self.common_options,
        )

    def timeseries_id(self, title, description, targets):
        gridPos = self.layout.next_half_width_graph()
        return TimeSeries(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            legendDisplayMode="table",
            legendPlacement="right",
            **self.common_options,
        )

    def table_info(self, title, description, targets, excluded_columns):
        gridPos = self.layout.next_half_width_graph()
        excludedByName = dict.fromkeys(excluded_columns, True)
        transformations = [{"id": "organize", "options": {
            "excludeByName": excludedByName}}]
        return Table(
            title=title,
            description=description,
            targets=targets,
            gridPos=gridPos,
            showHeader=True,
            filterable=True,
            transformations=transformations
        )

    def sub_panel(self):
        return Panels(self.datasource)


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