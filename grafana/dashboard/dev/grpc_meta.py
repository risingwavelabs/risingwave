from ..common import *
from . import section


def grpc_metrics_target(panels: Panels, name: str, filter: str):
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
                f"sum(irate({metric('meta_grpc_duration_seconds_sum', filter)}[$__rate_interval])) / sum(irate({metric('meta_grpc_duration_seconds_count', filter)}[$__rate_interval])) > 0",
                f"{name}_avg",
            ),
        ],
    )


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "gRPC Meta",
            [
                panels.subheader("Catalog Service"),
                grpc_metrics_target(
                    panels, "Create", "path='/meta.CatalogService/Create'"
                ),
                grpc_metrics_target(panels, "Drop", "path='/meta.CatalogService/Drop'"),
                grpc_metrics_target(
                    panels, "GetCatalog", "path='/meta.CatalogService/GetCatalog'"
                ),
                panels.subheader("Cluster Service"),
                grpc_metrics_target(
                    panels, "AddWorkerNode", "path='/meta.ClusterService/AddWorkerNode'"
                ),
                grpc_metrics_target(
                    panels, "ListAllNodes", "path='/meta.ClusterService/ListAllNodes'"
                ),
                panels.subheader("Stream Manager Service"),
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
        )
    ]
