from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    mv_total_size_filter = "metric='materialized_view_total_size'"
    return [
        outer_panels.row_collapsed(
            "Storage",
            [
                panels.timeseries_bytes(
                    "Object Size",
                    """
                    Objects are classified into 3 groups:
                    - not referenced by versions: these object are being deleted from object store.
                    - referenced by non-current versions: these objects are stale (not in the latest version), but those old versions may still be in use (e.g. long-running pinning). Thus those objects cannot be deleted at the moment.
                    - referenced by current version: these objects are in the latest version.
                    """,
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
                    ],
                ),
                panels.timeseries_kilobytes(
                    "Materialized View Size",
                    "The storage size of each materialized view",
                    [
                        panels.target(
                            f"{metric('storage_materialized_view_stats', mv_total_size_filter)}/1024",
                            "{{metric}}, mv id - {{table_id}} ",
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
                    ],
                ),
                panels.timeseries_bytes(
                    "Write Bytes",
                    "The number of bytes that have been written by compaction."
                    "Flush refers to the process of compacting Memtables to SSTables at Level 0."
                    "Compaction refers to the process of compacting SSTables at one level to another level.",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_write')}) by ({COMPONENT_LABEL}) > 0",
                            "Compaction - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum({metric('compactor_write_build_l0_bytes')}) by ({COMPONENT_LABEL}) > 0",
                            "Flush - {{%s}}" % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Storage Remote I/O (Bytes/s)",
                    "The remote storage read/write throughput",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_read_bytes')}[$__rate_interval])) by ({COMPONENT_LABEL})",
                            "read - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_write_bytes')}[$__rate_interval])) by ({COMPONENT_LABEL})",
                            "write - {{%s}}" % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Checkpoint Size",
                    "Size statistics for checkpoint",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_sync_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}))",
                                f"p{legend}" + " - {{%s}}" % COMPONENT_LABEL,
                            ),
                            [50, 99],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}) (rate({metric('state_store_sync_size_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}) (rate({metric('state_store_sync_size_count')}[$__rate_interval])) > 0",
                            "avg - {{%s}}" % COMPONENT_LABEL,
                        ),
                    ],
                ),
            ],
        )
    ]
