from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Kinesis Metrics",
            [
                panels.timeseries_count(
                    "Kinesis Throughput Exceeded Error count",
                    "Kinesis Throughput Exceeded Error count by source_id, source_name, fragment_id and shard_id",
                    [
                        panels.target(
                             f"sum(rate({metric('kinesis_throughput_exceeded_count')}[$__rate_interval])) by (source_id, source_name, fragment_id, shard_id)",
                            "source={{source_id}} source_name={{source_name}} fragment_id={{fragment_id}} shard_id={{shard_id}}",
                        ),
                    ]
                ),
                panels.timeseries_count(
                    "Kinesis Timeout Error count",
                    "Kinesis Timeout Error count by source_id, source_name, fragment_id and shard_id",
                    [
                        panels.target(
                            f"sum(rate({metric('kinesis_timeout_count')}[$__rate_interval])) by (source_id, source_name, fragment_id, shard_id)",
                            "source={{source_id}} source_name={{source_name}} fragment_id={{fragment_id}} shard_id={{shard_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Kinesis Rebuild Shard Iter Error count",
                    "Kinesis Rebuild Shard Iter Error count by source_id, source_name, fragment_id and shard_id",
                    [
                        panels.target(
                            f"sum(rate({metric('kinesis_rebuild_shard_iter_count')}[$__rate_interval])) by (source_id, source_name, fragment_id, shard_id)",
                            "source={{source_id}} source_name={{source_name}} fragment_id={{fragment_id}} shard_id={{shard_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Kinesis Early Terminate Shard Error count",
                    "Kinesis Early Terminate Shard Error count by source_id, source_name, fragment_id and shard_id",
                    [
                        panels.target(
                            f"sum(rate({metric('kinesis_early_terminate_shard_count')}[$__rate_interval])) by (source_id, source_name, fragment_id, shard_id)",
                            "source={{source_id}} source_name={{source_name}} fragment_id={{fragment_id}} shard_id={{shard_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Kinesis Lag Latency",
                    "Kinesis Lag Latency in ms by source_id, source_name, fragment_id and shard_id",
                    [
                        panels.target(
                            f"{metric('kinesis_lag_latency_ms')}",
                            "source={{source_id}} source_name={{source_name}} fragment_id={{fragment_id}} shard_id={{shard_id}}",
                        )
                    ],
                ),
            ],
        )
    ]
