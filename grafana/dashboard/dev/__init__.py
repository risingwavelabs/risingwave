from typing import Callable, List
from ..common import *


sections: List[Callable[[Panels], list]] = []


SECTION_HEADER_HEIGHT = 1.3

def section(func: Callable[[Panels], list]):
    sections.append(func)
    return func


def add_section_header(title: str):
    def _header(panels: Panels):
        return [
            panels.subheader(
                content=f"##### **{title}**",
                height=SECTION_HEADER_HEIGHT,
            )
        ]

    sections.append(_header)


# NOTE: The import order determines the order of the sections in the dashboard.

# Cluster
add_section_header("Cluster")
from . import cluster_alerts as _
from . import cluster_errors as _
from . import cluster_essential as _

# Metadata
add_section_header("Metadata")
from . import streaming_metadata as _
from . import system_params as _

# Streaming
add_section_header("Streaming")
from . import streaming_backfill as _
from . import streaming_relations as _
from . import streaming_fragments as _
from . import streaming_operators_overview as _
from . import streaming_operators_by_operator as _
from . import streaming_barrier as _

# Source
add_section_header("Source")
from . import source_general as _
from . import refresh_manager as _
from . import streaming_cdc as _
from . import kinesis_metrics as _
from . import kafka_metrics as _

# Sink
add_section_header("Sink")
from . import sink_metrics as _

# Iceberg
add_section_header("Iceberg")
from . import iceberg_metrics as _
from . import iceberg_compaction_metrics as _

# Batch
add_section_header("Batch")
from . import batch as _

# Storage
add_section_header("Storage")
from . import compaction as _
from . import object_storage as _
from . import hummock_read as _
from . import hummock_write as _
from . import hummock_tiered_cache as _
from . import hummock_manager as _
from . import sync_logstore_metrics as _

# Meta
add_section_header("Meta")
from . import backup_manager as _
from . import grpc_meta as _

# Misc
add_section_header("Misc")
from . import udf as _
from . import vector_search as _
from . import network_connection as _
from . import memory_manager as _

def generate_panels(panels: Panels):
    return [x for s in sections for x in s(panels)]
