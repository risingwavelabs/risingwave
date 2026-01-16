from typing import Callable, List
from ..common import *


sections: List[Callable[[Panels], list]] = []


def section(func: Callable[[Panels], list]):
    sections.append(func)
    return func


# NOTE: The import order determines the order of the sections in the dashboard.

# Cluster
from . import cluster_alerts as _
from . import cluster_errors as _
from . import cluster_essential as _

# Metadata
from . import streaming_metadata as _
from . import system_params as _

# Streaming
from . import streaming_backfill as _
from . import streaming_relations as _
from . import streaming_fragments as _
from . import streaming_operators_overview as _
from . import streaming_operators_by_operator as _
from . import streaming_barrier as _

# Source
from . import source_general as _
from . import refresh_manager as _
from . import streaming_cdc as _
from . import kinesis_metrics as _
from . import kafka_metrics as _

# Sink
from . import sink_metrics as _

# Iceberg
from . import iceberg_metrics as _
from . import iceberg_compaction_metrics as _

# Batch
from . import batch as _

# Storage
from . import compaction as _
from . import object_storage as _
from . import hummock_read as _
from . import hummock_write as _
from . import hummock_tiered_cache as _
from . import hummock_manager as _
from . import sync_logstore_metrics as _

# Meta
from . import backup_manager as _
from . import grpc_meta as _

# Misc
from . import udf as _
from . import vector_search as _
from . import network_connection as _
from . import memory_manager as _

def generate_panels(panels: Panels):
    return [x for s in sections for x in s(panels)]
