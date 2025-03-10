from typing import Callable, List
from ..common import *


sections: List[Callable[[Panels], list]] = []


def section(func: Callable[[Panels], list]):
    sections.append(func)
    return func


# The import order determines the order of the sections in the dashboard.
from . import actor_info as _
from . import cluster_essential as _
from . import streaming as _
from . import streaming_cdc as _
from . import streaming_actors as _
from . import streaming_actors_tokio as _
from . import streaming_exchange as _
from . import batch as _
from . import hummock_read as _
from . import hummock_write as _
from . import compaction as _
from . import object_storage as _
from . import hummock_tiered_cache as _
from . import hummock_manager as _
from . import backup_manager as _
from . import grpc_meta as _
from . import memory_manager as _
from . import sink_metrics as _
from . import kafka_metrics as _
from . import network_connection as _
from . import iceberg_metrics as _
from . import udf as _


def generate_panels(panels: Panels):
    return [x for s in sections for x in s(panels)]
