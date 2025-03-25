from typing import Callable, List
from ..common import *


sections: List[Callable[[Panels], list]] = []


def section(func: Callable[[Panels], list]):
    sections.append(func)
    return func


# The import order determines the order of the sections in the dashboard.
from . import actor_info as _
from . import overview as _
from . import cpu as _
from . import memory as _
from . import network as _
from . import storage as _
from . import streaming as _
from . import batch as _


def generate_panels(panels: Panels):
    return [x for s in sections for x in s(panels)]
