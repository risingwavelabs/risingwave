import sys
from datetime import datetime
from enum import Enum, auto


class LogLevel(Enum):
    DEBUG = auto()
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    HEADER = auto()
    SEPARATOR = auto()
    RESULT = auto()


class Color:
    HEADER = "\033[95m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def log(msg, level: LogLevel = LogLevel.INFO, indent: int = 0):
    """Enhanced logging function with color support and structure.

    Args:
        msg: The message to log
        level: LogLevel enum indicating the type of message
        indent: Number of spaces to indent the message
    """
    prefix = " " * indent
    timestamp = datetime.now().strftime("%H:%M:%S")

    if level == LogLevel.DEBUG:
        color = Color.BLUE
        prefix = f"{prefix}[DEBUG] "
    elif level == LogLevel.INFO:
        color = Color.GREEN
        prefix = f"{prefix}[INFO] "
    elif level == LogLevel.WARNING:
        color = Color.YELLOW
        prefix = f"{prefix}[WARN] "
    elif level == LogLevel.ERROR:
        color = Color.RED
        prefix = f"{prefix}[ERROR] "
    elif level == LogLevel.HEADER:
        color = Color.HEADER + Color.BOLD
        prefix = f"{prefix}=== "
        msg = f"{msg} ==="
    elif level == LogLevel.SEPARATOR:
        color = Color.BLUE
        msg = "=" * 40
        prefix = prefix
    elif level == LogLevel.RESULT:
        color = Color.GREEN + Color.BOLD
        prefix = f"{prefix}>>> "
    else:
        color = ""
        prefix = prefix

    # Only use colors if we're writing to a terminal
    if not sys.stdout.isatty():
        color = ""
        Color.ENDC = ""

    print(f"{color}{prefix}{msg}{Color.ENDC}", flush=True)
