import platform
import os
from distutils import util

# check environment
if 'arm64' in platform.machine() and 'mac' in util.get_platform():
    print("Found m1 chip")
    os.environ["GRPC_PYTHON_BUILD_SYSTEM_OPENSSL"] = "1"
    os.environ["GRPC_PYTHON_BUILD_SYSTEM_ZLIB"] = "1"
