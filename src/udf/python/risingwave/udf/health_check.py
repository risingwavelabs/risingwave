# Copyright 2023 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyarrow.flight import FlightClient
import sys


def check_udf_service_available(addr: str) -> bool:
    """Check if the UDF service is available at the given address."""
    try:
        client = FlightClient(f"grpc://{addr}")
        client.wait_for_available()
        return True
    except Exception as e:
        print(f"Error connecting to RisingWave UDF service: {str(e)}")
        return False


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python3 health_check.py <server_address>")
        sys.exit(1)

    server_address = sys.argv[1]
    if check_udf_service_available(server_address):
        print("OK")
    else:
        print("unavailable")
        exit(-1)
