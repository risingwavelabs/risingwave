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

import sys

sys.path.append("src/udf/python")  # noqa

from risingwave.udf import udf, UdfServer


@udf(input_types=["INT"], result_type="INT")
def echo(inp) -> int:
    return inp

@udf(input_types=[], result_type="INT")
def echo_42() -> int:
    return 42

if __name__ == "__main__":
    server = UdfServer(location="localhost:8816")
    server.add_function(echo)
    server.add_function(echo_42)
    server.serve()
