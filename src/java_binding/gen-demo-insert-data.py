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


def gen_row(index):
    v1 = int(index)
    v2 = int(index)
    v3 = int(index)
    v4 = float(index)
    v5 = float(index)
    v6 = index % 3 == 0
    v7 = "'" + str(index) * ((index % 10) + 1) + "'"
    v8 = "to_timestamp(" + str(index) + ")"
    v9 = index
    may_null = None if index % 5 == 0 else int(index)
    row_data = [v1, v2, v3, v4, v5, v6, v7, v8, v9, may_null]
    repr = [str(o) if o is not None else "null" for o in row_data]
    return "(" + ", ".join(repr) + ")"


data_size = int(sys.argv[1])
data = [gen_row(i) for i in range(data_size)]
print(", ".join(data))
