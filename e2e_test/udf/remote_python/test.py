# Copyright 2025 RisingWave Labs
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

import socket
import struct
import sys
import time
import random
from typing import Iterator, List, Optional, Tuple, Any
from decimal import Decimal

from arrow_udf import udf, udtf, UdfServer


@udf(input_types=[], result_type="INT")
def int_42() -> int:
    return 42


@udf(input_types=["INT"], result_type="INT")
def sleep(s: int) -> int:
    time.sleep(s)
    return 0


@udf(input_types=["INT", "INT"], result_type="INT")
def gcd(x: int, y: int) -> int:
    while y != 0:
        (x, y) = (y, x % y)
    return x


@udf(name="gcd3", input_types=["INT", "INT", "INT"], result_type="INT")
def gcd3(x: int, y: int, z: int) -> int:
    return gcd(gcd(x, y), z)


@udf(
    input_types=["BYTEA"],
    result_type="STRUCT<src_addr: VARCHAR, dst_addr: VARCHAR, src_port: SMALLINT, dst_port: SMALLINT>",
)
def extract_tcp_info(tcp_packet: bytes):
    src_addr, dst_addr = struct.unpack("!4s4s", tcp_packet[12:20])
    src_port, dst_port = struct.unpack("!HH", tcp_packet[20:24])
    src_addr = socket.inet_ntoa(src_addr)
    dst_addr = socket.inet_ntoa(dst_addr)
    return {
        "src_addr": src_addr,
        "dst_addr": dst_addr,
        "src_port": src_port,
        "dst_port": dst_port,
    }


@udtf(input_types="INT", result_types="INT")
def series(n: int) -> Iterator[int]:
    for i in range(n):
        yield i


@udtf(input_types="VARCHAR", result_types=["VARCHAR", "INT"])
def split(string: str) -> Iterator[Tuple[str, int]]:
    for s in string.split(" "):
        yield s, len(s)


@udf(input_types="VARCHAR", result_type="DECIMAL")
def hex_to_dec(hex: Optional[str]) -> Optional[Decimal]:
    if not hex:
        return None

    hex = hex.strip()
    dec = Decimal(0)

    while hex:
        chunk = hex[:16]
        chunk_value = int(hex[:16], 16)
        dec = dec * (1 << (4 * len(chunk))) + chunk_value
        hex = hex[16:]
    return dec


@udf(input_types=["FLOAT64"], result_type="DECIMAL")
def float_to_decimal(f: float) -> Decimal:
    return Decimal(f)


@udf(input_types=["DECIMAL", "DECIMAL"], result_type="DECIMAL")
def decimal_add(a: Decimal, b: Decimal) -> Decimal:
    return a + b


@udf(input_types=["VARCHAR[]", "INT"], result_type="VARCHAR")
def array_access(list: List[str], idx: int) -> Optional[str]:
    if idx == 0 or idx > len(list):
        return None
    return list[idx - 1]


@udf(input_types=["JSONB", "INT"], result_type="JSONB")
def jsonb_access(json: Any, i: int) -> Any:
    if not json:
        return None
    return json[i]


@udf(input_types=["JSONB[]"], result_type="JSONB")
def jsonb_concat(list: List[Any]) -> Any:
    if not list:
        return None
    return list


@udf(input_types="JSONB[]", result_type="JSONB[]")
def jsonb_array_identity(list: List[Any]) -> List[Any]:
    return list


@udf(
    input_types="STRUCT<v: JSONB[], len: INT>",
    result_type="STRUCT<v: JSONB[], len: INT>",
)
def jsonb_array_struct_identity(v: Tuple[List[Any], int]) -> Tuple[List[Any], int]:
    return v


@udf(
    input_types=[
        "boolean",
        "int16",
        "int32",
        "int64",
        "float32",
        "float64",
        "decimal",
        "date32",
        "time64",
        "timestamp",
        "interval",
        "string",
        "binary",
        "json",
        "struct<f1:int, f2:int>",
    ],
    result_type="""struct<
        boolean: boolean,
        int16: int16,
        int32: int32,
        int64: int64,
        float32: float32,
        float64: float64,
        decimal: decimal,
        date32: date32,
        time64: time64,
        timestamp: timestamp,
        interval: interval,
        string: string,
        binary: binary,
        json: json,
        struct: struct<f1:int, f2:int>,
    >""",
)
def return_all(
    bool,
    i16,
    i32,
    i64,
    f32,
    f64,
    decimal,
    date,
    time,
    timestamp,
    interval,
    varchar,
    bytea,
    jsonb,
    struct,
):
    return {
        "boolean": bool,
        "int16": i16,
        "int32": i32,
        "int64": i64,
        "float32": f32,
        "float64": f64,
        "decimal": decimal,
        "date32": date,
        "time64": time,
        "timestamp": timestamp,
        "interval": interval,
        "string": varchar,
        "binary": bytea,
        "json": jsonb,
        "struct": struct,
    }


@udf(
    input_types=[
        "boolean[]",
        "int16[]",
        "int32[]",
        "int64[]",
        "float32[]",
        "float64[]",
        "decimal[]",
        "date32[]",
        "time64[]",
        "timestamp[]",
        "interval[]",
        "string[]",
        "binary[]",
        "json[]",
        "struct<f1:int, f2:int>[]",
    ],
    result_type="""struct<
        boolean: boolean[],
        int16: int16[],
        int32: int32[],
        int64: int64[],
        float32: float32[],
        float64: float64[],
        decimal: decimal[],
        date32: date32[],
        time64: time64[],
        timestamp: timestamp[],
        interval: interval[],
        string: string[],
        binary: binary[],
        json: json[],
        struct: struct<f1:int, f2:int>[],
    >""",
)
def return_all_arrays(
    bool,
    i16,
    i32,
    i64,
    f32,
    f64,
    decimal,
    date,
    time,
    timestamp,
    interval,
    varchar,
    bytea,
    jsonb,
    struct,
):
    return {
        "boolean": bool,
        "int16": i16,
        "int32": i32,
        "int64": i64,
        "float32": f32,
        "float64": f64,
        "decimal": decimal,
        "date32": date,
        "time64": time,
        "timestamp": timestamp,
        "interval": interval,
        "string": varchar,
        "binary": bytea,
        "json": jsonb,
        "struct": struct,
    }


@udf(input_types=[], result_type="INT")
def random_int() -> int:
    return random.randint(0, 100)


if __name__ == "__main__":
    server = UdfServer(location="localhost:8815")
    server.add_function(int_42)
    server.add_function(sleep)
    server.add_function(gcd)
    server.add_function(gcd3)
    server.add_function(series)
    server.add_function(split)
    server.add_function(extract_tcp_info)
    server.add_function(hex_to_dec)
    server.add_function(float_to_decimal)
    server.add_function(decimal_add)
    server.add_function(array_access)
    server.add_function(jsonb_access)
    server.add_function(jsonb_concat)
    server.add_function(jsonb_array_identity)
    server.add_function(jsonb_array_struct_identity)
    server.add_function(return_all)
    server.add_function(return_all_arrays)
    server.add_function(random_int)
    server.serve()
