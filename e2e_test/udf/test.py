import socket
import struct
import sys
from typing import Iterator, List, Optional, Tuple, Any
from decimal import Decimal

sys.path.append("src/udf/python")  # noqa

from risingwave.udf import udf, udtf, UdfServer


@udf(input_types=[], result_type="INT")
def int_42() -> int:
    return 42


@udf(input_types=["INT", "INT"], result_type="INT")
def gcd(x: int, y: int) -> int:
    while y != 0:
        (x, y) = (y, x % y)
    return x


@udf(name="gcd3", input_types=["INT", "INT", "INT"], result_type="INT")
def gcd3(x: int, y: int, z: int) -> int:
    return gcd(gcd(x, y), z)


@udf(input_types=["BYTEA"], result_type="STRUCT<VARCHAR, VARCHAR, SMALLINT, SMALLINT>")
def extract_tcp_info(tcp_packet: bytes):
    src_addr, dst_addr = struct.unpack("!4s4s", tcp_packet[12:20])
    src_port, dst_port = struct.unpack("!HH", tcp_packet[20:24])
    src_addr = socket.inet_ntoa(src_addr)
    dst_addr = socket.inet_ntoa(dst_addr)
    return src_addr, dst_addr, src_port, dst_port


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


@udf(input_types="STRUCT<JSONB[], INT>", result_type="STRUCT<JSONB[], INT>")
def jsonb_array_struct_identity(v: Tuple[List[Any], int]) -> Tuple[List[Any], int]:
    return v


ALL_TYPES = "BOOLEAN,SMALLINT,INT,BIGINT,FLOAT4,FLOAT8,DECIMAL,DATE,TIME,TIMESTAMP,INTERVAL,VARCHAR,BYTEA,JSONB"


@udf(
    input_types=ALL_TYPES.split(","),
    result_type=f"struct<{ALL_TYPES}>",
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
):
    return (
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
    )


if __name__ == "__main__":
    server = UdfServer(location="0.0.0.0:8815")
    server.add_function(int_42)
    server.add_function(gcd)
    server.add_function(gcd3)
    server.add_function(series)
    server.add_function(split)
    server.add_function(extract_tcp_info)
    server.add_function(hex_to_dec)
    server.add_function(array_access)
    server.add_function(jsonb_access)
    server.add_function(jsonb_concat)
    server.add_function(jsonb_array_identity)
    server.add_function(jsonb_array_struct_identity)
    server.add_function(return_all)
    server.serve()
