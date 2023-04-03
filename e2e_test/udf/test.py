import socket
import struct
import sys
from typing import Iterator, Optional, Tuple
from decimal import Decimal
sys.path.append('src/udf/python')  # noqa

from risingwave.udf import udf, udtf, UdfServer


@udf(input_types=[], result_type='INT')
def int_42() -> int:
    return 42


@udf(input_types=['INT', 'INT'], result_type='INT')
def gcd(x: int, y: int) -> int:
    while y != 0:
        (x, y) = (y, x % y)
    return x


@udf(input_types=['INT', 'INT', 'INT'], result_type='INT')
def gcd3(x: int, y: int, z: int) -> int:
    return gcd(gcd(x, y), z)


@udf(input_types=['BYTEA'], result_type='STRUCT<VARCHAR, VARCHAR, SMALLINT, SMALLINT>')
def extract_tcp_info(tcp_packet: bytes):
    src_addr, dst_addr = struct.unpack('!4s4s', tcp_packet[12:20])
    src_port, dst_port = struct.unpack('!HH', tcp_packet[20:24])
    src_addr = socket.inet_ntoa(src_addr)
    dst_addr = socket.inet_ntoa(dst_addr)
    return src_addr, dst_addr, src_port, dst_port


@udtf(input_types='INT', result_types='INT')
def series(n: int) -> Iterator[int]:
    for i in range(n):
        yield i


@udtf(input_types='INT', result_types=['INT', 'VARCHAR'])
def series2(n: int) -> Iterator[Tuple[int, str]]:
    for i in range(n):
        yield i, f'#{i}'


@udf(input_types='VARCHAR', result_type='DECIMAL')
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
def array_access(list: list[str], idx: int) -> Optional[str]:
    if idx == 0 or idx > len(list):
        return None
    return list[idx - 1]


if __name__ == '__main__':
    server = UdfServer()
    server.add_function(int_42)
    server.add_function(gcd)
    server.add_function(gcd3)
    server.add_function(series)
    server.add_function(series2)
    server.add_function(extract_tcp_info)
    server.add_function(hex_to_dec)
    server.add_function(array_access)
    server.serve()
