import socket
import struct
import sys
from typing import Iterator
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


@udtf(input_types='INT', result_types='INT')
def series(n: int) -> Iterator[int]:
    for i in range(n):
        yield i


@udtf(input_types=['BINARY'], result_types=['VARCHAR', 'VARCHAR', 'SMALLINT', 'SMALLINT'])
def extract_tcp_info(tcp_packet: bytes) -> Iterator:
    src_addr, dst_addr = struct.unpack('!4s4s', tcp_packet[12:20])
    src_port, dst_port = struct.unpack('!HH', tcp_packet[20:24])
    src_addr = socket.inet_ntoa(src_addr)
    dst_addr = socket.inet_ntoa(dst_addr)
    yield src_addr, dst_addr, src_port, dst_port


if __name__ == '__main__':
    server = UdfServer()
    server.add_function(int_42)
    server.add_function(gcd)
    server.add_function(gcd3)
    server.add_function(series)
    server.add_function(extract_tcp_info)
    server.serve()
