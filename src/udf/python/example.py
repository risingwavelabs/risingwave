import socket
from typing import Iterator
from risingwave.udf import udf, udtf, UdfServer
import random
import struct


@udf(input_types=[], result_type='INT')
def random_int() -> int:
    return random.randint(0, 100)


@udf(input_types=['INT', 'INT'], result_type='INT')
def gcd(x: int, y: int) -> int:
    while y != 0:
        (x, y) = (y, x % y)
    return x


@udf(name='gcd3', input_types=['INT', 'INT', 'INT'], result_type='INT')
def gcd3(x: int, y: int, z: int) -> int:
    return gcd(gcd(x, y), z)


@udtf(input_types='INT', result_types='INT')
def series(n: int) -> Iterator[int]:
    for i in range(n):
        yield i


@udtf(input_types=['INT'], result_types=['INT', 'VARCHAR'])
def series2(n: int) -> Iterator[tuple[int, str]]:
    for i in range(n):
        yield i, str(i)


@udf(input_types=['BINARY'], result_type='STRUCT<src_ip VARCHAR, dst_ip VARCHAR, src_port SMALLINT, dst_port SMALLINT>')
def extract_tcp_info(tcp_packet: bytes):
    src_addr, dst_addr = struct.unpack('!4s4s', tcp_packet[12:20])
    src_port, dst_port = struct.unpack('!HH', tcp_packet[20:24])
    src_addr = socket.inet_ntoa(src_addr)
    dst_addr = socket.inet_ntoa(dst_addr)
    return src_addr, dst_addr, src_port, dst_port


if __name__ == '__main__':
    server = UdfServer(location="0.0.0.0:8815")
    server.add_function(random_int)
    server.add_function(gcd)
    server.add_function(gcd3)
    server.add_function(series)
    server.add_function(series2)
    server.add_function(extract_tcp_info)
    server.serve()
