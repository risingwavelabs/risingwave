# RisingWave Python API

This library provides a Python API for creating user-defined functions (UDF) in RisingWave.

## Introduction

RisingWave supports user-defined functions implemented as external functions.
With the RisingWave Python UDF SDK, users can define custom UDFs using Python and start a Python process as a UDF server.
RisingWave can then remotely access the UDF server to execute the defined functions.

## Installation

```sh
pip install risingwave
```

## Usage

Define functions in a Python file:

```python
# udf.py
from risingwave.udf import udf, udtf, UdfServer
import struct
import socket

# Define a scalar function
@udf(input_types=['INT', 'INT'], result_type='INT')
def gcd(x, y):
    while y != 0:
        (x, y) = (y, x % y)
    return x

# Define a scalar function that returns multiple values (within a struct)
@udf(input_types=['BYTEA'], result_type='STRUCT<VARCHAR, VARCHAR, SMALLINT, SMALLINT>')
def extract_tcp_info(tcp_packet: bytes):
    src_addr, dst_addr = struct.unpack('!4s4s', tcp_packet[12:20])
    src_port, dst_port = struct.unpack('!HH', tcp_packet[20:24])
    src_addr = socket.inet_ntoa(src_addr)
    dst_addr = socket.inet_ntoa(dst_addr)
    return src_addr, dst_addr, src_port, dst_port

# Define a table function
@udtf(input_types='INT', result_types='INT')
def series(n):
    for i in range(n):
        yield i

# Start a UDF server
if __name__ == '__main__':
    server = UdfServer(location="0.0.0.0:8815")
    server.add_function(gcd)
    server.add_function(series)
    server.serve()
```

Start the UDF server:

```sh
python3 udf.py
```

To create functions in RisingWave, use the following syntax:

```sql
create function <name> ( <arg_type>[, ...] )
    [ returns <ret_type> | returns table ( <column_name> <column_type> [, ...] ) ]
    language python as <name_defined_in_server>
    using link '<udf_server_address>';
```

- The `language` parameter must be set to `python`.
- The `as` parameter specifies the function name defined in the UDF server.
- The `link` parameter specifies the address of the UDF server.

For example:

```sql
create function gcd(int, int) returns int
language python as gcd using link 'http://localhost:8815';

create function series(int) returns table (x int)
language python as series using link 'http://localhost:8815';

select gcd(25, 15);

select * from series(10);
```

## Data Types

The RisingWave Python UDF SDK supports the following data types:

| SQL Type         | Python Type                    | Notes              |
| ---------------- | -----------------------------  | ------------------ |
| BOOLEAN          | bool                           |                    |
| SMALLINT         | int                            |                    |
| INT              | int                            |                    |
| BIGINT           | int                            |                    |
| REAL             | float                          |                    |
| DOUBLE PRECISION | float                          |                    |
| DECIMAL          | decimal.Decimal                |                    |
| DATE             | datetime.date                  |                    |
| TIME             | datetime.time                  |                    |
| TIMESTAMP        | datetime.datetime              |                    |
| INTERVAL         | MonthDayNano / (int, int, int) | Fields can be obtained by `months()`, `days()` and `nanoseconds()` from `MonthDayNano` |
| VARCHAR          | str                            |                    |
| BYTEA            | bytes                          |                    |
| JSONB            | any                            |                    |
| T[]              | list[T]                        |                    |
| STRUCT<>         | tuple                          |                    |
| ...others        |                                | Not supported yet. |
