# RisingWave Python API

This library provides a Python API for creating user-defined functions (UDF) in RisingWave.

Currently, RisingWave supports user-defined functions implemented as external functions.
Users need to define functions using the API provided by this library, and then start a Python process as a UDF server.
RisingWave calls the function remotely by accessing the UDF server at a given address.

## Installation

```sh
pip install risingwave
```

## Usage

Define functions in a Python file:

```python
# udf.py
from risingwave.udf import udf, udtf, UdfServer

# Define a scalar function
@udf(input_types=['INT', 'INT'], result_type='INT')
def gcd(x, y):
    while y != 0:
        (x, y) = (y, x % y)
    return x

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
