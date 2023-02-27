import sys
sys.path.append('src/udf/python')  # noqa

from risingwave.udf import udf, UdfServer


@udf(input_types=[], result_type='INT')
def int_42() -> int:
    return 42


@udf(input_types=['INT', 'INT'], result_type='INT')
def gcd(x: int, y: int) -> int:
    while y != 0:
        (x, y) = (y, x % y)
    return x


@udf(name='gcd', input_types=['INT', 'INT', 'INT'], result_type='INT')
def gcd3(x: int, y: int, z: int) -> int:
    return gcd(gcd(x, y), z)


if __name__ == '__main__':
    server = UdfServer()
    server.add_function(int_42)
    server.add_function(gcd)
    server.add_function(gcd3)
    server.serve()
