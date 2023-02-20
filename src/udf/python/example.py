from risingwave.udf import udf, UdfServer
import random


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


if __name__ == '__main__':
    server = UdfServer()
    server.add_function(random_int)
    server.add_function(gcd)
    server.add_function(gcd3)
    server.serve()
