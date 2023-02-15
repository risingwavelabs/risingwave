from risingwave.udf import udf, UdfServer
import pyarrow as pa


@udf(input_types=[pa.int32(), pa.int32()], result_type=pa.int32())
def gcd(x: int, y: int) -> int:
    while y != 0:
        (x, y) = (y, x % y)
    return x


if __name__ == '__main__':
    server = UdfServer()
    server.add_function(gcd)
    server.serve()
