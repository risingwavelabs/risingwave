import pathlib

import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet


class FlightServer(pa.flight.FlightServerBase):
    """
    Reference: https://arrow.apache.org/cookbook/py/flight.html#simple-parquet-storage-service-with-arrow-flight
    """

    def __init__(self, location="grpc://0.0.0.0:8815", **kwargs):
        super(FlightServer, self).__init__(location, **kwargs)
        self._location = location
        self._functions = {}

    def add_function(self, name: str, func):
        """Add a function to the server."""
        self._functions[name] = func

    def do_exchange(self, context, descriptor, reader, writer):
        """Run a simple echo server."""
        func = self._functions[descriptor.path[0].decode('utf-8')]
        schema = pa.schema([
            ('c', pa.int32()),
        ])
        writer.begin(schema)
        for chunk in reader:
            print(pa.Table.from_batches([chunk.data]))
            result = self.call_func(func, chunk.data)
            writer.write_table(result)

    def call_func(self, func, batch: pa.RecordBatch) -> pa.Table:
        data = pa.array([func(batch[0][i].as_py(), batch[1][i].as_py())
                        for i in range(len(batch[0]))])
        return pa.Table.from_arrays([data], names=['c'])


def gcd(x: int, y: int) -> int:
    while y != 0:
        (x, y) = (y, x % y)
    return x


if __name__ == '__main__':
    server = FlightServer()
    server.add_function("gcd", gcd)
    server.serve()
