import pathlib

import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet

# Reference: https://arrow.apache.org/cookbook/py/flight.html#simple-parquet-storage-service-with-arrow-flight


class FlightServer(pa.flight.FlightServerBase):

    def __init__(self, location="grpc://0.0.0.0:8815", **kwargs):
        super(FlightServer, self).__init__(location, **kwargs)
        self._location = location

    def do_exchange(self, context, descriptor, reader, writer):
        """Run a simple echo server."""
        started = False
        for chunk in reader:
            print(pa.Table.from_batches([chunk.data]))
            if not started and chunk.data:
                writer.begin(chunk.data.schema)
                started = True
            if chunk.app_metadata and chunk.data:
                writer.write_with_metadata(chunk.data, chunk.app_metadata)
            elif chunk.app_metadata:
                writer.write_metadata(chunk.app_metadata)
            elif chunk.data:
                writer.write_batch(chunk.data)
            else:
                assert False, "Should not happen"


if __name__ == '__main__':
    server = FlightServer()
    server.serve()
