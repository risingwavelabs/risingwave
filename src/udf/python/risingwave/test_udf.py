from multiprocessing import Process
import pytest
from risingwave.udf import udf, UdfServer, _to_data_type
import pyarrow as pa
import pyarrow.flight as flight
import time
from typing import Any

def flight_server():
    server = UdfServer(location="localhost:8815")
    server.add_function(add)
    server.add_function(wait)
    server.add_function(wait_concurrent)
    return server

def flight_client():
    client = flight.FlightClient(("localhost", 8815))
    return client

# Define a scalar function
@udf(input_types=['INT', 'INT'], result_type='INT')
def add(x, y):
    return x + y

@udf(input_types=['INT'], result_type='INT')
def wait(x):
    time.sleep(0.01)
    return 0

@udf(input_types=['INT'], result_type='INT', io_threads=32)
def wait_concurrent(x):
    time.sleep(0.01)
    return 0

def test_simple():
    LEN = 64
    data = pa.Table.from_arrays([
        pa.array(range(0, LEN)),
        pa.array(range(0, LEN))
    ], names=["x", "y"])

    batches = data.to_batches(max_chunksize=512)

    with flight_client() as client, flight_server() as server:
        flight_info = flight.FlightDescriptor.for_path(b"add")
        writer, reader = client.do_exchange(descriptor=flight_info)
        with writer:
            writer.begin(schema=data.schema)
            for batch in batches:
                writer.write_batch(batch)
            writer.done_writing()

            chunk = reader.read_chunk()
            assert len(chunk.data) == LEN
            assert chunk.data.column('output').equals(pa.array(range(0, LEN * 2, 2), type=pa.int32()))

def test_io_concurrency():
    LEN = 64
    data = pa.Table.from_arrays([
        pa.array(range(0, LEN))
    ], names=["x"])
    batches = data.to_batches(max_chunksize=512)

    # Check that our original function is available in the module root
    # when using io_threads
    assert '__original_wait_concurrent' in globals()

    with flight_client() as client, flight_server() as server:
        flight_info = flight.FlightDescriptor.for_path(b"wait")
        writer, reader = client.do_exchange(descriptor=flight_info)
        with writer:
            writer.begin(schema=data.schema)
            for batch in batches:
                writer.write_batch(batch)
            writer.done_writing()
            start_time = time.time()

            total_len = 0
            for chunk in reader:
                total_len += len(chunk.data)

            assert total_len == LEN

            elapsed_time = time.time() - start_time # ~0.64s
            assert elapsed_time > 0.5

        flight_info = flight.FlightDescriptor.for_path(b"wait_concurrent")
        writer, reader = client.do_exchange(descriptor=flight_info)
        with writer:
            writer.begin(schema=data.schema)
            for batch in batches:
                writer.write_batch(batch)
            writer.done_writing()
            start_time = time.time()

            total_len = 0
            for chunk in reader:
                total_len += len(chunk.data)

            assert total_len == LEN

            elapsed_time = time.time() - start_time
            assert elapsed_time < 0.25
        