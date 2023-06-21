from multiprocessing import Process
import pytest
from risingwave.udf import udf, UdfServer, _to_data_type
import pyarrow as pa
import pyarrow.flight as flight
import time
import datetime
from typing import Any


def flight_server():
    server = UdfServer(location="localhost:8815")
    server.add_function(add)
    server.add_function(wait)
    server.add_function(wait_concurrent)
    server.add_function(return_all)
    return server


def flight_client():
    client = flight.FlightClient(("localhost", 8815))
    return client


# Define a scalar function
@udf(input_types=["INT", "INT"], result_type="INT")
def add(x, y):
    return x + y


@udf(input_types=["INT"], result_type="INT")
def wait(x):
    time.sleep(0.01)
    return 0


@udf(input_types=["INT"], result_type="INT", io_threads=32)
def wait_concurrent(x):
    time.sleep(0.01)
    return 0


@udf(
    input_types=[
        "BOOLEAN",
        "SMALLINT",
        "INT",
        "BIGINT",
        "FLOAT4",
        "FLOAT8",
        "DECIMAL",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "INTERVAL",
        "VARCHAR",
        "BYTEA",
        "JSONB",
    ],
    result_type="""struct<
        BOOLEAN,
        SMALLINT,
        INT,
        BIGINT,
        FLOAT4,
        FLOAT8,
        DECIMAL,
        DATE,
        TIME,
        TIMESTAMP,
        INTERVAL,
        VARCHAR,
        BYTEA,
        JSONB
    >""",
)
def return_all(
    bool,
    i16,
    i32,
    i64,
    f32,
    f64,
    decimal,
    date,
    time,
    timestamp,
    interval,
    varchar,
    bytea,
    jsonb,
):
    return (
        bool,
        i16,
        i32,
        i64,
        f32,
        f64,
        decimal,
        date,
        time,
        timestamp,
        interval,
        varchar,
        bytea,
        jsonb,
    )


def test_simple():
    LEN = 64
    data = pa.Table.from_arrays(
        [pa.array(range(0, LEN)), pa.array(range(0, LEN))], names=["x", "y"]
    )

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
            assert chunk.data.column("output").equals(
                pa.array(range(0, LEN * 2, 2), type=pa.int32())
            )


def test_io_concurrency():
    LEN = 64
    data = pa.Table.from_arrays([pa.array(range(0, LEN))], names=["x"])
    batches = data.to_batches(max_chunksize=512)

    with flight_client() as client, flight_server() as server:
        # Single-threaded function takes a long time
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

            elapsed_time = time.time() - start_time  # ~0.64s
            assert elapsed_time > 0.5

        # Multi-threaded I/O bound function will take a much shorter time
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


def test_all_types():
    arrays = [
        pa.array([True], type=pa.bool_()),
        pa.array([1], type=pa.int16()),
        pa.array([1], type=pa.int32()),
        pa.array([1], type=pa.int64()),
        pa.array([1], type=pa.float32()),
        pa.array([1], type=pa.float64()),
        pa.array([10**37], type=pa.decimal128(38)),
        pa.array([datetime.date(2023, 6, 1)], type=pa.date32()),
        pa.array([datetime.time(1, 2, 3, 456789)], type=pa.time64("us")),
        pa.array(
            [datetime.datetime(2023, 6, 1, 1, 2, 3, 456789)],
            type=pa.timestamp("us"),
        ),
        pa.array([(1, 2, 3)], type=pa.month_day_nano_interval()),
        pa.array(["string"], type=pa.string()),
        pa.array(["bytes"], type=pa.binary()),
        pa.array(['{ "key": 1 }'], type=pa.large_string()),
    ]
    batch = pa.RecordBatch.from_arrays(arrays, names=["" for _ in arrays])

    with flight_client() as client, flight_server() as server:
        flight_info = flight.FlightDescriptor.for_path(b"return_all")
        writer, reader = client.do_exchange(descriptor=flight_info)
        with writer:
            writer.begin(schema=batch.schema)
            writer.write_batch(batch)
            writer.done_writing()

            chunk = reader.read_chunk()
            assert [v.as_py() for _, v in chunk.data.column(0)[0].items()] == [
                True,
                1,
                1,
                1,
                1.0,
                1.0,
                10**37,
                datetime.date(2023, 6, 1),
                datetime.time(1, 2, 3, 456789),
                datetime.datetime(2023, 6, 1, 1, 2, 3, 456789),
                (1, 2, 3),
                "string",
                b"bytes",
                '{"key": 1}',
            ]
