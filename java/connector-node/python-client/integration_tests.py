# Copyright 2023 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import argparse
import json
import grpc
import connector_service_pb2_grpc
import connector_service_pb2
import psycopg2


def make_mock_schema():
    # todo
    schema = connector_service_pb2.TableSchema(
        columns=[
            connector_service_pb2.TableSchema.Column(name="id", data_type=2),
            connector_service_pb2.TableSchema.Column(name="name", data_type=7)
        ],
        pk_indices=[0]
    )
    return schema

def make_mock_schema_stream_chunk():
    schema = connector_service_pb2.TableSchema(
        columns=[
            connector_service_pb2.TableSchema.Column(name="v1", data_type=1),
            connector_service_pb2.TableSchema.Column(name="v2", data_type=2),
            connector_service_pb2.TableSchema.Column(name="v3", data_type=3),
            connector_service_pb2.TableSchema.Column(name="v4", data_type=4),
            connector_service_pb2.TableSchema.Column(name="v5", data_type=5),
            connector_service_pb2.TableSchema.Column(name="v6", data_type=6),
            connector_service_pb2.TableSchema.Column(name="v7", data_type=7),
        ],
        pk_indices=[0]
    )
    return schema


def load_input(input_file):
    with open(input_file, 'r') as file:
        sink_input = json.load(file)
    return sink_input

def load_binary_input(input_file):
    with open(input_file, 'rb') as file:
        sink_input = file.read()
    return sink_input


def test_upsert_sink(type, prop, input_file):
    sink_input = load_input(input_file)
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = connector_service_pb2_grpc.ConnectorServiceStub(channel)
        request_list = [
            connector_service_pb2.SinkStreamRequest(start=connector_service_pb2.SinkStreamRequest.StartSink(
                format=connector_service_pb2.SinkPayloadFormat.JSON,
                sink_config=connector_service_pb2.SinkConfig(
                    connector_type=type,
                    properties=prop,
                    table_schema=make_mock_schema()
                )
            ))]
        epoch = 0
        batch_id = 1
        for batch in sink_input:
            request_list.append(connector_service_pb2.SinkStreamRequest(
                start_epoch=connector_service_pb2.SinkStreamRequest.StartEpoch(epoch=epoch)))
            row_ops = []
            for row in batch:
                row_ops.append(connector_service_pb2.SinkStreamRequest.WriteBatch.JsonPayload.RowOp(
                    op_type=row['op_type'], line=str(row['line'])))
            request_list.append(connector_service_pb2.SinkStreamRequest(write=connector_service_pb2.SinkStreamRequest.WriteBatch(
                json_payload=connector_service_pb2.SinkStreamRequest.WriteBatch.JsonPayload(row_ops=row_ops),
                batch_id=batch_id,
                epoch=epoch
            )))
            request_list.append(connector_service_pb2.SinkStreamRequest(sync=connector_service_pb2.SinkStreamRequest.SyncBatch(epoch=epoch)))
            epoch += 1
            batch_id += 1

        response_iter = stub.SinkStream(iter(request_list))
        for req in request_list:
            try:
                print("REQUEST", req)
                print("RESPONSE OK:", next(response_iter))
            except Exception as e:
                print("Integration test failed: ", e)
                exit(1)

def test_sink(type, prop, input_file):
    sink_input = load_input(input_file)
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = connector_service_pb2_grpc.ConnectorServiceStub(channel)
        request_list = [
            connector_service_pb2.SinkStreamRequest(start=connector_service_pb2.SinkStreamRequest.StartSink(
                format=connector_service_pb2.SinkPayloadFormat.JSON,
                sink_config=connector_service_pb2.SinkConfig(
                    connector_type=type,
                    properties=prop,
                    table_schema=make_mock_schema()
                )
            ))]
        epoch = 0
        batch_id = 1
        for batch in sink_input:
            request_list.append(connector_service_pb2.SinkStreamRequest(
                start_epoch=connector_service_pb2.SinkStreamRequest.StartEpoch(epoch=epoch)))
            row_ops = []
            for row in batch:
                row_ops.append(connector_service_pb2.SinkStreamRequest.WriteBatch.JsonPayload.RowOp(
                    op_type=1, line=str(row)))
            request_list.append(connector_service_pb2.SinkStreamRequest(write=connector_service_pb2.SinkStreamRequest.WriteBatch(
                json_payload=connector_service_pb2.SinkStreamRequest.WriteBatch.JsonPayload(row_ops=row_ops),
                batch_id=batch_id,
                epoch=epoch
            )))
            request_list.append(connector_service_pb2.SinkStreamRequest(sync=connector_service_pb2.SinkStreamRequest.SyncBatch(epoch=epoch)))
            epoch += 1
            batch_id += 1

        response_iter = stub.SinkStream(iter(request_list))
        for req in request_list:
            try:
                print("REQUEST", req)
                print("RESPONSE OK:", next(response_iter))
            except Exception as e:
                print("Integration test failed: ", e)
                exit(1)


def test_sink_stream_chunk(type, prop, input_file):
    sink_input=load_binary_input(input_file)
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = connector_service_pb2_grpc.ConnectorServiceStub(channel)
        request_list = [
            connector_service_pb2.SinkStreamRequest(start=connector_service_pb2.SinkStreamRequest.StartSink(
                format=connector_service_pb2.SinkPayloadFormat.STREAM_CHUNK,
                sink_config=connector_service_pb2.SinkConfig(
                    connector_type=type,
                    properties=prop,
                    table_schema=make_mock_schema_stream_chunk()
                )
            ))]
        epoch = 0
        batch_id = 1
        request_list.append(connector_service_pb2.SinkStreamRequest(
            start_epoch=connector_service_pb2.SinkStreamRequest.StartEpoch(epoch=epoch)))
        request_list.append(connector_service_pb2.SinkStreamRequest(write=connector_service_pb2.SinkStreamRequest.WriteBatch(
            stream_chunk_payload=connector_service_pb2.SinkStreamRequest.WriteBatch.StreamChunkPayload(binary_data=sink_input),
            batch_id=batch_id,
            epoch=epoch
        )))
        request_list.append(connector_service_pb2.SinkStreamRequest(sync=connector_service_pb2.SinkStreamRequest.SyncBatch(epoch=epoch)))
        epoch += 1
        batch_id += 1

        response_iter = stub.SinkStream(iter(request_list))
        for req in request_list:
            try:
                print("REQUEST", req)
                print("RESPONSE OK:", next(response_iter))
            except Exception as e:
                print("Integration test failed: ", e)
                exit(1)


def test_file_sink(input_file):
    test_sink("file", {"output.path": "/tmp/connector",}, input_file)


def test_jdbc_sink(input_file):
    test_sink("jdbc",
              {"jdbc.url": "jdbc:postgresql://localhost:5432/test?user=test&password=connector",
               "table.name": "test"},
              input_file)

    # validate results
    validate_jdbc_sink(input_file)

def validate_jdbc_sink(input_file):
    conn = psycopg2.connect("dbname=test user=test password=connector host=localhost port=5432")
    cur = conn.cursor()
    cur.execute("SELECT * FROM test")
    rows = cur.fetchall()
    expected = [list(row.values()) for batch in load_input(input_file) for row in batch]
    if len(rows) != len(expected):
        print("Integration test failed: expected {} rows, but got {}".format(len(expected), len(rows)))
        exit(1)
    for i in range(len(rows)):
        if len(rows[i]) != len(expected[i]):
            print("Integration test failed: expected {} columns, but got {}".format(len(expected[i]), len(rows[i])))
            exit(1)
        for j in range(len(rows[i])):
            if rows[i][j] != expected[i][j]:
                print(
                    "Integration test failed: expected {} at row {}, column {}, but got {}".format(expected[i][j], i, j,
                                                                                                   rows[i][j]))
                exit(1)

def test_iceberg_sink(input_file):
    test_sink("iceberg",
              {"type":"append-only",
               "warehouse.path":"s3a://bucket",
               "s3.endpoint": "http://127.0.0.1:9000",
               "s3.access.key": "minioadmin",
               "s3.secret.key": "minioadmin",
               "database.name":"demo_db",
               "table.name":"demo_table"},
              input_file)

def test_upsert_iceberg_sink(input_file):
    test_upsert_sink("iceberg",
              {"type":"upsert",
               "warehouse.path":"s3a://bucket",
               "s3.endpoint": "http://127.0.0.1:9000",
               "s3.access.key": "minioadmin",
               "s3.secret.key": "minioadmin",
               "database.name":"demo_db",
               "table.name":"demo_table"},
              input_file)

def test_deltalake_sink(input_file):
    test_sink("deltalake",
              {
                "location":"s3a://bucket/delta",
                "s3.access.key": "minioadmin",
                "s3.secret.key": "minioadmin",
                "s3.endpoint": "127.0.0.1:9000",
              },
              input_file)

def test_stream_chunk_sink(input_binary_file):
    test_sink_stream_chunk("file", {"output.path": "/tmp/connector",}, input_binary_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--file_sink', action='store_true', help="run file sink test")
    parser.add_argument('--jdbc_sink', action='store_true', help="run jdbc sink test")
    parser.add_argument('--stream_chunk_sink', action='store_true', help="run print stream chunk sink test")
    parser.add_argument('--iceberg_sink', action='store_true', help="run iceberg sink test")
    parser.add_argument('--upsert_iceberg_sink', action='store_true', help="run upsert iceberg sink test")
    parser.add_argument('--deltalake_sink', action='store_true', help="run deltalake sink test")
    parser.add_argument('--input_file', default="./data/sink_input.json", help="input data to run tests")
    parser.add_argument('--input_binary_file', default="./data/stream_chunk_data", help="input stream chunk data to run tests")
    args = parser.parse_args()
    if args.file_sink:
        test_file_sink(args.input_file)
    if args.jdbc_sink:
        test_jdbc_sink(args.input_file)
    if args.stream_chunk_sink:
        test_stream_chunk_sink(args.input_binary_file)
    if args.iceberg_sink:
        test_iceberg_sink(args.input_file)
    if args.upsert_iceberg_sink:
        test_upsert_iceberg_sink(args.input_file)
    if args.deltalake_sink:
        test_deltalake_sink(args.input_file)
