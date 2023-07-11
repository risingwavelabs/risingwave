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
import data_pb2
import psycopg2


def make_mock_schema():
    # todo
    schema = connector_service_pb2.TableSchema(
        columns=[
            connector_service_pb2.TableSchema.Column(
                name="id", data_type=data_pb2.DataType(type_name=2)
            ),
            connector_service_pb2.TableSchema.Column(
                name="name", data_type=data_pb2.DataType(type_name=7)
            ),
        ],
        pk_indices=[0],
    )
    return schema


def make_mock_schema_stream_chunk():
    schema = connector_service_pb2.TableSchema(
        columns=[
            connector_service_pb2.TableSchema.Column(
                name="v1", data_type=data_pb2.DataType(type_name=1)
            ),
            connector_service_pb2.TableSchema.Column(
                name="v2", data_type=data_pb2.DataType(type_name=2)
            ),
            connector_service_pb2.TableSchema.Column(
                name="v3", data_type=data_pb2.DataType(type_name=3)
            ),
            connector_service_pb2.TableSchema.Column(
                name="v4", data_type=data_pb2.DataType(type_name=4)
            ),
            connector_service_pb2.TableSchema.Column(
                name="v5", data_type=data_pb2.DataType(type_name=5)
            ),
            connector_service_pb2.TableSchema.Column(
                name="v6", data_type=data_pb2.DataType(type_name=6)
            ),
            connector_service_pb2.TableSchema.Column(
                name="v7", data_type=data_pb2.DataType(type_name=7)
            ),
        ],
        pk_indices=[0],
    )
    return schema


def load_input(input_file):
    with open(input_file, "r") as file:
        sink_input = json.load(file)
    return sink_input


def load_binary_input(input_file):
    with open(input_file, "rb") as file:
        sink_input = file.read()
    return sink_input


def load_json_payload(input_file):
    sink_input = load_input(input_file)
    payloads = []
    for batch in sink_input:
        row_ops = []
        for row in batch:
            row_ops.append(
                connector_service_pb2.SinkStreamRequest.WriteBatch.JsonPayload.RowOp(
                    op_type=row["op_type"], line=str(row["line"])
                )
            )

        payloads.append(
            {
                "json_payload": connector_service_pb2.SinkStreamRequest.WriteBatch.JsonPayload(
                    row_ops=row_ops
                )
            }
        )
    return payloads


def load_stream_chunk_payload(input_file):
    payloads = []
    sink_input = load_binary_input(input_file)
    payloads.append(
        {
            "stream_chunk_payload": connector_service_pb2.SinkStreamRequest.WriteBatch.StreamChunkPayload(
                binary_data=sink_input
            )
        }
    )
    return payloads


def test_sink(prop, format, payload_input, table_schema):
    # read input, Add StartSink request
    request_list = [
        connector_service_pb2.SinkStreamRequest(
            start=connector_service_pb2.SinkStreamRequest.StartSink(
                format=format,
                sink_config=connector_service_pb2.SinkConfig(
                    connector_type=prop["connector"],
                    properties=prop,
                    table_schema=table_schema,
                ),
            )
        )
    ]

    with grpc.insecure_channel("localhost:50051") as channel:
        stub = connector_service_pb2_grpc.ConnectorServiceStub(channel)
        epoch = 1
        batch_id = 1
        # construct request
        for payload in payload_input:
            request_list.append(
                connector_service_pb2.SinkStreamRequest(
                    start_epoch=connector_service_pb2.SinkStreamRequest.StartEpoch(
                        epoch=epoch
                    )
                )
            )
            request_list.append(
                connector_service_pb2.SinkStreamRequest(
                    write=connector_service_pb2.SinkStreamRequest.WriteBatch(
                        batch_id=batch_id,
                        epoch=epoch,
                        **payload,
                    )
                )
            )

            request_list.append(
                connector_service_pb2.SinkStreamRequest(
                    sync=connector_service_pb2.SinkStreamRequest.SyncBatch(epoch=epoch)
                )
            )
            epoch += 1
            batch_id += 1
        # send request
        response_iter = stub.SinkStream(iter(request_list))
        for req in request_list:
            try:
                print("REQUEST", req)
                print("RESPONSE OK:", next(response_iter))
            except Exception as e:
                print("Integration test failed: ", e)
                exit(1)


def validate_jdbc_sink(input_file):
    conn = psycopg2.connect(
        "dbname=test user=test password=connector host=localhost port=5432"
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM test")
    rows = cur.fetchall()
    expected = [list(row.values()) for batch in load_input(input_file) for row in batch]

    def convert(b):
        return [(item[1]["id"], item[1]["name"]) for item in b]

    expected = convert(expected)

    if len(rows) != len(expected):
        print(
            "Integration test failed: expected {} rows, but got {}".format(
                len(expected), len(rows)
            )
        )
        exit(1)
    for i in range(len(rows)):
        if len(rows[i]) != len(expected[i]):
            print(
                "Integration test failed: expected {} columns, but got {}".format(
                    len(expected[i]), len(rows[i])
                )
            )
            exit(1)
        for j in range(len(rows[i])):
            if rows[i][j] != expected[i][j]:
                print(
                    "Integration test failed: expected {} at row {}, column {}, but got {}".format(
                        expected[i][j], i, j, rows[i][j]
                    )
                )
                exit(1)


def test_file_sink(param):
    prop = {
        "connector": "file",
        "output.path": "/tmp/connector",
    }
    test_sink(prop, **param)


def test_jdbc_sink(input_file, param):
    prop = {
        "connector": "jdbc",
        "jdbc.url": "jdbc:postgresql://localhost:5432/test?user=test&password=connector",
        "table.name": "test",
        "type": "upsert",
    }
    test_sink(prop, **param)
    # validate results
    validate_jdbc_sink(input_file)


def test_elasticsearch_sink(param):
    prop = {
        "connector": "elasticsearch-7",
        "url": "http://127.0.0.1:9200",
        "index": "test",
    }
    test_sink(prop, **param)


def test_iceberg_sink(param):
    prop = {
        "connector": "iceberg",
        "type": "append-only",
        "warehouse.path": "s3a://bucket",
        "s3.endpoint": "http://127.0.0.1:9000",
        "s3.access.key": "minioadmin",
        "s3.secret.key": "minioadmin",
        "database.name": "demo_db",
        "table.name": "demo_table",
    }
    test_sink(prop, **param)


def test_upsert_iceberg_sink(param):
    prop = {
        "connector": "iceberg",
        "type": "upsert",
        "warehouse.path": "s3a://bucket",
        "s3.endpoint": "http://127.0.0.1:9000",
        "s3.access.key": "minioadmin",
        "s3.secret.key": "minioadmin",
        "database.name": "demo_db",
        "table.name": "demo_table",
    }
    type = "iceberg"
    # need to make sure all ops as Insert
    test_sink(prop, **param)


def test_deltalake_sink(param):
    prop = {
        "connector": "deltalake",
        "location": "s3a://bucket/delta",
        "s3.access.key": "minioadmin",
        "s3.secret.key": "minioadmin",
        "s3.endpoint": "127.0.0.1:9000",
    }
    test_sink(prop, **param)


def test_stream_chunk_data_format(param):
    prop = {
        "connector": "file",
        "output.path": "/tmp/connector",
    }
    test_sink(prop, **param)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--file_sink", action="store_true", help="run file sink test")
    parser.add_argument("--jdbc_sink", action="store_true", help="run jdbc sink test")
    parser.add_argument(
        "--stream_chunk_format_test",
        action="store_true",
        help="run print stream chunk sink test",
    )
    parser.add_argument(
        "--iceberg_sink", action="store_true", help="run iceberg sink test"
    )
    parser.add_argument(
        "--upsert_iceberg_sink",
        action="store_true",
        help="run upsert iceberg sink test",
    )
    parser.add_argument(
        "--deltalake_sink", action="store_true", help="run deltalake sink test"
    )
    parser.add_argument(
        "--input_file", default="./data/sink_input.json", help="input data to run tests"
    )
    parser.add_argument(
        "--input_binary_file",
        default="./data/sink_input",
        help="input stream chunk data to run tests",
    )
    parser.add_argument(
        "--es_sink", action="store_true", help="run elasticsearch sink test"
    )
    parser.add_argument(
        "--data_format_use_json", default=True, help="choose json or streamchunk"
    )
    args = parser.parse_args()
    use_json = args.data_format_use_json == True or args.data_format_use_json == "True"
    if use_json:
        payload = load_json_payload(args.input_file)
        format = connector_service_pb2.SinkPayloadFormat.JSON
    else:
        payload = load_stream_chunk_payload(args.input_binary_file)
        format = connector_service_pb2.SinkPayloadFormat.STREAM_CHUNK

    # stream chunk format
    if args.stream_chunk_format_test:
        param = {
            "format": format,
            "payload_input": payload,
            "table_schema": make_mock_schema_stream_chunk(),
        }
        test_stream_chunk_data_format(param)

    param = {
        "format": format,
        "payload_input": payload,
        "table_schema": make_mock_schema(),
    }

    if args.file_sink:
        test_file_sink(param)
    if args.jdbc_sink:
        test_jdbc_sink(args.input_file, param)
    if args.iceberg_sink:
        test_iceberg_sink(param)
    if args.deltalake_sink:
        test_deltalake_sink(param)
    if args.es_sink:
        test_elasticsearch_sink(param)

    # json format
    if args.upsert_iceberg_sink:
        test_upsert_iceberg_sink(param)
