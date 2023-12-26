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
import plan_common_pb2
import data_pb2
import psycopg2


def make_mock_schema():
    # todo
    schema = connector_service_pb2.TableSchema(
        columns=[
            plan_common_pb2.ColumnDesc(
                name="id", column_type=data_pb2.DataType(type_name=2)
            ),
            plan_common_pb2.ColumnDesc(
                name="name", column_type=data_pb2.DataType(type_name=7)
            ),
        ],
        pk_indices=[0],
    )
    return schema


def make_mock_schema_stream_chunk():
    schema = connector_service_pb2.TableSchema(
        columns=[
            plan_common_pb2.ColumnDesc(
                name="v1", column_type=data_pb2.DataType(type_name=1)
            ),
            plan_common_pb2.ColumnDesc(
                name="v2", column_type=data_pb2.DataType(type_name=2)
            ),
            plan_common_pb2.ColumnDesc(
                name="v3", column_type=data_pb2.DataType(type_name=3)
            ),
            plan_common_pb2.ColumnDesc(
                name="v4", column_type=data_pb2.DataType(type_name=4)
            ),
            plan_common_pb2.ColumnDesc(
                name="v5", column_type=data_pb2.DataType(type_name=5)
            ),
            plan_common_pb2.ColumnDesc(
                name="v6", column_type=data_pb2.DataType(type_name=6)
            ),
            plan_common_pb2.ColumnDesc(
                name="v7", column_type=data_pb2.DataType(type_name=7)
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
                connector_service_pb2.SinkWriterStreamRequest.WriteBatch.JsonPayload.RowOp(
                    op_type=row["op_type"], line=str(row["line"])
                )
            )

        payloads.append(
            {
                "json_payload": connector_service_pb2.SinkWriterStreamRequest.WriteBatch.JsonPayload(
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
            "stream_chunk_payload": connector_service_pb2.SinkWriterStreamRequest.WriteBatch.StreamChunkPayload(
                binary_data=sink_input
            )
        }
    )
    return payloads


def test_sink(prop, format, payload_input, table_schema, is_coordinated=False):
    sink_param = connector_service_pb2.SinkParam(
        sink_id=0,
        properties=prop,
        table_schema=table_schema,
    )

    # read input, Add StartSink request
    request_list = [
        connector_service_pb2.SinkWriterStreamRequest(
            start=connector_service_pb2.SinkWriterStreamRequest.StartSink(
                format=format,
                sink_param=sink_param,
            )
        )
    ]

    with grpc.insecure_channel("localhost:50051") as channel:
        stub = connector_service_pb2_grpc.ConnectorServiceStub(channel)
        epoch = 1
        batch_id = 1
        epoch_list = []
        # construct request
        for payload in payload_input:
            request_list.append(
                connector_service_pb2.SinkWriterStreamRequest(
                    write_batch=connector_service_pb2.SinkWriterStreamRequest.WriteBatch(
                        batch_id=batch_id, epoch=epoch, **payload
                    )
                )
            )

            request_list.append(
                connector_service_pb2.SinkWriterStreamRequest(
                    barrier=connector_service_pb2.SinkWriterStreamRequest.Barrier(
                        epoch=epoch, is_checkpoint=True
                    )
                )
            )
            epoch_list.append(epoch)
            epoch += 1
            batch_id += 1
        # send request
        response_iter = stub.SinkWriterStream(iter(request_list))
        metadata_list = []
        try:
            response = next(response_iter)
            assert response.HasField("start")
            for epoch in epoch_list:
                response = next(response_iter)
                assert response.HasField("commit")
                assert epoch == response.commit.epoch
                metadata_list.append((epoch, response.commit.metadata))
        except Exception as e:
            print("Integration test failed: ", e)
            exit(1)

        if is_coordinated:
            request_list = [
                connector_service_pb2.SinkCoordinatorStreamRequest(
                    start=connector_service_pb2.SinkCoordinatorStreamRequest.StartCoordinator(
                        param=sink_param
                    )
                )
            ]
            request_list += [
                connector_service_pb2.SinkCoordinatorStreamRequest(
                    commit=connector_service_pb2.SinkCoordinatorStreamRequest.CommitMetadata(
                        epoch=epoch, metadata=[metadata]
                    )
                )
                for (epoch, metadata) in metadata_list
            ]

            response_iter = stub.SinkCoordinatorStream(iter(request_list))
            try:
                response = next(response_iter)
                assert response.HasField("start")
                for epoch, _ in metadata_list:
                    response = next(response_iter)
                    assert response.HasField("commit")
                    assert epoch == response.commit.epoch
            except Exception as e:
                print("Integration test failed: ", e)
                exit(1)


def test_file_sink(param):
    prop = {
        "connector": "file",
        "output.path": "/tmp/connector",
    }
    test_sink(prop, **param)


def test_elasticsearch_sink(param):
    prop = {
        "connector": "elasticsearch",
        "url": "http://127.0.0.1:9200",
        "index": "test",
    }
    test_sink(prop, **param)


def test_iceberg_sink(param):
    prop = {
        "connector": "iceberg_java",
        "type": "append-only",
        "warehouse.path": "s3a://bucket",
        "s3.endpoint": "http://127.0.0.1:9000",
        "s3.access.key": "minioadmin",
        "s3.secret.key": "minioadmin",
        "database.name": "demo_db",
        "table.name": "demo_table",
    }
    test_sink(prop, is_coordinated=True, **param)


def test_upsert_iceberg_sink(param):
    prop = {
        "connector": "iceberg_java",
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
    test_sink(prop, is_coordinated=True, **param)


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
    if args.iceberg_sink:
        test_iceberg_sink(param)
    if args.deltalake_sink:
        test_deltalake_sink(param)
    if args.es_sink:
        test_elasticsearch_sink(param)

    # json format
    if args.upsert_iceberg_sink:
        test_upsert_iceberg_sink(param)
