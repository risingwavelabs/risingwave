// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.SinkConfig;
import com.risingwave.proto.Data.Op;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class SinkStreamObserverTest {

    public SinkConfig fileSinkConfig =
            SinkConfig.newBuilder()
                    .setTableSchema(TableSchema.getMockTableProto())
                    .setConnectorType("file")
                    .putAllProperties(Map.of("output.path", "/tmp/rw-connector"))
                    .build();

    @Test
    public void testOnNext_StartTaskValidation() {

        StreamObserver<ConnectorServiceProto.SinkResponse> testResponseObserver =
                createNoisyFailResponseObserver();
        SinkStreamObserver sinkStreamObserver = getMockSinkStreamObserver(testResponseObserver);
        ConnectorServiceProto.SinkStreamRequest firstSync =
                ConnectorServiceProto.SinkStreamRequest.newBuilder()
                        .setSync(
                                ConnectorServiceProto.SinkStreamRequest.SyncBatch.newBuilder()
                                        .setEpoch(1)
                                        .build())
                        .build();

        // test validation of start sink
        boolean exceptionThrown = false;
        try {
            sinkStreamObserver.onNext(firstSync);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().toLowerCase().contains("sink is not initialized"));
        }
        if (!exceptionThrown) {
            Assert.fail(
                    "Expected exception not thrown: \"Sink is not initialized. Invoke `CreateSink` first.\"");
        }
    }

    private static StreamObserver<ConnectorServiceProto.SinkResponse>
            createNoisyFailResponseObserver() {
        return new StreamObserver<>() {
            @Override
            public void onNext(ConnectorServiceProto.SinkResponse sinkResponse) {
                // response ok
            }

            @Override
            public void onError(Throwable throwable) {
                throw new RuntimeException(throwable);
            }

            @Override
            public void onCompleted() {}
        };
    }

    private static SinkStreamObserver getMockSinkStreamObserver(
            StreamObserver<ConnectorServiceProto.SinkResponse> testResponseObserver) {
        return new SinkStreamObserver(testResponseObserver);
    }

    @Test
    public void testOnNext_syncValidation() {
        SinkStreamObserver sinkStreamObserver =
                getMockSinkStreamObserver(createNoisyFailResponseObserver());
        ConnectorServiceProto.SinkStreamRequest startSink =
                ConnectorServiceProto.SinkStreamRequest.newBuilder()
                        .setStart(
                                ConnectorServiceProto.SinkStreamRequest.StartSink.newBuilder()
                                        .setSinkConfig(fileSinkConfig)
                                        .setFormat(ConnectorServiceProto.SinkPayloadFormat.JSON)
                                        .build())
                        .build();
        ConnectorServiceProto.SinkStreamRequest firstSync =
                ConnectorServiceProto.SinkStreamRequest.newBuilder()
                        .setSync(
                                ConnectorServiceProto.SinkStreamRequest.SyncBatch.newBuilder()
                                        .setEpoch(0)
                                        .build())
                        .build();
        ConnectorServiceProto.SinkStreamRequest duplicateSync =
                ConnectorServiceProto.SinkStreamRequest.newBuilder()
                        .setSync(
                                ConnectorServiceProto.SinkStreamRequest.SyncBatch.newBuilder()
                                        .setEpoch(0)
                                        .build())
                        .build();

        // test validation of sync
        boolean exceptionThrown = false;
        try {
            sinkStreamObserver.onNext(startSink);
            sinkStreamObserver.onNext(firstSync);
            sinkStreamObserver.onNext(duplicateSync);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().toLowerCase().contains("epoch"));
        }
        if (!exceptionThrown) {
            Assert.fail("Expected exception not thrown: `No epoch assigned. Invoke `StartEpoch`.`");
        }
    }

    @Test
    public void testOnNext_startEpochValidation() {

        SinkStreamObserver sinkStreamObserver;
        ConnectorServiceProto.SinkStreamRequest startSink =
                ConnectorServiceProto.SinkStreamRequest.newBuilder()
                        .setStart(
                                ConnectorServiceProto.SinkStreamRequest.StartSink.newBuilder()
                                        .setSinkConfig(fileSinkConfig)
                                        .setFormat(ConnectorServiceProto.SinkPayloadFormat.JSON)
                                        .build())
                        .build();
        ConnectorServiceProto.SinkStreamRequest firstSync =
                ConnectorServiceProto.SinkStreamRequest.newBuilder()
                        .setSync(
                                ConnectorServiceProto.SinkStreamRequest.SyncBatch.newBuilder()
                                        .setEpoch(0)
                                        .build())
                        .build();
        ConnectorServiceProto.SinkStreamRequest startEpoch =
                ConnectorServiceProto.SinkStreamRequest.newBuilder()
                        .setStartEpoch(
                                ConnectorServiceProto.SinkStreamRequest.StartEpoch.newBuilder()
                                        .setEpoch(0)
                                        .build())
                        .build();
        ConnectorServiceProto.SinkStreamRequest duplicateStartEpoch =
                ConnectorServiceProto.SinkStreamRequest.newBuilder()
                        .setStartEpoch(
                                ConnectorServiceProto.SinkStreamRequest.StartEpoch.newBuilder()
                                        .setEpoch(0)
                                        .build())
                        .build();

        // test validation of start epoch
        boolean exceptionThrown = false;
        try {
            sinkStreamObserver = getMockSinkStreamObserver(createNoisyFailResponseObserver());
            sinkStreamObserver.onNext(startSink);
            sinkStreamObserver.onNext(firstSync);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().toLowerCase().contains("epoch is not started"));
        }
        if (!exceptionThrown) {
            Assert.fail(
                    "Expected exception not thrown: `Epoch is not started. Invoke `StartEpoch`.`");
        }

        exceptionThrown = false;
        try {
            sinkStreamObserver = getMockSinkStreamObserver(createNoisyFailResponseObserver());
            sinkStreamObserver.onNext(startSink);
            sinkStreamObserver.onNext(startEpoch);
            sinkStreamObserver.onNext(duplicateStartEpoch);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(
                    e.getMessage().toLowerCase().contains("new epoch id should be larger"));
        }
        if (!exceptionThrown) {
            Assert.fail(
                    "Expected exception not thrown: `invalid epoch: new epoch ID should be larger than current epoch`");
        }
    }

    @Test
    public void testOnNext_writeValidation() {
        SinkStreamObserver sinkStreamObserver;

        ConnectorServiceProto.SinkStreamRequest startSink =
                ConnectorServiceProto.SinkStreamRequest.newBuilder()
                        .setStart(
                                ConnectorServiceProto.SinkStreamRequest.StartSink.newBuilder()
                                        .setFormat(ConnectorServiceProto.SinkPayloadFormat.JSON)
                                        .setSinkConfig(fileSinkConfig))
                        .build();
        ConnectorServiceProto.SinkStreamRequest firstStartEpoch =
                ConnectorServiceProto.SinkStreamRequest.newBuilder()
                        .setStartEpoch(
                                ConnectorServiceProto.SinkStreamRequest.StartEpoch.newBuilder()
                                        .setEpoch(0)
                                        .build())
                        .build();

        ConnectorServiceProto.SinkStreamRequest firstWrite =
                ConnectorServiceProto.SinkStreamRequest.newBuilder()
                        .setWrite(
                                ConnectorServiceProto.SinkStreamRequest.WriteBatch.newBuilder()
                                        .setEpoch(0)
                                        .setBatchId(1)
                                        .setJsonPayload(
                                                ConnectorServiceProto.SinkStreamRequest.WriteBatch
                                                        .JsonPayload.newBuilder()
                                                        .addRowOps(
                                                                ConnectorServiceProto
                                                                        .SinkStreamRequest
                                                                        .WriteBatch.JsonPayload
                                                                        .RowOp.newBuilder()
                                                                        .setOpType(Op.INSERT)
                                                                        .setLine(
                                                                                "{\"id\": 1, \"name\": \"test\"}")
                                                                        .build()))
                                        .build())
                        .build();

        ConnectorServiceProto.SinkStreamRequest firstSync =
                ConnectorServiceProto.SinkStreamRequest.newBuilder()
                        .setSync(
                                ConnectorServiceProto.SinkStreamRequest.SyncBatch.newBuilder()
                                        .setEpoch(0)
                                        .build())
                        .build();

        ConnectorServiceProto.SinkStreamRequest secondStartEpoch =
                ConnectorServiceProto.SinkStreamRequest.newBuilder()
                        .setStartEpoch(
                                ConnectorServiceProto.SinkStreamRequest.StartEpoch.newBuilder()
                                        .setEpoch(1)
                                        .build())
                        .build();

        ConnectorServiceProto.SinkStreamRequest secondWrite =
                ConnectorServiceProto.SinkStreamRequest.newBuilder()
                        .setWrite(
                                ConnectorServiceProto.SinkStreamRequest.WriteBatch.newBuilder()
                                        .setEpoch(0)
                                        .setBatchId(2)
                                        .setJsonPayload(
                                                ConnectorServiceProto.SinkStreamRequest.WriteBatch
                                                        .JsonPayload.newBuilder()
                                                        .addRowOps(
                                                                ConnectorServiceProto
                                                                        .SinkStreamRequest
                                                                        .WriteBatch.JsonPayload
                                                                        .RowOp.newBuilder()
                                                                        .setOpType(Op.INSERT)
                                                                        .setLine(
                                                                                "{\"id\": 2, \"name\": \"test\"}")
                                                                        .build()))
                                        .build())
                        .build();

        boolean exceptionThrown = false;
        try {
            sinkStreamObserver = getMockSinkStreamObserver(createNoisyFailResponseObserver());
            sinkStreamObserver.onNext(startSink);
            sinkStreamObserver.onNext(firstStartEpoch);
            sinkStreamObserver.onNext(firstWrite);
            sinkStreamObserver.onNext(firstWrite);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().toLowerCase().contains("batch id"));
        }
        if (!exceptionThrown) {
            Assert.fail("Expected exception not thrown: `invalid batch id`");
        }

        exceptionThrown = false;
        try {
            sinkStreamObserver = getMockSinkStreamObserver(createNoisyFailResponseObserver());
            sinkStreamObserver.onNext(startSink);
            sinkStreamObserver.onNext(firstStartEpoch);
            sinkStreamObserver.onNext(firstWrite);
            sinkStreamObserver.onNext(firstSync);
            sinkStreamObserver.onNext(secondStartEpoch);
            sinkStreamObserver.onNext(secondWrite); // with mismatched epoch
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().toLowerCase().contains("invalid epoch"));
        }
        if (!exceptionThrown) {
            Assert.fail(
                    "Expected exception not thrown: `invalid epoch: expected write to epoch 1, got 0`");
        }
    }
}
