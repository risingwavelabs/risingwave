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

package com.risingwave.connector.sink;

import com.risingwave.connector.SinkWriterStreamObserver;
import com.risingwave.connector.TestUtils;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data.Op;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class SinkStreamObserverTest {

    public ConnectorServiceProto.SinkParam fileSinkParam =
            ConnectorServiceProto.SinkParam.newBuilder()
                    .setTableSchema(TestUtils.getMockTableProto())
                    .putAllProperties(
                            Map.of("output.path", "/tmp/rw-connector", "connector", "file"))
                    .build();

    @Test
    public void testOnNext_StartTaskValidation() {

        StreamObserver<ConnectorServiceProto.SinkWriterStreamResponse> testResponseObserver =
                createNoisyFailResponseObserver();
        SinkWriterStreamObserver sinkWriterStreamObserver =
                getMockSinkStreamObserver(testResponseObserver);
        ConnectorServiceProto.SinkWriterStreamRequest firstSync =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setBarrier(
                                ConnectorServiceProto.SinkWriterStreamRequest.Barrier.newBuilder()
                                        .setEpoch(1)
                                        .setIsCheckpoint(true)
                                        .build())
                        .build();

        // test validation of start sink
        boolean exceptionThrown = false;
        try {
            sinkWriterStreamObserver.onNext(firstSync);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().toLowerCase().contains("sink is not initialized"));
        }
        if (!exceptionThrown) {
            Assert.fail(
                    "Expected exception not thrown: \"Sink is not initialized. Invoke `CreateSink` first.\"");
        }
    }

    private static StreamObserver<ConnectorServiceProto.SinkWriterStreamResponse>
            createNoisyFailResponseObserver() {
        return new StreamObserver<>() {
            @Override
            public void onNext(ConnectorServiceProto.SinkWriterStreamResponse sinkResponse) {
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

    private static SinkWriterStreamObserver getMockSinkStreamObserver(
            StreamObserver<ConnectorServiceProto.SinkWriterStreamResponse> testResponseObserver) {
        return new SinkWriterStreamObserver(testResponseObserver);
    }

    @Test
    public void testOnNext_syncValidation() {
        SinkWriterStreamObserver sinkWriterStreamObserver =
                getMockSinkStreamObserver(createNoisyFailResponseObserver());
        ConnectorServiceProto.SinkWriterStreamRequest startSink =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setStart(
                                ConnectorServiceProto.SinkWriterStreamRequest.StartSink.newBuilder()
                                        .setSinkParam(fileSinkParam)
                                        .setFormat(ConnectorServiceProto.SinkPayloadFormat.JSON)
                                        .build())
                        .build();
        ConnectorServiceProto.SinkWriterStreamRequest firstSync =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setBarrier(
                                ConnectorServiceProto.SinkWriterStreamRequest.Barrier.newBuilder()
                                        .setEpoch(0)
                                        .setIsCheckpoint(true)
                                        .build())
                        .build();
        ConnectorServiceProto.SinkWriterStreamRequest duplicateSync =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setBarrier(
                                ConnectorServiceProto.SinkWriterStreamRequest.Barrier.newBuilder()
                                        .setEpoch(0)
                                        .setIsCheckpoint(true)
                                        .build())
                        .build();

        // test validation of sync
        boolean exceptionThrown = false;
        try {
            sinkWriterStreamObserver.onNext(startSink);
            sinkWriterStreamObserver.onNext(firstSync);
            sinkWriterStreamObserver.onNext(duplicateSync);
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

        SinkWriterStreamObserver sinkWriterStreamObserver;
        ConnectorServiceProto.SinkWriterStreamRequest startSink =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setStart(
                                ConnectorServiceProto.SinkWriterStreamRequest.StartSink.newBuilder()
                                        .setSinkParam(fileSinkParam)
                                        .setFormat(ConnectorServiceProto.SinkPayloadFormat.JSON)
                                        .build())
                        .build();
        ConnectorServiceProto.SinkWriterStreamRequest firstSync =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setBarrier(
                                ConnectorServiceProto.SinkWriterStreamRequest.Barrier.newBuilder()
                                        .setEpoch(0)
                                        .setIsCheckpoint(true)
                                        .build())
                        .build();

        // test validation of start epoch
        sinkWriterStreamObserver = getMockSinkStreamObserver(createNoisyFailResponseObserver());
        sinkWriterStreamObserver.onNext(startSink);
        sinkWriterStreamObserver.onNext(firstSync);
    }

    @Test
    public void testOnNext_writeValidation() {
        SinkWriterStreamObserver sinkWriterStreamObserver;

        ConnectorServiceProto.SinkWriterStreamRequest startSink =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setStart(
                                ConnectorServiceProto.SinkWriterStreamRequest.StartSink.newBuilder()
                                        .setFormat(ConnectorServiceProto.SinkPayloadFormat.JSON)
                                        .setSinkParam(fileSinkParam))
                        .build();

        ConnectorServiceProto.SinkWriterStreamRequest firstWrite =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setWriteBatch(
                                ConnectorServiceProto.SinkWriterStreamRequest.WriteBatch
                                        .newBuilder()
                                        .setEpoch(0)
                                        .setBatchId(1)
                                        .setJsonPayload(
                                                ConnectorServiceProto.SinkWriterStreamRequest
                                                        .WriteBatch.JsonPayload.newBuilder()
                                                        .addRowOps(
                                                                ConnectorServiceProto
                                                                        .SinkWriterStreamRequest
                                                                        .WriteBatch.JsonPayload
                                                                        .RowOp.newBuilder()
                                                                        .setOpType(Op.INSERT)
                                                                        .setLine(
                                                                                "{\"id\": 1, \"name\": \"test\"}")
                                                                        .build()))
                                        .build())
                        .build();

        ConnectorServiceProto.SinkWriterStreamRequest firstSync =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setBarrier(
                                ConnectorServiceProto.SinkWriterStreamRequest.Barrier.newBuilder()
                                        .setEpoch(0)
                                        .setIsCheckpoint(true)
                                        .build())
                        .build();

        ConnectorServiceProto.SinkWriterStreamRequest secondWrite =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setWriteBatch(
                                ConnectorServiceProto.SinkWriterStreamRequest.WriteBatch
                                        .newBuilder()
                                        .setEpoch(1)
                                        .setBatchId(2)
                                        .setJsonPayload(
                                                ConnectorServiceProto.SinkWriterStreamRequest
                                                        .WriteBatch.JsonPayload.newBuilder()
                                                        .addRowOps(
                                                                ConnectorServiceProto
                                                                        .SinkWriterStreamRequest
                                                                        .WriteBatch.JsonPayload
                                                                        .RowOp.newBuilder()
                                                                        .setOpType(Op.INSERT)
                                                                        .setLine(
                                                                                "{\"id\": 2, \"name\": \"test\"}")
                                                                        .build()))
                                        .build())
                        .build();

        ConnectorServiceProto.SinkWriterStreamRequest secondWriteWrongEpoch =
                ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                        .setWriteBatch(
                                ConnectorServiceProto.SinkWriterStreamRequest.WriteBatch
                                        .newBuilder()
                                        .setEpoch(2)
                                        .setBatchId(3)
                                        .setJsonPayload(
                                                ConnectorServiceProto.SinkWriterStreamRequest
                                                        .WriteBatch.JsonPayload.newBuilder()
                                                        .addRowOps(
                                                                ConnectorServiceProto
                                                                        .SinkWriterStreamRequest
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
            sinkWriterStreamObserver = getMockSinkStreamObserver(createNoisyFailResponseObserver());
            sinkWriterStreamObserver.onNext(startSink);
            sinkWriterStreamObserver.onNext(firstWrite);
            sinkWriterStreamObserver.onNext(firstWrite);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().toLowerCase().contains("batch id"));
        }
        if (!exceptionThrown) {
            Assert.fail("Expected exception not thrown: `invalid batch id`");
        }

        exceptionThrown = false;
        try {
            sinkWriterStreamObserver = getMockSinkStreamObserver(createNoisyFailResponseObserver());
            sinkWriterStreamObserver.onNext(startSink);
            sinkWriterStreamObserver.onNext(firstWrite);
            sinkWriterStreamObserver.onNext(firstSync);
            sinkWriterStreamObserver.onNext(secondWrite); // with mismatched epoch
            sinkWriterStreamObserver.onNext(secondWriteWrongEpoch);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().toLowerCase().contains("invalid epoch"));
        }
        if (!exceptionThrown) {
            Assert.fail(
                    "Expected exception not thrown: `invalid epoch: expected write to epoch 2, got 1`");
        }
    }
}
