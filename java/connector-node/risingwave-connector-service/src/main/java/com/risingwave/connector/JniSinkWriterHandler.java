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

import com.risingwave.java.binding.Binding;
import com.risingwave.proto.ConnectorServiceProto;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JniSinkWriterHandler
        implements StreamObserver<ConnectorServiceProto.SinkWriterStreamResponse> {
    private static final Logger LOG = LoggerFactory.getLogger(JniSinkWriterHandler.class);

    private long requestRxPtr;

    private long responseTxPtr;

    private boolean success;

    public JniSinkWriterHandler(long requestRxPtr, long responseTxPtr) {
        this.requestRxPtr = requestRxPtr;
        this.responseTxPtr = responseTxPtr;
    }

    public static void runJniSinkWriterThread(long requestRxPtr, long responseTxPtr) {
        // For jni.rs
        java.lang.Thread.currentThread()
                .setContextClassLoader(java.lang.ClassLoader.getSystemClassLoader());

        JniSinkWriterHandler handler = new JniSinkWriterHandler(requestRxPtr, responseTxPtr);

        SinkWriterStreamObserver observer = new SinkWriterStreamObserver(handler);

        try {
            byte[] requestBytes;
            while ((requestBytes = Binding.recvSinkWriterRequestFromChannel(handler.requestRxPtr))
                    != null) {
                var request = ConnectorServiceProto.SinkWriterStreamRequest.parseFrom(requestBytes);

                observer.onNext(request);
                if (!handler.success) {
                    throw new RuntimeException("fail to sendSinkWriterResponseToChannel");
                }
            }
            observer.onCompleted();
        } catch (Throwable t) {
            observer.onError(t);
        }
        LOG.info("end of runJniSinkWriterThread");
    }

    @Override
    public void onNext(ConnectorServiceProto.SinkWriterStreamResponse response) {
        this.success =
                Binding.sendSinkWriterResponseToChannel(this.responseTxPtr, response.toByteArray());
    }

    @Override
    public void onError(Throwable throwable) {
        LOG.error("JniSinkWriterHandler onError: ", throwable);
    }

    @Override
    public void onCompleted() {
        LOG.info("JniSinkWriterHandler onCompleted");
    }
}
