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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JniSinkCoordinatorHandler {
    private static final Logger LOG = LoggerFactory.getLogger(JniSinkCoordinatorHandler.class);

    public static void runJniSinkCoordinatorThread(long requestRxPtr, long responseTxPtr) {
        // For jni.rs
        java.lang.Thread.currentThread()
                .setContextClassLoader(java.lang.ClassLoader.getSystemClassLoader());
        JniSinkCoordinatorResponseObserver responseObserver =
                new JniSinkCoordinatorResponseObserver(responseTxPtr);
        SinkCoordinatorStreamObserver sinkCoordinatorStreamObserver =
                new SinkCoordinatorStreamObserver(responseObserver);
        try {
            byte[] requestBytes;
            while ((requestBytes = Binding.recvSinkCoordinatorRequestFromChannel(requestRxPtr))
                    != null) {
                var request =
                        ConnectorServiceProto.SinkCoordinatorStreamRequest.parseFrom(requestBytes);
                sinkCoordinatorStreamObserver.onNext(request);
                if (!responseObserver.isSuccess()) {
                    throw new RuntimeException("fail to sendSinkCoordinatorResponseToChannel");
                }
            }
            sinkCoordinatorStreamObserver.onCompleted();
        } catch (Throwable t) {
            sinkCoordinatorStreamObserver.onError(t);
        }
        LOG.info("end of runJniSinkCoordinatorThread");
    }
}
