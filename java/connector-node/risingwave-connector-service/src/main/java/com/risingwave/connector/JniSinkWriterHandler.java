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
import com.risingwave.java.binding.JniSinkWriterStreamRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JniSinkWriterHandler {
    private static final Logger LOG = LoggerFactory.getLogger(JniSinkWriterHandler.class);

    public static void runJniSinkWriterThread(long requestRxPtr, long responseTxPtr) {
        // For jni.rs
        java.lang.Thread.currentThread()
                .setContextClassLoader(java.lang.ClassLoader.getSystemClassLoader());
        JniSinkWriterResponseObserver responseObserver =
                new JniSinkWriterResponseObserver(responseTxPtr);
        SinkWriterStreamObserver sinkWriterStreamObserver =
                new SinkWriterStreamObserver(responseObserver);
        try {
            while (true) {
                try (JniSinkWriterStreamRequest request =
                        Binding.recvSinkWriterRequestFromChannel(requestRxPtr)) {
                    if (request == null) {
                        break;
                    }
                    sinkWriterStreamObserver.onNext(request.asPbRequest());
                }
                if (!responseObserver.isSuccess()) {
                    throw new RuntimeException("fail to sendSinkWriterResponseToChannel");
                }
            }
            sinkWriterStreamObserver.onCompleted();
        } catch (Throwable t) {
            sinkWriterStreamObserver.onError(t);
        }
        LOG.info("end of runJniSinkWriterThread");
    }
}
