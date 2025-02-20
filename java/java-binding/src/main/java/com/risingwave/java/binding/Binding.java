// Copyright 2025 RisingWave Labs
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

package com.risingwave.java.binding;

import io.questdb.jar.jni.JarJniLoader;

public class Binding {
    private static final boolean IS_EMBEDDED_CONNECTOR =
            Boolean.parseBoolean(System.getProperty("is_embedded_connector"));

    static {
        if (!IS_EMBEDDED_CONNECTOR) {
            JarJniLoader.loadLib(Binding.class, "/risingwave/jni", "risingwave_java_binding");
        }
    }

    static void ensureInitialized() {}

    public static native void tracingSlf4jEvent(
            String threadName, String name, int level, String message);

    public static native boolean tracingSlf4jEventEnabled(int level);

    /**
     * Used to get the default number of vnodes for a table, if its `maybeVnodeCount` field is not
     * set.
     */
    public static native int defaultVnodeCount();

    static native long iteratorNewStreamChunk(long pointer);

    static native boolean iteratorNext(long pointer);

    static native void iteratorClose(long pointer);

    static native long newStreamChunkFromPayload(byte[] streamChunkPayload);

    static native long newStreamChunkFromPretty(String str);

    static native void streamChunkClose(long pointer);

    static native byte[] iteratorGetKey(long pointer);

    static native int iteratorGetOp(long pointer);

    static native boolean iteratorIsNull(long pointer, int index);

    static native short iteratorGetInt16Value(long pointer, int index);

    static native int iteratorGetInt32Value(long pointer, int index);

    static native long iteratorGetInt64Value(long pointer, int index);

    static native float iteratorGetFloatValue(long pointer, int index);

    static native double iteratorGetDoubleValue(long pointer, int index);

    static native boolean iteratorGetBooleanValue(long pointer, int index);

    static native String iteratorGetStringValue(long pointer, int index);

    static native java.time.LocalDateTime iteratorGetTimestampValue(long pointer, int index);

    static native java.time.OffsetDateTime iteratorGetTimestamptzValue(long pointer, int index);

    static native java.math.BigDecimal iteratorGetDecimalValue(long pointer, int index);

    static native java.time.LocalTime iteratorGetTimeValue(long pointer, int index);

    static native java.time.LocalDate iteratorGetDateValue(long pointer, int index);

    static native String iteratorGetIntervalValue(long pointer, int index);

    static native String iteratorGetJsonbValue(long pointer, int index);

    static native byte[] iteratorGetByteaValue(long pointer, int index);

    // TODO: object or object array?
    static native Object iteratorGetArrayValue(long pointer, int index, Class<?> clazz);

    public static native boolean sendCdcSourceMsgToChannel(long channelPtr, byte[] msg);

    public static native boolean sendCdcSourceErrorToChannel(long channelPtr, String errorMsg);

    public static native void cdcSourceSenderClose(long channelPtr);

    public static native com.risingwave.java.binding.JniSinkWriterStreamRequest
            recvSinkWriterRequestFromChannel(long channelPtr);

    public static native boolean sendSinkWriterResponseToChannel(long channelPtr, byte[] msg);

    public static native boolean sendSinkWriterErrorToChannel(long channelPtr, String msg);

    public static native byte[] recvSinkCoordinatorRequestFromChannel(long channelPtr);

    public static native boolean sendSinkCoordinatorResponseToChannel(long channelPtr, byte[] msg);
}
