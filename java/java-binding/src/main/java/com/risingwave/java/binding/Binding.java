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

package com.risingwave.java.binding;

import io.questdb.jar.jni.JarJniLoader;

public class Binding {
    static {
        JarJniLoader.loadLib(Binding.class, "/risingwave/jni", "risingwave_java_binding");
    }

    public static native int vnodeCount();

    // hummock iterator method
    // Return a pointer to the iterator
    static native long hummockIteratorNew(byte[] readPlan);

    // return a pointer to the next row
    static native long hummockIteratorNext(long pointer);

    // Since the underlying rust does not have garbage collection, we will have to manually call
    // close on the iterator to release the iterator instance pointed by the pointer.
    static native void hummockIteratorClose(long pointer);

    // row method
    static native byte[] rowGetKey(long pointer);

    static native int rowGetOp(long pointer);

    static native boolean rowIsNull(long pointer, int index);

    static native short rowGetInt16Value(long pointer, int index);

    static native int rowGetInt32Value(long pointer, int index);

    static native long rowGetInt64Value(long pointer, int index);

    static native float rowGetFloatValue(long pointer, int index);

    static native double rowGetDoubleValue(long pointer, int index);

    static native boolean rowGetBooleanValue(long pointer, int index);

    static native String rowGetStringValue(long pointer, int index);

    static native java.sql.Timestamp rowGetTimestampValue(long pointer, int index);

    static native java.math.BigDecimal rowGetDecimalValue(long pointer, int index);

    static native java.sql.Time rowGetTimeValue(long pointer, int index);

    static native java.sql.Date rowGetDateValue(long pointer, int index);

    static native String rowGetIntervalValue(long pointer, int index);

    static native String rowGetJsonbValue(long pointer, int index);

    static native byte[] rowGetByteaValue(long pointer, int index);

    // TODO: object or object array?
    static native Object rowGetArrayValue(long pointer, int index, Class<?> clazz);

    // Since the underlying rust does not have garbage collection, we will have to manually call
    // close on the row to release the row instance pointed by the pointer.
    static native void rowClose(long pointer);

    // stream chunk iterator method
    static native long streamChunkIteratorNew(byte[] streamChunkPayload);

    static native long streamChunkIteratorNext(long pointer);

    static native void streamChunkIteratorClose(long pointer);
}
