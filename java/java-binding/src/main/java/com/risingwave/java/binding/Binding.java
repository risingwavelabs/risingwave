package com.risingwave.java.binding;

public class Binding {
    static {
        System.loadLibrary("risingwave_java_binding");
    }

    public static native int vnodeCount();

    // iterator method
    // Return a pointer to the iterator
    static native long iteratorNew(byte[] readPlan);

    // return a pointer to the next row
    static native long iteratorNext(long pointer);

    // Since the underlying rust does not have garbage collection, we will have to manually call
    // close on the iterator to release the iterator instance pointed by the pointer.
    static native void iteratorClose(long pointer);

    // row method
    static native byte[] rowGetKey(long pointer);

    static native boolean rowIsNull(long pointer, int index);

    static native short rowGetInt16Value(long pointer, int index);

    static native int rowGetInt32Value(long pointer, int index);

    static native long rowGetInt64Value(long pointer, int index);

    static native float rowGetFloatValue(long pointer, int index);

    static native double rowGetDoubleValue(long pointer, int index);

    static native boolean rowGetBooleanValue(long pointer, int index);

    static native String rowGetStringValue(long pointer, int index);

    // Since the underlying rust does not have garbage collection, we will have to manually call
    // close on the row to release the row instance pointed by the pointer.
    static native void rowClose(long pointer);
}
